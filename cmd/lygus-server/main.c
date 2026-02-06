/**
 * cmd/lygus-server/main.c - CLI orchestration
 * The server library (src/server/) knows nothing about CLI concerns.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <getopt.h>

// Event loop
#include "event/event_loop.h"
#include "storage/storage_mgr.h"

// Server library
#include "server/server.h"

// Raft + glue
#include "raft.h"
#include "raft/raft_glue.h"

// Logging
#include "util/logging.h"

// ============================================================================
// Version
// ============================================================================

#define LYGUS_VERSION "0.3.0"

// ============================================================================
// App Context (owned by main)
// ============================================================================

typedef struct {
    // Core components (we own these)
    event_loop_t    *loop;
    raft_t          *raft;
    raft_glue_ctx_t  glue_ctx;
    server_t        *server;

    // Timers
    ev_timer_t      *tick_timer;

    // Config
    int              node_id;
    const char      *peers_file;
    const char      *data_dir;
    int              port;
    int              verbose;
    uint32_t         snapshot_threshold;
    bool             leader_only;    // Benchmark mode: disable follower reads

    // State tracking
    uint64_t         last_tick_ms;
    bool             was_leader;
    uint64_t         last_term;      // Track term for change detection

} app_t;

static app_t g_app;

// ============================================================================
// Signal Handling (platform-specific, CLI concern)
// ============================================================================

#ifdef __linux__
#include <sys/signalfd.h>
#include <unistd.h>

static int g_signal_fd = -1;

static void on_signal(event_loop_t *loop, int fd, uint32_t events, void *data) {
    (void)data; (void)events;
    struct signalfd_siginfo si;
    if (read(fd, &si, sizeof(si)) != sizeof(si)) return;

    if (si.ssi_signo == SIGTERM || si.ssi_signo == SIGINT) {
        printf("\nShutting down...\n");
        event_loop_stop(loop);
    }
}

static int setup_signals(event_loop_t *loop) {
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGTERM);
    sigaddset(&mask, SIGINT);
    sigprocmask(SIG_BLOCK, &mask, NULL);

    g_signal_fd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (g_signal_fd < 0) return -1;

    event_loop_add(loop, g_signal_fd, EV_READ, on_signal, NULL);
    signal(SIGPIPE, SIG_IGN);
    return 0;
}

static void cleanup_signals(event_loop_t *loop) {
    if (g_signal_fd >= 0) {
        event_loop_del(loop, g_signal_fd);
        close(g_signal_fd);
    }
}

#else
// macOS/BSD fallback
static volatile sig_atomic_t g_shutdown = 0;

static void sighandler(int sig) { (void)sig; g_shutdown = 1; }

static int setup_signals(event_loop_t *loop) {
    (void)loop;
    signal(SIGTERM, sighandler);
    signal(SIGINT, sighandler);
    signal(SIGPIPE, SIG_IGN);
    return 0;
}

static void cleanup_signals(event_loop_t *loop) { (void)loop; }

static void check_shutdown(event_loop_t *loop) {
    if (g_shutdown) event_loop_stop(loop);
}
#endif

// ============================================================================
// Tick Timer (orchestration)
// ============================================================================

static void on_tick(event_loop_t *loop, void *data) {
    app_t *app = (app_t *)data;
    uint64_t now_ms = event_loop_now_ms(loop);
    uint32_t elapsed = (uint32_t)(now_ms - app->last_tick_ms);
    (void)elapsed;
    app->last_tick_ms = now_ms;

    // Tick Raft
    raft_tick(app->raft);

    // Detect term changes and notify server
    // This is CRITICAL for linearizability - pending reads must be
    // invalidated when term changes to prevent stale reads
    uint64_t current_term = raft_get_term(app->raft);
    if (current_term != app->last_term) {
        server_on_term_change(app->server, current_term);
        app->last_term = current_term;
    }

    // Notify ALR of applied entries
    server_on_apply(app->server, raft_get_last_applied(app->raft));

    // Process Raft network
    glue_process_network(&app->glue_ctx, app->raft);

    // Tick server (timeout sweep for both reads and writes)
    server_tick(app->server, now_ms);

    // Check leadership changes
    bool is_leader = raft_is_leader(app->raft);
    if (is_leader != app->was_leader) {
        server_on_leadership_change(app->server, is_leader);
        app->was_leader = is_leader;
    }

#ifndef __linux__
    check_shutdown(loop);
#endif
}

// ============================================================================
// Raft Callbacks (wire server hooks)
// ============================================================================

// Wrap the glue apply_entry to also notify server
static int apply_entry_wrapper(void *ctx, uint64_t index, uint64_t term,
                                raft_entry_type_t type, const void *data, size_t len) {
    // First, apply to storage via glue
    int ret = glue_apply_entry(ctx, index, term, type, data, len);

    // Then notify server of commit
    if (ret == 0 && g_app.server) {
        server_on_commit(g_app.server, index, term);
    }

    return ret;
}

static int log_truncate_wrapper(void *ctx, uint64_t index) {
    int ret = glue_log_truncate_after(ctx, index);

    if (g_app.server) {
        server_on_log_truncate(g_app.server, index);
    }

    return ret;
}

static void on_readindex_complete_wrapper(void *ctx, uint64_t req_id,
                                           uint64_t read_index, int err) {
    (void)ctx;
    if (g_app.server) {
        server_on_readindex_complete(g_app.server, req_id, read_index, err);
    }
}

static raft_callbacks_t make_callbacks(void) {
    raft_callbacks_t cb = glue_make_callbacks();
    // Override to add server notifications
    cb.apply_entry = apply_entry_wrapper;
    cb.log_truncate_after = log_truncate_wrapper;
    cb.on_readindex_complete = on_readindex_complete_wrapper;
    return cb;
}
// ============================================================================
// CLI
// ============================================================================

static void usage(const char *prog) {
    fprintf(stderr,
        "lygus-server v%s\n\n"
        "Usage: %s [OPTIONS]\n\n"
        "Options:\n"
        "  -n, --node-id=ID            Node ID (required)\n"
        "  -p, --peers=FILE            Peers file (required)\n"
        "  -d, --data-dir=PATH         Data directory (required)\n"
        "  -l, --listen=PORT           Client port (default: 8080)\n"
        "  -s, --snapshot-threshold=N  Entries before snapshot (default: 10000)\n"
        "  -v, --verbose               Verbose logging\n"
        "      --leader-only           Disable follower reads (benchmark baseline)\n"
        "  -h, --help                  Show help\n",
        LYGUS_VERSION, prog);
}

static int parse_args(int argc, char **argv, app_t *app) {
    static struct option opts[] = {
        {"node-id",            required_argument, 0, 'n'},
        {"peers",              required_argument, 0, 'p'},
        {"data-dir",           required_argument, 0, 'd'},
        {"listen",             required_argument, 0, 'l'},
        {"snapshot-threshold", required_argument, 0, 's'},
        {"verbose",            no_argument,       0, 'v'},
        {"leader-only",        no_argument,       0, 'L'},
        {"help",               no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    // Defaults
    app->port = 8080;
    app->snapshot_threshold = 10000;
    app->leader_only = false;

    int c;
    while ((c = getopt_long(argc, argv, "n:p:d:l:s:vh", opts, NULL)) != -1) {
        switch (c) {
            case 'n': app->node_id = atoi(optarg); break;
            case 'p': app->peers_file = optarg; break;
            case 'd': app->data_dir = optarg; break;
            case 'l': app->port = atoi(optarg); break;
            case 's': app->snapshot_threshold = (uint32_t)atoi(optarg); break;
            case 'v': app->verbose = 1; break;
            case 'L': app->leader_only = true; break;
            case 'h': usage(argv[0]); exit(0);
            default: return -1;
        }
    }

    if (app->node_id < 0 || !app->peers_file || !app->data_dir) {
        fprintf(stderr, "Error: --node-id, --peers, and --data-dir are required\n");
        return -1;
    }

    return 0;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char **argv) {
    int ret = 0;
    memset(&g_app, 0, sizeof(g_app));

    // --- Parse args ---
    if (parse_args(argc, argv, &g_app) < 0) {
        usage(argv[0]);
        return 1;
    }

    // --- Init logging ---
    char log_path[512];
    snprintf(log_path, sizeof(log_path), "%s/lygus.log", g_app.data_dir);
    lygus_log_init(log_path, (uint16_t)g_app.node_id);
    if (g_app.verbose) lygus_log_set_mode(LYGUS_LOG_DETAILED);

    // --- Create event loop ---
    g_app.loop = event_loop_create();
    if (!g_app.loop) {
        fprintf(stderr, "Failed to create event loop\n");
        ret = 1; goto cleanup_log;
    }

    // --- Setup signals ---
    if (setup_signals(g_app.loop) < 0) {
        fprintf(stderr, "Failed to setup signals\n");
        ret = 1; goto cleanup_loop;
    }

    // --- Init storage + glue ---
    if (glue_ctx_init(&g_app.glue_ctx, g_app.data_dir,
                      g_app.node_id, g_app.peers_file) != 0) {
        fprintf(stderr, "Failed to init storage\n");
        ret = 1; goto cleanup_signals;
    }

    // --- Load peers ---
    peer_info_t peers[16];
    int num_peers = network_load_peers(g_app.peers_file, peers, 16);
    if (num_peers < 0) {
        fprintf(stderr, "Failed to load peers\n");
        ret = 1; goto cleanup_glue;
    }

    // --- Create Raft ---
    raft_config_t raft_cfg = RAFT_CONFIG_DEFAULT;
    raft_cfg.flags |= RAFT_FLAG_AUTO_SNAPSHOT;
    raft_cfg.snapshot_threshold = g_app.snapshot_threshold;

    raft_callbacks_t callbacks = make_callbacks();
    g_app.raft = raft_create(g_app.node_id, num_peers, &raft_cfg, &callbacks, &g_app.glue_ctx);
    if (!g_app.raft) {
        fprintf(stderr, "Failed to create Raft\n");
        ret = 1; goto cleanup_glue;
    }

    printf("[Config] snapshot_threshold=%u, auto_snapshot=%s, leader_only=%s\n",
           g_app.snapshot_threshold,
           (raft_cfg.flags & RAFT_FLAG_AUTO_SNAPSHOT) ? "on" : "off",
           g_app.leader_only ? "on" : "off");

    // --- RESTORE LOG FROM WAL ---
    if (glue_restore_raft_log(&g_app.glue_ctx, g_app.raft) != 0) {
        fprintf(stderr, "Failed to restore Raft log\n");
        ret = 1; goto cleanup_raft;
    }

    // --- Start network ---
    if (glue_ctx_start_network(&g_app.glue_ctx) != 0) {
        fprintf(stderr, "Failed to start network\n");
        ret = 1; goto cleanup_raft;
    }

    // --- Create server (library component) ---
    // Server internally creates handler, which creates ALR + pending
    server_config_t srv_cfg = {
        .loop              = g_app.loop,
        .raft              = g_app.raft,
        .glue_ctx          = &g_app.glue_ctx,
        .storage           = g_app.glue_ctx.storage,
        .kv                = storage_mgr_get_kv(g_app.glue_ctx.storage),
        .port              = g_app.port,
        .leader_only_reads = g_app.leader_only,
        .version           = LYGUS_VERSION,
    };
    g_app.server = server_create(&srv_cfg);
    if (!g_app.server) {
        fprintf(stderr, "Failed to create server on port %d\n", g_app.port);
        ret = 1; goto cleanup_network;
    }


    // --- Setup tick timer ---
    g_app.last_tick_ms = event_loop_now_ms(g_app.loop);
    g_app.last_term = raft_get_term(g_app.raft);  // Initialize term tracking
    g_app.tick_timer = event_loop_timer_add(g_app.loop, 1, on_tick, &g_app);

    // --- Run ---
    printf("lygus-server v%s started (node %d, port %d%s)\n",
           LYGUS_VERSION, g_app.node_id, g_app.port,
           g_app.leader_only ? ", leader-only reads" : "");

    event_loop_run(g_app.loop);

    // --- Cleanup (reverse order) ---
    printf("Shutting down...\n");

    event_loop_timer_del(g_app.loop, g_app.tick_timer);
    server_destroy(g_app.server);

cleanup_network:
    glue_ctx_stop_network(&g_app.glue_ctx);

cleanup_raft:
    raft_destroy(g_app.raft);

cleanup_glue:
    glue_ctx_destroy(&g_app.glue_ctx);

cleanup_signals:
    cleanup_signals(g_app.loop);

cleanup_loop:
    event_loop_destroy(g_app.loop);

cleanup_log:
    lygus_log_shutdown();

    printf("Goodbye.\n");
    return ret;
}