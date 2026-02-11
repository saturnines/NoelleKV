/**
 * server.c - TCP server implementation with DAG integration
 *
 * Changes from original:
 *   - Passes DAG config + network handle to handler
 *   - server_tick drains gossip inbox and dispatches to handler
 *   - server_try_apply_entry hooks DAG batch apply into glue layer
 */

#include "server.h"
#include "handler.h"
#include "conn.h"
#include "protocol.h"
#include "network/network.h"
#include "dag_entry.h"
#include "event/event_loop.h"
#include "merkle_dag.h"
#include "dag_serial.h"
#include "state/kv_store.h"
#include "storage/storage_mgr.h"

#include "platform/platform.h"
#include "public/lygus_errors.h"

#include <stdlib.h>
#include <string.h>

// ============================================================================
// Defaults
// ============================================================================

#define DEFAULT_BACKLOG          64
#define DEFAULT_MAX_CONNECTIONS  1024
#define DEFAULT_MAX_REQUEST      (1024 * 1024)
#define DEFAULT_BUFFER_SIZE      4096
#define GOSSIP_RECV_BUF_SIZE     65536
#define MAX_GOSSIP_PER_TICK      64    // Don't starve the event loop

// ============================================================================
// Connection Tracking
// ============================================================================

typedef struct conn_node {
    conn_t           *conn;
    struct conn_node *next;
    struct conn_node *prev;
} conn_node_t;

// ============================================================================
// Internal Structure
// ============================================================================

struct server {
    // Dependencies (borrowed)
    event_loop_t    *loop;
    raft_t          *raft;
    raft_glue_ctx_t *glue_ctx;
    storage_mgr_t   *storage;
    network_t       *net;

    // Listen socket
    lygus_socket_t   listen_sock;
    int              port;

    // Handler (owned)
    handler_t       *handler;

    // Connection list
    conn_node_t     *conns;
    int              num_conns;
    int              max_conns;

    // Config for new connections
    conn_config_t    conn_cfg;

    // Gossip recv buffer (owned)
    uint8_t         *gossip_buf;

    // Stats
    uint64_t         conns_total;
    uint64_t         conns_rejected;
};

// ============================================================================
// Forward Declarations
// ============================================================================

static void on_accept(event_loop_t *loop, int fd, uint32_t events, void *data);
static void on_conn_message(conn_t *conn, const char *data, size_t len, void *ctx);
static void on_conn_close(conn_t *conn, void *ctx);

// ============================================================================
// Connection List Helpers
// ============================================================================

static conn_node_t *conn_list_add(server_t *srv, conn_t *conn) {
    conn_node_t *node = calloc(1, sizeof(*node));
    if (!node) return NULL;

    node->conn = conn;
    node->next = srv->conns;
    node->prev = NULL;

    if (srv->conns) {
        srv->conns->prev = node;
    }
    srv->conns = node;
    srv->num_conns++;

    return node;
}

static void conn_list_remove(server_t *srv, conn_node_t *node) {
    if (!node) return;

    if (node->prev) {
        node->prev->next = node->next;
    } else {
        srv->conns = node->next;
    }

    if (node->next) {
        node->next->prev = node->prev;
    }

    srv->num_conns--;
    free(node);
}

static conn_node_t *conn_list_find(server_t *srv, conn_t *conn) {
    for (conn_node_t *n = srv->conns; n; n = n->next) {
        if (n->conn == conn) return n;
    }
    return NULL;
}

// ============================================================================
// DAG Batch Replay for WAL Recovery (Fix #4)
// ============================================================================

typedef struct {
    lygus_kv_t *kv;
} dag_replay_ctx_t;

/**
 * Apply a single DAG node to the KV store during WAL replay.
 * Same logic as handler.c:apply_node_to_kv but operates on an
 * arbitrary KV pointer (which may be a freshly-loaded snapshot).
 */
static void replay_apply_node(dag_node_t *node, void *ctx) {
    dag_replay_ctx_t *rc = (dag_replay_ctx_t *)ctx;
    if (!node || node->value_len == 0) return;

    uint8_t op = node->value[0];
    if (op == DAG_OP_PUT && node->value_len > 1) {
        lygus_kv_put(rc->kv, node->key, node->key_len,
                     node->value + 1, node->value_len - 1);
    } else if (op == DAG_OP_DEL) {
        lygus_kv_del(rc->kv, node->key, node->key_len);
    }
}

/**
 * Callback for storage_mgr WAL replay of DAG batch entries.
 *
 * Receives the raw WAL value (which is the full Raft entry including
 * the 0xDA marker), deserializes into a temp DAG, topo-sorts, and
 * applies each operation to the provided KV store.
 */
static int dag_replay_callback(const uint8_t *entry, size_t len,
                                lygus_kv_t *kv, void *ctx) {
    (void)ctx;

    if (!entry || len == 0 || !kv) return -1;

    // The WAL value is the raw Raft entry: [0xDA][batch_payload...]
    if (!dag_entry_is_batch(entry, len)) return -1;

    const uint8_t *payload = dag_entry_batch_payload(entry);
    size_t payload_len = dag_entry_batch_len(len);
    if (payload_len < 4) return -1;

    // Temp DAG for deserialization (small — only for this single batch)
    merkle_dag_t *tmp = dag_create(65536, 16 * 1024 * 1024);
    if (!tmp) return -1;

    int count = dag_deserialize_batch(tmp, payload, payload_len);
    if (count < 0) {
        dag_destroy(tmp);
        return -1;
    }

    // Apply in deterministic topo order
    dag_replay_ctx_t rc = { .kv = kv };
    dag_iter_topo(tmp, replay_apply_node, &rc);

    dag_destroy(tmp);
    return 0;
}

// ============================================================================
// Lifecycle
// ============================================================================

server_t *server_create(const server_config_t *cfg) {
    if (!cfg || !cfg->loop || !cfg->raft || !cfg->storage || !cfg->kv || cfg->port <= 0) {
        return NULL;
    }

    if (lygus_socket_init() < 0) {
        return NULL;
    }

    server_t *srv = calloc(1, sizeof(*srv));
    if (!srv) return NULL;

    srv->loop = cfg->loop;
    srv->raft = cfg->raft;
    srv->glue_ctx = cfg->glue_ctx;
    srv->storage = cfg->storage;
    srv->net = cfg->net;
    srv->port = cfg->port;
    srv->max_conns = cfg->max_connections > 0 ? (int)cfg->max_connections : DEFAULT_MAX_CONNECTIONS;
    srv->listen_sock = LYGUS_INVALID_SOCKET;

    // Gossip recv buffer
    srv->gossip_buf = malloc(GOSSIP_RECV_BUF_SIZE);
    if (!srv->gossip_buf) {
        free(srv);
        return NULL;
    }

    // Connection config
    srv->conn_cfg.read_buf_init = cfg->initial_buffer_size > 0 ?
                                  cfg->initial_buffer_size : DEFAULT_BUFFER_SIZE;
    srv->conn_cfg.write_buf_init = srv->conn_cfg.read_buf_init;
    srv->conn_cfg.read_buf_max = cfg->max_request_size > 0 ?
                                 cfg->max_request_size : DEFAULT_MAX_REQUEST;
    srv->conn_cfg.on_message = on_conn_message;
    srv->conn_cfg.on_close = on_conn_close;
    srv->conn_cfg.ctx = srv;

    // Create handler with DAG config
    handler_config_t handler_cfg = {
        .loop = cfg->loop,
        .raft = cfg->raft,
        .glue_ctx = cfg->glue_ctx,
        .storage = cfg->storage,
        .kv = cfg->kv,
        .net = cfg->net,
        .node_id = cfg->node_id,
        .num_peers = cfg->num_peers,
        .dag_max_nodes = cfg->dag_max_nodes,
        .dag_arena_size = cfg->dag_arena_size,
        .batch_buf_size = cfg->batch_buf_size,
        .max_pending = cfg->max_pending,
        .request_timeout_ms = cfg->request_timeout_ms,
        .alr_capacity = cfg->alr_capacity,
        .alr_slab_size = cfg->alr_slab_size,
        .alr_timeout_ms = cfg->alr_timeout_ms,
        .leader_only_reads = cfg->leader_only_reads,
        .version = cfg->version,
    };

    srv->handler = handler_create(&handler_cfg);
    if (!srv->handler) {
        free(srv->gossip_buf);
        free(srv);
        return NULL;
    }

    // Register DAG batch replay callback so that WAL replay
    // (triggered by log truncation below applied_index) correctly
    // deserializes + topo-sorts DAG batches instead of storing them
    // as raw blobs under the __dag__ sentinel key.
    if (srv->storage) {
        storage_mgr_set_dag_replay(srv->storage, dag_replay_callback, NULL);
    }

    // Create listen socket
    lygus_socket_t sock = lygus_socket_tcp();
    if (sock == LYGUS_INVALID_SOCKET) {
        handler_destroy(srv->handler);
        free(srv->gossip_buf);
        free(srv);
        return NULL;
    }

    lygus_socket_set_reuseaddr(sock);
    lygus_socket_set_nonblocking(sock);

    if (lygus_socket_bind(sock, cfg->bind_addr, (uint16_t)cfg->port) < 0) {
        lygus_socket_close(sock);
        handler_destroy(srv->handler);
        free(srv->gossip_buf);
        free(srv);
        return NULL;
    }

    int backlog = cfg->backlog > 0 ? cfg->backlog : DEFAULT_BACKLOG;
    if (lygus_socket_listen(sock, backlog) < 0) {
        lygus_socket_close(sock);
        handler_destroy(srv->handler);
        free(srv->gossip_buf);
        free(srv);
        return NULL;
    }

    srv->listen_sock = sock;

    if (event_loop_add(cfg->loop, lygus_socket_to_fd(sock), EV_READ, on_accept, srv) < 0) {
        lygus_socket_close(sock);
        handler_destroy(srv->handler);
        free(srv->gossip_buf);
        free(srv);
        return NULL;
    }

    return srv;
}

void server_destroy(server_t *srv) {
    if (!srv) return;

    while (srv->conns) {
        conn_node_t *node = srv->conns;
        handler_on_conn_close(srv->handler, node->conn);
        conn_destroy(node->conn);
        conn_list_remove(srv, node);
    }

    if (srv->listen_sock != LYGUS_INVALID_SOCKET) {
        event_loop_del(srv->loop, lygus_socket_to_fd(srv->listen_sock));
        lygus_socket_close(srv->listen_sock);
    }

    handler_destroy(srv->handler);
    free(srv->gossip_buf);
    free(srv);
}

// ============================================================================
// Accept Handler
// ============================================================================

static void on_accept(event_loop_t *loop, int fd, uint32_t events, void *data) {
    (void)fd;
    (void)events;
    server_t *srv = (server_t *)data;

    while (1) {
        char addr_str[64];
        lygus_socket_t client_sock = lygus_socket_accept(srv->listen_sock,
                                                         addr_str, sizeof(addr_str));

        if (client_sock == LYGUS_INVALID_SOCKET) {
            break;
        }

        srv->conns_total++;

        if (srv->num_conns >= srv->max_conns) {
            lygus_socket_close(client_sock);
            srv->conns_rejected++;
            continue;
        }

        lygus_socket_set_nonblocking(client_sock);
        lygus_socket_set_nodelay(client_sock);

        conn_t *conn = conn_create(loop, client_sock, addr_str, &srv->conn_cfg);
        if (!conn) {
            lygus_socket_close(client_sock);
            srv->conns_rejected++;
            continue;
        }

        if (!conn_list_add(srv, conn)) {
            conn_destroy(conn);
            srv->conns_rejected++;
            continue;
        }
    }
}

// ============================================================================
// Connection Callbacks
// ============================================================================

static void on_conn_message(conn_t *conn, const char *data, size_t len, void *ctx) {
    server_t *srv = (server_t *)ctx;
    handler_process(srv->handler, conn, data, len);
}

static void on_conn_close(conn_t *conn, void *ctx) {
    server_t *srv = (server_t *)ctx;

    handler_on_conn_close(srv->handler, conn);

    conn_node_t *node = conn_list_find(srv, conn);
    if (node) {
        conn_list_remove(srv, node);
    }
}

// ============================================================================
// Gossip Drain
// ============================================================================

/**
 * Drain gossip inbox, up to `limit` messages.
 * Pass 0 for unlimited (drain everything).
 */
static void drain_gossip_n(server_t *srv, int limit) {
    if (!srv->net) return;

    for (int i = 0; limit == 0 || i < limit; i++) {
        int from_id = -1;
        uint8_t msg_type = 0;

        int len = network_recv_gossip(srv->net, &from_id, &msg_type,
                                       srv->gossip_buf, GOSSIP_RECV_BUF_SIZE);

        if (len <= 0) break;  // No more gossip messages

        handler_on_gossip(srv->handler, from_id, msg_type,
                          srv->gossip_buf, (size_t)len);
    }
}

// ============================================================================
// Event Hooks
// ============================================================================

void server_tick(server_t *srv, uint64_t now_ms) {
    if (!srv) return;

    // Drain gossip inbox (bounded, don't starve event loop)
    drain_gossip_n(srv, MAX_GOSSIP_PER_TICK);

    // Handler tick (timeouts, gossip anti-entropy)
    handler_tick(srv->handler, now_ms);
}

void server_on_commit(server_t *srv, uint64_t index, uint64_t term) {
    if (!srv) return;
    handler_on_commit(srv->handler, index, term);
}

void server_on_apply(server_t *srv, uint64_t last_applied) {
    if (!srv) return;
    handler_on_apply(srv->handler, last_applied);
}

void server_on_leadership_change(server_t *srv, bool is_leader) {
    if (!srv) return;
    handler_on_leadership_change(srv->handler, is_leader);
}

void server_on_term_change(server_t *srv, uint64_t new_term) {
    if (!srv) return;
    handler_on_term_change(srv->handler, new_term);
}

void server_on_log_truncate(server_t *srv, uint64_t from_index) {
    if (!srv) return;
    handler_on_log_truncate(srv->handler, from_index);
}

void server_on_readindex_complete(server_t *srv, uint64_t req_id,
                                   uint64_t read_index, int err) {
    if (!srv) return;
    handler_on_readindex_complete(srv->handler, req_id, read_index, err);
}

/**
 * Try to apply a Raft log entry as a DAG batch.
 *
 * Call this from your glue layer's apply_entry callback:
 *
 *   void apply_entry(const uint8_t *entry, size_t len, void *ctx) {
 *       server_t *srv = (server_t *)ctx;
 *       if (server_try_apply_entry(srv, entry, len) == 0) {
 *           return;  // DAG batch applied
 *       }
 *       // Legacy apply path (existing glue_deserialize_put/del)
 *       ...
 *   }
 */
int server_try_apply_entry(server_t *srv, const uint8_t *entry, size_t len) {
    if (!srv || !entry) return -1;

    if (!dag_entry_is_batch(entry, len)) {
        return -1;  // Not a DAG batch, use legacy path
    }

    return handler_apply_dag_batch(srv->handler, entry, len);
}

/**
 * Flush pending DAG writes to Raft (if leader).
 *
 * Called from the glue layer when a ReadIndex request arrives from
 * a follower.  A ReadIndex IS an observation — some node wants to
 * read, so we must commit pending writes before responding with
 * commit_index.
 *
 * Add to server.h:
 *   void server_flush_dag(server_t *srv);
 */
void server_flush_dag(server_t *srv) {
    if (!srv) return;
    // UNBOUNDED drain: every push that has reached the mailbox MUST be
    // in the DAG before we propose.  This is the drain-before-flush
    // invariant — the foundation of the linearizability argument.
    drain_gossip_n(srv, 0);
    handler_flush_dag(srv->handler);
}

// ============================================================================
// Stats
// ============================================================================

void server_get_stats(const server_t *srv, server_stats_t *out) {
    if (!srv || !out) return;

    memset(out, 0, sizeof(*out));

    out->connections_active = srv->num_conns;
    out->connections_total = srv->conns_total;
    out->connections_rejected = srv->conns_rejected;

    handler_stats_t h_stats;
    handler_get_stats(srv->handler, &h_stats);

    out->requests_total = h_stats.requests_total;
    out->requests_ok = h_stats.requests_ok;
    out->requests_error = h_stats.requests_error;
    out->requests_timeout = h_stats.requests_timeout;
    out->reads_total = h_stats.reads_total;
    out->writes_total = h_stats.writes_total;
    out->writes_pending = h_stats.writes_pending;

    out->dag_inserts = h_stats.dag_inserts;
    out->dag_batches_proposed = h_stats.dag_batches_proposed;
    out->dag_batches_applied = h_stats.dag_batches_applied;
    out->dag_node_count = h_stats.dag_node_count;
}

int server_connection_count(const server_t *srv) {
    return srv ? srv->num_conns : 0;
}

size_t server_pending_count(const server_t *srv) {
    if (!srv) return 0;
    handler_stats_t stats;
    handler_get_stats(srv->handler, &stats);
    return stats.writes_pending;
}

int server_get_port(const server_t *srv) {
    return srv ? srv->port : 0;
}