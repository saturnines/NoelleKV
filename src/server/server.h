/**
 * server.h - TCP server with Raft-backed request handling
 *
 * Library component - does NOT own event loop, Raft, or storage.
 * Caller creates and manages those, passes them in via config.
 *
 * Usage:
 *   // Caller creates dependencies
 *   event_loop_t *loop = event_loop_create();
 *   raft_t *raft = raft_create(...);
 *   storage_mgr_t *storage = ...;
 *
 *   // Pass to server
 *   server_config_t cfg = {
 *       .loop = loop,
 *       .raft = raft,
 *       .storage = storage,
 *       .port = 8080,
 *   };
 *   server_t *srv = server_create(&cfg);
 *
 *   // Caller runs the loop and calls server_tick, server_on_* as needed
 *   event_loop_run(loop);
 *
 *   // Caller cleans up in reverse order
 *   server_destroy(srv);
 *   raft_destroy(raft);
 *   event_loop_destroy(loop);
 */

#ifndef LYGUS_SERVER_H
#define LYGUS_SERVER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Forward Declarations
// ============================================================================

// CALLER OWNS THIS
typedef struct event_loop event_loop_t;
typedef struct raft raft_t;
typedef struct raft_glue_ctx raft_glue_ctx_t;
typedef struct storage_mgr storage_mgr_t;
typedef struct lygus_kv lygus_kv_t;

// Internal
typedef struct server server_t;

// ============================================================================
// Configuration
// ============================================================================

/**
 * Server configuration
 *
 * Pointer fields are borrowed, not owned. Caller ensures they
 * outlive the server.
 */
typedef struct {
    // === Required: Injected dependencies ===
    event_loop_t    *loop;
    raft_t          *raft;
    raft_glue_ctx_t *glue_ctx;
    storage_mgr_t   *storage;
    lygus_kv_t      *kv;              // for linearizable reads

    // === Required: Network ===
    int              port;

    // === Optional: Network tuning (0 = default) ===
    int              backlog;             // default: 64
    const char      *bind_addr;           // default: all interfaces

    // === Optional: Connection limits (0 = default) ===
    size_t           max_connections;     // default: 256
    size_t           max_request_size;    // default: 1MB
    size_t           initial_buffer_size; // default: 4KB

    // === Optional: Request handling (0 = default) ===
    size_t           max_pending;         // default: 1024
    uint32_t         request_timeout_ms;  // default: 5000
    uint16_t         alr_capacity;        // default: 4096
    size_t           alr_slab_size;       // default: 16MB
    uint32_t         alr_timeout_ms;      // default: 5000

    // === Benchmark mode ===
    bool             leader_only_reads;   // Disable ALR, force reads through leader

    // === Optional: Metadata ===
    const char      *version;
} server_config_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create server
 *
 * Binds to port and registers with event loop.
 *
 * @param config  Configuration (pointers borrowed, not copied)
 * @return Server handle, or NULL on error
 */
server_t *server_create(const server_config_t *config);

/**
 * Destroy server
 *
 * Closes connections, fails pending requests, removes from event loop.
 * Does NOT destroy injected dependencies.
 */
void server_destroy(server_t *srv);

// ============================================================================
// Event Hooks (caller invokes these)
// ============================================================================

/**
 * Periodic tick - call from timer callback
 *
 * @param srv     Server
 * @param now_ms  Current monotonic time
 */
void server_tick(server_t *srv, uint64_t now_ms);

/**
 * Raft commit - call from apply_entry callback
 */
void server_on_commit(server_t *srv, uint64_t index, uint64_t term);

/**
 * Raft apply - call when entries are applied to state machine
 */
void server_on_apply(server_t *srv, uint64_t last_applied);

/**
 * Leadership change - call when raft state changes
 */
void server_on_leadership_change(server_t *srv, bool is_leader);

/**
 * Term change - call when raft term changes
 *
 * This invalidates all pending reads to preserve linearizability.
 */
void server_on_term_change(server_t *srv, uint64_t new_term);

/**
 * Log truncation - call from log_truncate_after callback
 */
void server_on_log_truncate(server_t *srv, uint64_t from_index);

/**
 * ReadIndex complete - call when ReadIndex RPC response arrives
 */
void server_on_readindex_complete(server_t *srv, uint64_t req_id,
                                   uint64_t read_index, int err);

// ============================================================================
// Stats
// ============================================================================

typedef struct {
    int       connections_active;
    uint64_t  connections_total;
    uint64_t  connections_rejected;

    uint64_t  requests_total;
    uint64_t  requests_ok;
    uint64_t  requests_error;
    uint64_t  requests_timeout;

    uint64_t  reads_total;
    uint64_t  writes_total;
    uint64_t  writes_pending;
} server_stats_t;

void server_get_stats(const server_t *srv, server_stats_t *out);
int server_connection_count(const server_t *srv);
size_t server_pending_count(const server_t *srv);
int server_get_port(const server_t *srv);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_SERVER_H