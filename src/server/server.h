/**
 * server.h - TCP server with DAG + Raft-backed request handling
 *
 * Usage unchanged from original. New DAG config fields are optional
 * and default to sensible values.
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

typedef struct event_loop event_loop_t;
typedef struct raft raft_t;
typedef struct raft_glue_ctx raft_glue_ctx_t;
typedef struct storage_mgr storage_mgr_t;
typedef struct lygus_kv lygus_kv_t;
typedef struct network network_t;
typedef struct server server_t;

// ============================================================================
// Configuration
// ============================================================================

typedef struct {
    // === Required: Injected dependencies ===
    event_loop_t    *loop;
    raft_t          *raft;
    raft_glue_ctx_t *glue_ctx;
    storage_mgr_t   *storage;
    lygus_kv_t      *kv;
    network_t       *net;             // Network layer (for gossip transport)

    // === Required: Network ===
    int              port;

    // === Optional: Network tuning (0 = default) ===
    int              backlog;
    const char      *bind_addr;

    // === Optional: Connection limits (0 = default) ===
    size_t           max_connections;
    size_t           max_request_size;
    size_t           initial_buffer_size;

    // === Optional: Request handling (0 = default) ===
    size_t           max_pending;
    uint32_t         request_timeout_ms;
    uint16_t         alr_capacity;
    size_t           alr_slab_size;
    uint32_t         alr_timeout_ms;

    // === DAG configuration (0 = default) ===
    int              node_id;         // This node's ID
    int              num_peers;       // Total peers in cluster
    size_t           dag_max_nodes;   // Max DAG nodes between commits (default: 65536)
    size_t           dag_arena_size;  // Arena bytes (default: 16MB)
    size_t           batch_buf_size;  // Batch serialization buffer (default: 8MB)

    // === Benchmark mode ===
    bool             leader_only_reads;

    // === Optional: Metadata ===
    const char      *version;
} server_config_t;

// ============================================================================
// Lifecycle
// ============================================================================

server_t *server_create(const server_config_t *config);
void server_destroy(server_t *srv);

// ============================================================================
// Event Hooks (caller invokes these)
// ============================================================================

/**
 * Periodic tick - call from timer callback
 *
 * Drains gossip inbox, runs gossip anti-entropy,
 * sweeps timeouts, and handles pending completions.
 */
void server_tick(server_t *srv, uint64_t now_ms);

/** Raft commit */
void server_on_commit(server_t *srv, uint64_t index, uint64_t term);

/** Raft apply */
void server_on_apply(server_t *srv, uint64_t last_applied);

/** Leadership change */
void server_on_leadership_change(server_t *srv, bool is_leader);

/** Term change */
void server_on_term_change(server_t *srv, uint64_t new_term);

/** Log truncation */
void server_on_log_truncate(server_t *srv, uint64_t from_index);

/** ReadIndex complete */
void server_on_readindex_complete(server_t *srv, uint64_t req_id,
                                   uint64_t read_index, int err);

/**
 * Apply a Raft log entry (called from glue layer)
 *
 * Checks if the entry is a DAG batch (first byte == 0xDA).
 * If so, deserializes and applies to KV via the handler.
 * If not, returns -1 (caller should use legacy apply path).
 *
 * @return 0 if DAG batch was applied, -1 if not a DAG batch
 */
int server_try_apply_entry(server_t *srv, const uint8_t *entry, size_t len);

/**
 * Flush pending DAG writes to Raft (leader only).
 *
 * Called from the glue layer when a ReadIndex request arrives from
 * a follower.  A ReadIndex IS an observation in the Hollow Purple
 * model — some node wants to read, so we must commit pending DAG
 * writes before responding with commit_index.
 */
void server_flush_dag(server_t *srv);

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

    // DAG stats
    uint64_t  dag_inserts;
    uint64_t  dag_batches_proposed;
    uint64_t  dag_batches_applied;
    size_t    dag_node_count;
} server_stats_t;

void server_get_stats(const server_t *srv, server_stats_t *out);
int server_connection_count(const server_t *srv);
size_t server_pending_count(const server_t *srv);
int server_get_port(const server_t *srv);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_SERVER_H