/**
 * server.h - TCP server with DAG + Raft-backed request handling
 *
 * Frontier design: reads served from leader DAG via quorum ping,
 * writes via bilateral durable replication. Raft is background GC only.
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
    network_t       *net;

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

    // === DAG configuration (0 = default) ===
    int              node_id;
    int              num_peers;
    size_t           dag_max_nodes;
    size_t           dag_arena_size;
    size_t           batch_buf_size;
    const char      *dag_arena_path;
    bool             dag_msync_enabled;

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
 * sweeps timeouts, and runs background drain (GC).
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

/** ReadIndex complete (no-op in frontier — kept for glue layer compat) */
void server_on_readindex_complete(server_t *srv, uint64_t req_id,
                                   uint64_t read_index, int err);

/**
 * Apply a Raft log entry (called from glue layer)
 *
 * Checks if the entry is a DAG batch (first byte == 0xDA).
 * If so, deserializes and applies to KV via the handler.
 * If not, returns -1 (caller should use legacy apply path).
 */
int server_try_apply_entry(server_t *srv, const uint8_t *entry, size_t len);

/**
 * Flush pending DAG writes (legacy — no-op in frontier).
 * Background drain in handler_tick replaces this.
 */
void server_flush_dag(server_t *srv);

/** Reset DAG after snapshot install (drain gossip inbox first) */
void server_reset_dag(server_t *srv);

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