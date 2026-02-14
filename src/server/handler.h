/**
 * handler.h - Request handling with DAG + Raft integration
 *
 * Write path:  client PUT/DEL → DAG insert → immediate ack → gossip push
 * Read path:   client GET → leader proposes DAG batch → ALR → serve from KV
 * Apply path:  Raft commits batch → deserialize → topo sort → apply to KV
 */

#ifndef LYGUS_HANDLER_H
#define LYGUS_HANDLER_H

#include "conn.h"
#include "protocol.h"
#include "pending.h"

#include <stdint.h>
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
typedef struct handler handler_t;
typedef struct network network_t;

// DAG types (included directly — merkle_dag_t is an anonymous struct, can't forward-declare)
#include "merkle_dag.h"
#include "gossip.h"

// ============================================================================
// Configuration
// ============================================================================

/** Callback to drain gossip inbox before proposing a batch */
typedef void (*handler_drain_fn)(void *ctx);

/**
 * Handler config
 *
 * All pointers borrowed, not owned.
 */
typedef struct {
    // Dependencies (required)
    event_loop_t    *loop;
    raft_t          *raft;
    raft_glue_ctx_t *glue_ctx;
    storage_mgr_t   *storage;
    lygus_kv_t      *kv;
    network_t       *net;             // Network layer for gossip
    handler_drain_fn drain_gossip;    // Drain gossip inbox (called before leader propose)
    void            *drain_gossip_ctx;

    // DAG configuration
    size_t           dag_max_nodes;   // Max DAG nodes between commits (default: 65536)
    size_t           dag_arena_size;  // Arena for keys/values (default: 16MB)
    size_t           batch_buf_size;  // Scratch for batch serialization (default: 8MB)
    int              node_id;         // This node's ID (for gossip)
    int              num_peers;       // Total peers in cluster

    // Limits
    size_t           max_pending;
    size_t           max_key_size;
    size_t           max_value_size;
    uint32_t         request_timeout_ms;

    // ALR config
    uint16_t         alr_capacity;
    size_t           alr_slab_size;
    uint32_t         alr_timeout_ms;

    // Benchmark mode: disable ALR, force all reads through leader
    bool             leader_only_reads;

    // Metadata
    const char      *version;
} handler_config_t;

// ============================================================================
// Lifecycle
// ============================================================================

handler_t *handler_create(const handler_config_t *config);
void handler_destroy(handler_t *h);

// ============================================================================
// Request Processing
// ============================================================================

/**
 * Process request from connection
 */
void handler_process(handler_t *h, conn_t *conn, const char *line, size_t len);

// ============================================================================
// Event Hooks
// ============================================================================

/** Raft committed an entry */
void handler_on_commit(handler_t *h, uint64_t index, uint64_t term);

/** Raft applied an entry */
void handler_on_apply(handler_t *h, uint64_t last_applied);

/** Leadership changed */
void handler_on_leadership_change(handler_t *h, bool is_leader);

/** Log was truncated */
void handler_on_log_truncate(handler_t *h, uint64_t from_index);

/** Connection closed */
void handler_on_conn_close(handler_t *h, conn_t *conn);

/** Term changed - invalidates pending reads */
void handler_on_term_change(handler_t *h, uint64_t new_term);

/** Periodic maintenance (call every tick) */
void handler_tick(handler_t *h, uint64_t now_ms);

/** ReadIndex response received */
void handler_on_readindex_complete(handler_t *h, uint64_t req_id,
                                    uint64_t read_index, int err);

/** Reset DAG state (snapshot install path) */
void handler_reset_dag(handler_t *h);
// ============================================================================
// DAG + Gossip Hooks
// ============================================================================

/**
 * Handle incoming gossip message from network
 *
 * Called when a MSG_GOSSIP_* or MSG_DAG_PUSH message arrives.
 *
 * @param h         Handler
 * @param from_peer Source peer ID
 * @param msg_type  Message type (30-35)
 * @param data      Payload
 * @param len       Payload length
 */
void handler_on_gossip(handler_t *h, int from_peer, uint8_t msg_type,
                       const uint8_t *data, size_t len);

/**
 * Apply a committed DAG batch entry to the KV state machine
 *
 * Called from the Raft glue layer's apply_entry when it detects a
 * DAG batch entry (first byte == 0xDA).
 *
 * @param h      Handler
 * @param entry  Raw Raft log entry
 * @param len    Entry length
 * @return 0 on success, -1 on error
 */
int handler_apply_dag_batch(handler_t *h, const uint8_t *entry, size_t len);

/**
 * Get the DAG (for external access, e.g. stats)
 */
merkle_dag_t *handler_get_dag(const handler_t *h);

/**
 * Flush pending DAG writes to Raft (leader only).
 * Called when a ReadIndex arrives — an observation that triggers commitment.
 */
void handler_flush_dag(handler_t *h);

// ============================================================================
// Stats
// ============================================================================

typedef struct {
    uint64_t requests_total;
    uint64_t requests_ok;
    uint64_t requests_error;
    uint64_t requests_timeout;
    uint64_t reads_total;
    uint64_t writes_total;
    uint64_t writes_pending;
    uint64_t reads_pending;

    // DAG stats
    uint64_t dag_inserts;         // Total writes absorbed by DAG
    uint64_t dag_batches_proposed; // Batches proposed to Raft
    uint64_t dag_batches_applied;  // Batches applied from Raft log
    uint64_t dag_nodes_gossiped;   // Nodes sent via push-on-write
    size_t   dag_node_count;       // Current DAG size (between commits)
} handler_stats_t;

void handler_get_stats(const handler_t *h, handler_stats_t *out);
size_t handler_pending_count(const handler_t *h);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_HANDLER_H