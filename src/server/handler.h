/**
 * handler.h - Request handling and Raft integration
 *
 * Internal to server library. Wires together:
 *   - conn.h (connections)
 *   - protocol.h (parsing)
 *   - pending.h (tracking)
 *   - Raft (consensus)
 *   - Storage (reads)
 *
 * This is NOT and should not be a public API, server.h exposes the clean interface.
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

// ============================================================================
// Configuration
// ============================================================================

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
    lygus_kv_t      *kv;              // KV store for linearizable reads

    // Limits
    size_t           max_pending;
    size_t           max_key_size;
    size_t           max_value_size;
    uint32_t         request_timeout_ms;

    // ALR (Asynchronous Linearizable Reads) config
    uint16_t         alr_capacity;
    size_t           alr_slab_size;
    uint32_t         alr_timeout_ms;  // Read timeout (default: 5000ms)

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
 *
 * Called from conn's on_message callback.
 */
void handler_process(handler_t *h, conn_t *conn, const char *line, size_t len);

// ============================================================================
// Event Hooks
// ============================================================================

/** Raft committed an entry */
void handler_on_commit(handler_t *h, uint64_t index, uint64_t term);

/** Raft applied an entry*/
void handler_on_apply(handler_t *h, uint64_t last_applied);

/** Leadership changed */
void handler_on_leadership_change(handler_t *h, bool is_leader);

/** Log was truncated */
void handler_on_log_truncate(handler_t *h, uint64_t from_index);

/** Connection closed */
void handler_on_conn_close(handler_t *h, conn_t *conn);

/** Term changed - invalidates pending reads */
void handler_on_term_change(handler_t *h, uint64_t new_term);

/** Periodic maintenance */
void handler_tick(handler_t *h, uint64_t now_ms);

/** ReadIndex response received */
void handler_on_readindex_complete(handler_t *h, uint64_t req_id,
                                    uint64_t read_index, int err);

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
} handler_stats_t;

void handler_get_stats(const handler_t *h, handler_stats_t *out);
size_t handler_pending_count(const handler_t *h);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_HANDLER_H