/**
 * alr.h - Almost Local Reads (Lazy-ALR for Raft)
 *
 * Implements linearizable reads from any replica using the Lazy-ALR
 * technique from "The LAW Theorem" paper.
 * You must call alr_on_read_index() when ReadIndex responses arrive.
 */

#ifndef ALR_H
#define ALR_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "raft.h"
#include "public/lygus_errors.h"

// Forward declarations
typedef struct alr alr_t;
typedef struct lygus_kv lygus_kv_t;

// ============================================================================
// Stats
// ============================================================================

typedef struct {
    uint64_t reads_total;       // Total reads queued
    uint64_t reads_completed;   // Successfully completed
    uint64_t reads_stale;       // Failed due to stale sync
    uint64_t reads_failed;      // Failed for other reasons
    uint64_t reads_timeout;     // Failed due to timeout

    uint64_t syncs_issued;      // NOOPs proposed (leader)
    uint64_t read_index_issued; // ReadIndex requests sent (follower)
    uint64_t piggybacks;        // Syncs piggybacked on writes
    uint64_t batched;           // Reads batched onto existing sync

    // Current state
    uint16_t pending_count;     // Reads currently queued
    size_t   slab_used;         // Current slab usage
    size_t   slab_high_water;   // Peak slab usage
    bool     pending_read_index; // ReadIndex request in flight
} alr_stats_t;

// ============================================================================
// Callbacks
// ============================================================================

/**
 * Response callback - called when a read completes or fails.
 *
 * @param conn   Connection handle (from alr_read)
 * @param key    Key that was read
 * @param klen   Key length
 * @param val    Value (NULL on error or not found)
 * @param vlen   Value length
 * @param err    Error code (LYGUS_OK on success)
 * @param ctx    User context
 */
typedef void (*alr_respond_fn)(void *conn, const void *key, size_t klen,
                                const void *val, size_t vlen,
                                lygus_err_t err, void *ctx);

// ============================================================================
// Configuration
// ============================================================================

typedef struct {
    raft_t         *raft;        // Required: Raft instance
    lygus_kv_t     *kv;          // Required: KV store
    alr_respond_fn  respond;     // Required: Response callback
    void           *respond_ctx; // Optional: Callback context

    uint16_t capacity;           // Max pending reads (default: 4096)
    size_t   slab_size;          // Key slab size (default: 16MB)
    uint32_t timeout_ms;         // Read timeout in ms (default: 5000)
} alr_config_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create an ALR instance.
 */
alr_t *alr_create(const alr_config_t *cfg);

/**
 * Destroy an ALR instance.
 * Any pending reads will NOT receive callbacks.
 */
void alr_destroy(alr_t *alr);

// ============================================================================
// Core Operations
// ============================================================================

/**
 * Queue a read request.
 *
 * The read will be executed once a sync point is established and applied.
 * Response delivered via the respond callback.
 *
 * @param alr     ALR instance
 * @param key     Key to read
 * @param klen    Key length
 * @param conn    Connection handle (passed to callback)
 * @param now_ms  Current monotonic time in milliseconds
 *
 * @return LYGUS_OK           - Read queued successfully
 *         LYGUS_ERR_BATCH_FULL - Buffer full, retry later
 *         LYGUS_ERR_TRY_LEADER - Follower can't serve, redirect to leader
 *         LYGUS_ERR_SYNC_FAILED - Failed to establish sync point
 */
lygus_err_t alr_read(alr_t *alr, const void *key, size_t klen, void *conn, uint64_t now_ms);

/**
 * Notify ALR that entries have been applied to the state machine.
 *
 * Call this from your Raft apply callback. This triggers completion
 * of reads whose sync point has been reached.
 *
 * @param alr           ALR instance
 * @param applied_index Highest applied log index
 */
void alr_notify(alr_t *alr, uint64_t applied_index);

/**
 * Handle ReadIndex response from leader.
 *
 * Call this when the Raft layer receives a ReadIndex response.
 *
 * @param alr     ALR instance
 * @param req_id  Request ID (from raft_request_read_index_async)
 * @param index   Read index returned by leader (ignored if err != 0)
 * @param err     0 on success, non-zero on failure
 */
void alr_on_read_index(alr_t *alr, uint64_t req_id, uint64_t index, int err);

/**
 * Notify ALR of a term change.
 *
 * Call this when the Raft term changes. Invalidates pending syncs
 * and fails reads waiting on ReadIndex.
 *
 * @param alr       ALR instance
 * @param new_term  The new term
 */
void alr_on_term_change(alr_t *alr, uint64_t new_term);

/**
 * Cancel all pending reads for a connection.
 *
 * Call this when a client disconnects. Callbacks will be invoked
 * with LYGUS_ERR_NET.
 *
 * @param alr   ALR instance
 * @param conn  Connection handle
 *
 * @return Number of reads cancelled
 */
int alr_cancel_conn(alr_t *alr, void *conn);

/**
 * Sweep for timed-out reads.
 *
 * Call this periodically (e.g., from tick handler). Fails reads
 * that have exceeded their deadline.
 *
 * @param alr     ALR instance
 * @param now_ms  Current monotonic time in milliseconds
 *
 * @return Number of reads timed out
 */
int alr_timeout_sweep(alr_t *alr, uint64_t now_ms);

/**
 * Get statistics.
 */
void alr_get_stats(const alr_t *alr, alr_stats_t *out);

#endif // ALR_H