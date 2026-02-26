/**
 * alr.h - Frontier Follower Reads (Lazy-ALR via Ack Stream)
 *
 * Implements linearizable reads from follower replicas using the
 * Lazy-ALR technique adapted for the frontier protocol.
 *
 * Sync mechanism:
 *   - Follower sends SYNC_REQ to leader
 *   - Leader responds with max_acked_seq (linearization point)
 *   - Follower waits for contiguous prefix >= max_acked_seq
 *   - Follower serves all reads in the batch
 *
 * Under write load, SYNC_REQ piggybacks on MSG_DAG_PUSH_FF_ACK
 * and the response piggybacks on MSG_DAG_PUSH — zero additional
 * messages.  Under quiet load, one explicit RTT.
 *
 * State machine (maps 1:1 to TLA+ spec):
 *   followerPendingReads  →  reads waiting for next sync
 *   followerSyncBatch     →  reads in current sync (closed)
 *   followerSyncTarget    →  max_acked_seq from leader, 0 = inactive
 *
 * CRITICAL: reads arriving after sync is sent go into pendingReads
 * (next batch), NOT syncBatch (current batch).  Mixing them breaks
 * linearizability — see §5A and TLA+ FollowerSendSync comments.
 */

#ifndef ALR_H
#define ALR_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "public/lygus_errors.h"

typedef struct alr alr_t;

// ============================================================================
// Stats
// ============================================================================

typedef struct {
    uint64_t reads_total;
    uint64_t reads_completed;
    uint64_t reads_failed;
    uint64_t reads_timeout;
    uint64_t syncs_sent;          // SYNC_REQ messages sent
    uint64_t syncs_piggybacked;   // Syncs piggybacked on ff-ack
    uint64_t batched;             // Reads batched onto existing sync

    uint16_t pending_count;       // Reads in pending queue
    uint16_t batch_count;         // Reads in active sync batch
    uint64_t contiguous_prefix;   // Current prefix watermark
} alr_stats_t;

// ============================================================================
// Callback
// ============================================================================

/**
 * Called when a batch of reads is ready to serve, or when reads fail.
 *
 * For success: handler.c does the DAG lookup + response, filtering
 *   to writes with leader_seq <= sync_target.
 * For failure: handler.c sends error to client.
 *
 * @param conn         Client connection
 * @param key          Key to read
 * @param klen         Key length
 * @param sync_target  leader's max_acked_seq (0 on error or empty)
 * @param err          LYGUS_OK = serve from DAG, anything else = fail
 * @param ctx          User context (handler_t*)
 */
typedef void (*alr_serve_fn)(void *conn, const void *key, size_t klen,
                              uint64_t sync_target, lygus_err_t err,
                              void *ctx);

// ============================================================================
// Configuration
// ============================================================================

typedef struct {
    alr_serve_fn  serve;          // Required: serve/fail callback
    void         *serve_ctx;      // Optional: callback context

    uint16_t capacity;            // Max pending reads (default: 4096)
    size_t   slab_size;           // Key slab size (default: 16MB)
    uint32_t timeout_ms;          // Read timeout in ms (default: 5000)
} alr_config_t;

// ============================================================================
// Lifecycle
// ============================================================================

alr_t *alr_create(const alr_config_t *cfg);
void   alr_destroy(alr_t *alr);

// ============================================================================
// Core Operations (called by handler.c)
// ============================================================================

/**
 * Queue a follower read.
 *
 * If no sync is in flight, the read goes into pending (next batch).
 * handler.c is responsible for calling alr_send_sync() afterward.
 *
 * @return LYGUS_OK or LYGUS_ERR_BATCH_FULL
 */
lygus_err_t alr_read(alr_t *alr, const void *key, size_t klen,
                      void *conn, uint64_t now_ms);

/**
 * Close the pending batch and mark sync as in-flight.
 *
 * Moves all pending reads → sync batch.  Returns true if there
 * are reads to sync (caller should send MSG_FOLLOWER_SYNC_REQ).
 * Returns false if no pending reads or a sync is already active.
 *
 * @param now_ms  Current time — used for sync-level timeout (Fix #2).
 *
 * Maps to TLA+ FollowerSendSync.
 */
bool alr_send_sync(alr_t *alr, uint64_t now_ms);

/**
 * Leader responded with max_acked_seq.
 *
 * If max_seq == 0: serve batch immediately (empty visible set).
 * If max_seq >  0: set target, wait for prefix gate.
 *
 * Maps to TLA+ FollowerReceiveSync.
 */
void alr_recv_sync(alr_t *alr, uint64_t max_seq);

/**
 * Notify that a DAG node with this leader_seq arrived (ff-push).
 *
 * Updates the contiguous prefix tracker.  If the prefix gate
 * is now satisfied, serves the batch.
 *
 * Called from handler.c on every ff-push delivery that has a
 * nonzero leader_seq.
 */
void alr_notify_seq(alr_t *alr, uint64_t seq);

/**
 * Sync failed (leader unreachable, term change, etc).
 * Fails all reads in both pending and batch queues.
 *
 * Maps to TLA+ FollowerSyncTimeout + election cleanup.
 */
void alr_flush(alr_t *alr, lygus_err_t err);

/**
 * Fix #4: Flush only the in-flight sync batch, keeping pending reads.
 * Use on term change — pending reads can join the next sync.
 *
 * Maps to TLA+ FollowerSyncTimeout (batch only).
 */
void alr_flush_sync(alr_t *alr, lygus_err_t err);

/**
 * Cancel all reads for a disconnected client.
 */
int alr_cancel_conn(alr_t *alr, void *conn);

/**
 * Sweep for timed-out reads.
 */
int alr_timeout_sweep(alr_t *alr, uint64_t now_ms);

/**
 * Is a sync currently in flight?
 * handler.c uses this to decide whether to piggyback on ff-ack.
 */
bool alr_sync_active(const alr_t *alr);

/**
 * Are there pending reads waiting for a sync?
 * handler.c uses this to decide whether to send SYNC_REQ.
 */
bool alr_has_pending(const alr_t *alr);

/**
 * Get the current contiguous prefix (for debugging/stats).
 */
uint64_t alr_get_prefix(const alr_t *alr);

void alr_get_stats(const alr_t *alr, alr_stats_t *out);

#endif // ALR_H