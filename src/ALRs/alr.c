/**
 * alr.c - Frontier Follower Reads (Lazy-ALR via Ack Stream)
 *
 * State machine (maps 1:1 to TLA+ MalevolentShrineFrontier spec):
 *
 *   pending[]  = followerPendingReads  — reads waiting for next sync
 *   batch[]    = followerSyncBatch     — reads in current sync (closed)
 *   sync_target = followerSyncTarget   — max_acked_seq, 0 = inactive
 *
 * Prefix tracker:
 *   leader_seq = (term << 32) | counter.  Counters are dense across
 *   terms because sync_complete reconstructs from max existing.
 *   We track the contiguous prefix over the counter (lower 32 bits).
 *   Gap buffer handles out-of-order ff-push delivery.
 *
 * Memory layout:
 *   Two flat arrays (pending, batch) swapped on send_sync.
 *   Keys live in a bump-allocated slab, reset when both arrays empty.
 */

#include "alr.h"

#include <stdlib.h>
#include <string.h>

// ============================================================================
// Defaults
// ============================================================================

#define ALR_DEFAULT_CAPACITY    4096
#define ALR_DEFAULT_SLAB_SIZE   (16 * 1024 * 1024)
#define ALR_DEFAULT_TIMEOUT_MS  5000

// Max out-of-order sequence numbers we can buffer.
// DAG size between drains is ~200 nodes, so 512 is generous.
#define ALR_GAP_BUF_SIZE        512

// ============================================================================
// Internal Types
// ============================================================================

typedef struct {
    void    *conn;
    void    *key;           // pointer into slab
    size_t   klen;
    uint64_t deadline_ms;
    bool     cancelled;     // connection closed before serve
} alr_read_entry_t;

// ============================================================================
// Internal Structure
// ============================================================================

struct alr {
    // --- Read queues (TLA+ followerPendingReads / followerSyncBatch) ---
    alr_read_entry_t *pending;      // next batch
    alr_read_entry_t *batch;        // current sync batch
    uint16_t pending_count;
    uint16_t batch_count;
    uint16_t capacity;              // shared cap for both arrays

    // --- Key slab (bump allocator, reset when both queues empty) ---
    uint8_t *slab;
    size_t   slab_size;
    size_t   slab_cursor;

    // --- Sync state ---
    bool     sync_in_flight;        // SYNC_REQ sent, waiting for response
    uint64_t sync_target;           // full leader_seq target (0 = waiting)

    // --- Contiguous prefix tracker ---
    //
    // prefix: highest k such that counters 1..k have all been seen.
    // gap_buf: sorted array of counters > prefix (out-of-order arrivals).
    //
    // O(1) amortized per notify_seq.  Binary search insert on gaps,
    // linear drain when a gap fills — but gap_count is bounded by
    // DAG size between drains (~200).
    uint32_t prefix;
    uint32_t gap_buf[ALR_GAP_BUF_SIZE];
    uint16_t gap_count;

    // --- Timing ---
    uint32_t timeout_ms;

    // --- Callback ---
    alr_serve_fn serve;
    void        *serve_ctx;

    // --- Stats ---
    alr_stats_t stats;
};

// ============================================================================
// Gap Buffer Helpers
// ============================================================================

/**
 * Insert a counter into the sorted gap buffer.
 * Duplicates are ignored.  Overflow is silently dropped (reads will
 * block until the missing seq arrives and the gate clears naturally).
 */
static void gap_insert(alr_t *alr, uint32_t counter) {
    if (alr->gap_count >= ALR_GAP_BUF_SIZE) return;

    // Binary search for insertion point
    uint16_t lo = 0, hi = alr->gap_count;
    while (lo < hi) {
        uint16_t mid = (lo + hi) / 2;
        if (alr->gap_buf[mid] < counter) lo = mid + 1;
        else if (alr->gap_buf[mid] == counter) return;  // dup
        else hi = mid;
    }

    // Shift right and insert
    if (lo < alr->gap_count) {
        memmove(&alr->gap_buf[lo + 1], &alr->gap_buf[lo],
                (alr->gap_count - lo) * sizeof(uint32_t));
    }
    alr->gap_buf[lo] = counter;
    alr->gap_count++;
}

/**
 * Drain consecutive counters from the front of the gap buffer
 * into the prefix.  E.g. if prefix=5 and gap_buf=[6,7,9],
 * prefix becomes 7 and gap_buf becomes [9].
 */
static void drain_gaps(alr_t *alr) {
    while (alr->gap_count > 0 && alr->gap_buf[0] == alr->prefix + 1) {
        alr->prefix++;
        alr->gap_count--;
        if (alr->gap_count > 0) {
            memmove(&alr->gap_buf[0], &alr->gap_buf[1],
                    alr->gap_count * sizeof(uint32_t));
        }
    }
}

// ============================================================================
// Internal: Serve / Fail Batch
// ============================================================================

static void maybe_reset_slab(alr_t *alr) {
    if (alr->pending_count == 0 && alr->batch_count == 0) {
        alr->slab_cursor = 0;
    }
}

/**
 * Serve or fail all reads in the current sync batch.
 * On success (err == LYGUS_OK), sync_target is passed to the callback
 * so handler.c can filter DAG reads to leader_seq <= target.
 */
static void complete_batch(alr_t *alr, lygus_err_t err) {
    uint64_t target = (err == LYGUS_OK) ? alr->sync_target : 0;

    for (uint16_t i = 0; i < alr->batch_count; i++) {
        alr_read_entry_t *r = &alr->batch[i];
        if (r->cancelled) continue;

        alr->serve(r->conn, r->key, r->klen, target, err, alr->serve_ctx);

        if (err == LYGUS_OK) alr->stats.reads_completed++;
        else                 alr->stats.reads_failed++;
    }

    alr->batch_count = 0;
    alr->sync_in_flight = false;
    alr->sync_target = 0;
    maybe_reset_slab(alr);
}

// ============================================================================
// Lifecycle
// ============================================================================

alr_t *alr_create(const alr_config_t *cfg) {
    if (!cfg || !cfg->serve) return NULL;

    alr_t *alr = calloc(1, sizeof(*alr));
    if (!alr) return NULL;

    alr->capacity = cfg->capacity > 0 ? cfg->capacity : ALR_DEFAULT_CAPACITY;
    alr->slab_size = cfg->slab_size > 0 ? cfg->slab_size : ALR_DEFAULT_SLAB_SIZE;
    alr->timeout_ms = cfg->timeout_ms > 0 ? cfg->timeout_ms : ALR_DEFAULT_TIMEOUT_MS;

    alr->pending = calloc(alr->capacity, sizeof(alr_read_entry_t));
    alr->batch   = calloc(alr->capacity, sizeof(alr_read_entry_t));
    alr->slab    = malloc(alr->slab_size);

    if (!alr->pending || !alr->batch || !alr->slab) {
        free(alr->pending);
        free(alr->batch);
        free(alr->slab);
        free(alr);
        return NULL;
    }

    alr->serve     = cfg->serve;
    alr->serve_ctx = cfg->serve_ctx;

    return alr;
}

void alr_destroy(alr_t *alr) {
    if (!alr) return;
    free(alr->pending);
    free(alr->batch);
    free(alr->slab);
    free(alr);
}

// ============================================================================
// Core: Queue a follower read
// ============================================================================

lygus_err_t alr_read(alr_t *alr, const void *key, size_t klen,
                      void *conn, uint64_t now_ms) {
    if (!alr || !key || klen == 0) return LYGUS_ERR_INVALID_ARG;
    if (alr->pending_count >= alr->capacity) return LYGUS_ERR_BATCH_FULL;
    if (alr->slab_cursor + klen > alr->slab_size) return LYGUS_ERR_BATCH_FULL;

    // Copy key into slab
    void *key_ptr = alr->slab + alr->slab_cursor;
    memcpy(key_ptr, key, klen);
    alr->slab_cursor += klen;

    alr_read_entry_t *r = &alr->pending[alr->pending_count++];
    r->conn = conn;
    r->key = key_ptr;
    r->klen = klen;
    r->deadline_ms = now_ms + alr->timeout_ms;
    r->cancelled = false;

    alr->stats.reads_total++;

    // If a sync is already in flight, this read naturally goes into
    // the next batch (pending).  That's correct — batch isolation.
    if (alr->sync_in_flight) {
        alr->stats.batched++;
    }

    return LYGUS_OK;
}

// ============================================================================
// Core: Close batch and mark sync as in-flight
// ============================================================================

bool alr_send_sync(alr_t *alr) {
    if (!alr) return false;
    if (alr->pending_count == 0) return false;
    if (alr->sync_in_flight) return false;
    if (alr->batch_count > 0) return false;  // shouldn't happen

    // Swap pending ↔ batch (O(1) pointer swap).
    // Both arrays have the same capacity, allocated in create.
    alr_read_entry_t *tmp_ptr = alr->batch;
    alr->batch = alr->pending;
    alr->pending = tmp_ptr;

    alr->batch_count = alr->pending_count;
    alr->pending_count = 0;

    alr->sync_in_flight = true;
    alr->sync_target = 0;  // waiting for leader response

    alr->stats.syncs_sent++;
    return true;
}

// ============================================================================
// Core: Leader responded with max_acked_seq
// ============================================================================

void alr_recv_sync(alr_t *alr, uint64_t max_seq) {
    if (!alr) return;
    if (!alr->sync_in_flight) return;
    if (alr->batch_count == 0) return;

    // max_seq == 0: no acked writes exist.  Serve immediately.
    // This handles the TLA+ FollowerReceiveSync maxSeq=0 edge case,
    // where followerSyncTarget=0 would otherwise be ambiguous with
    // "no sync response yet".
    if (max_seq == 0) {
        // sync_target stays 0, which complete_batch reads as "empty"
        complete_batch(alr, LYGUS_OK);
        return;
    }

    alr->sync_target = max_seq;

    // Check if the prefix gate is already satisfied.
    // This can happen if the follower already received all writes
    // via ff-push before the sync response arrived.
    uint32_t target_counter = (uint32_t)(max_seq & 0xFFFFFFFF);
    if (alr->prefix >= target_counter) {
        complete_batch(alr, LYGUS_OK);
    }
    // Otherwise, alr_notify_seq will trigger serve when gaps fill.
}

// ============================================================================
// Core: New DAG node arrived via ff-push
// ============================================================================

void alr_notify_seq(alr_t *alr, uint64_t seq) {
    if (!alr || seq == 0) return;

    // Extract dense counter (lower 32 bits).
    // Counters are dense across terms because sync_complete
    // reconstructs from max existing counter.
    uint32_t counter = (uint32_t)(seq & 0xFFFFFFFF);

    if (counter <= alr->prefix) return;  // already tracked

    if (counter == alr->prefix + 1) {
        // Next expected — advance prefix and drain any buffered
        alr->prefix = counter;
        drain_gaps(alr);
    } else {
        // Out of order — buffer for later
        gap_insert(alr, counter);
    }

    // Check prefix gate if we have an active target
    if (alr->sync_target > 0) {
        uint32_t target_counter = (uint32_t)(alr->sync_target & 0xFFFFFFFF);
        if (alr->prefix >= target_counter) {
            complete_batch(alr, LYGUS_OK);
        }
    }
}

// ============================================================================
// Core: Flush all reads (term change, leadership change, timeout)
// ============================================================================

void alr_flush(alr_t *alr, lygus_err_t err) {
    if (!alr) return;

    // Fail the sync batch
    for (uint16_t i = 0; i < alr->batch_count; i++) {
        alr_read_entry_t *r = &alr->batch[i];
        if (!r->cancelled && r->conn) {
            alr->serve(r->conn, r->key, r->klen, 0, err, alr->serve_ctx);
            alr->stats.reads_failed++;
        }
    }
    alr->batch_count = 0;

    // Fail pending reads
    for (uint16_t i = 0; i < alr->pending_count; i++) {
        alr_read_entry_t *r = &alr->pending[i];
        if (!r->cancelled && r->conn) {
            alr->serve(r->conn, r->key, r->klen, 0, err, alr->serve_ctx);
            alr->stats.reads_failed++;
        }
    }
    alr->pending_count = 0;

    alr->sync_in_flight = false;
    alr->sync_target = 0;
    alr->slab_cursor = 0;

    // Don't reset prefix — counters are dense across terms,
    // so the prefix remains valid.  The sync target is cleared,
    // so the prefix gate won't fire until the next sync.
}

// ============================================================================
// Core: Cancel reads for a disconnected client
// ============================================================================

int alr_cancel_conn(alr_t *alr, void *conn) {
    if (!alr || !conn) return 0;
    int cancelled = 0;

    for (uint16_t i = 0; i < alr->pending_count; i++) {
        alr_read_entry_t *r = &alr->pending[i];
        if (r->conn == conn && !r->cancelled) {
            r->cancelled = true;
            r->conn = NULL;
            cancelled++;
        }
    }

    for (uint16_t i = 0; i < alr->batch_count; i++) {
        alr_read_entry_t *r = &alr->batch[i];
        if (r->conn == conn && !r->cancelled) {
            r->cancelled = true;
            r->conn = NULL;
            cancelled++;
        }
    }

    return cancelled;
}

// ============================================================================
// Core: Timeout sweep
// ============================================================================

int alr_timeout_sweep(alr_t *alr, uint64_t now_ms) {
    if (!alr) return 0;
    int expired = 0;

    for (uint16_t i = 0; i < alr->pending_count; i++) {
        alr_read_entry_t *r = &alr->pending[i];
        if (r->cancelled) continue;
        if (now_ms >= r->deadline_ms) {
            if (r->conn) {
                alr->serve(r->conn, r->key, r->klen,
                           0, LYGUS_ERR_TIMEOUT, alr->serve_ctx);
            }
            r->cancelled = true;
            r->conn = NULL;
            alr->stats.reads_timeout++;
            expired++;
        }
    }

    for (uint16_t i = 0; i < alr->batch_count; i++) {
        alr_read_entry_t *r = &alr->batch[i];
        if (r->cancelled) continue;
        if (now_ms >= r->deadline_ms) {
            if (r->conn) {
                alr->serve(r->conn, r->key, r->klen,
                           0, LYGUS_ERR_TIMEOUT, alr->serve_ctx);
            }
            r->cancelled = true;
            r->conn = NULL;
            alr->stats.reads_timeout++;
            expired++;
        }
    }

    return expired;
}

// ============================================================================
// Accessors
// ============================================================================

bool alr_sync_active(const alr_t *alr) {
    return alr ? alr->sync_in_flight : false;
}

bool alr_has_pending(const alr_t *alr) {
    return alr ? (alr->pending_count > 0) : false;
}

uint64_t alr_get_prefix(const alr_t *alr) {
    return alr ? (uint64_t)alr->prefix : 0;
}

void alr_get_stats(const alr_t *alr, alr_stats_t *out) {
    if (!alr || !out) return;
    *out = alr->stats;
    out->pending_count = alr->pending_count;
    out->batch_count = alr->batch_count;
    out->contiguous_prefix = (uint64_t)alr->prefix;
}