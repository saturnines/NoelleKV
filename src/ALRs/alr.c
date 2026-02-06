/**
 * alr.c - Almost Local Reads (Lazy-ALR for Raft)
 *
 * Implements linearizable reads from any replica using the Lazy-ALR
 * technique from "The LAW Theorem" paper.
 *
 * Sync strategies (in priority order):
 * 1. Piggyback on pending log entry (leader or follower)
 * 2. Leader: propose NOOP
 * 3. Follower: async ReadIndex RPC to leader
 */

#include "alr.h"
#include "raft.h"
#include "public/lygus_errors.h"
#include "state/kv_store.h"

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

// ============================================================================
// Defaults
// ============================================================================

#define ALR_DEFAULT_CAPACITY    4096
#define ALR_DEFAULT_SLAB_SIZE   (16 * 1024 * 1024)  // 16MB
#define ALR_DEFAULT_TIMEOUT_MS  5000                // 5 seconds
#define ALR_MAX_VALUE_SIZE      (64 * 1024)         // 64KB stack buffer

// ============================================================================
// Internal Types
// ============================================================================

typedef enum {
    READ_STATE_READY,           // Has sync_index, waiting for apply
    READ_STATE_AWAITING_INDEX,  // Waiting for ReadIndex response
    READ_STATE_CANCELLED,       // Connection died
} read_state_t;

typedef struct {
    void        *conn;
    void        *key;
    size_t       klen;
    uint64_t     sync_index;
    uint64_t     sync_term;
    uint64_t     deadline_ms;       // Absolute timeout
    uint64_t     read_index_id;     // Which ReadIndex request this is waiting on
    read_state_t state;
} pending_read_t;

struct alr {
    // Ring buffer
    pending_read_t *reads;
    uint16_t head;
    uint16_t count;
    uint16_t capacity;

    // Slab allocator
    uint8_t *slab;
    size_t   slab_size;
    size_t   slab_cursor;
    size_t   slab_high_water;

    // Timing
    uint32_t timeout_ms;

    // Sync state
    uint64_t last_issued_sync;
    uint64_t last_applied;
    uint64_t last_issued_sync_term;
    uint64_t last_issued_sync_time_ms;

    // ReadIndex state
    uint64_t read_index_seq;         // Monotonic ID generator

    // Tracks the currently in-flight sync batch
    uint64_t active_read_index_id;   // 0 if no sync is in progress
    uint64_t active_read_index_term; // Term when the active ReadIndex was issued

    // Dependencies
    raft_t         *raft;
    lygus_kv_t     *kv;
    alr_respond_fn  respond;
    void           *respond_ctx;

    // Stats
    alr_stats_t stats;
};

// ============================================================================
// Ring Helpers
// ============================================================================

static inline uint16_t ring_idx(alr_t *alr, uint16_t offset) {
    return (alr->head + offset) % alr->capacity;
}

static inline pending_read_t *ring_at(alr_t *alr, uint16_t offset) {
    return &alr->reads[ring_idx(alr, offset)];
}

static inline pending_read_t *ring_head(alr_t *alr) {
    return &alr->reads[alr->head];
}

static inline pending_read_t *ring_tail(alr_t *alr) {
    return &alr->reads[ring_idx(alr, alr->count)];
}

// ============================================================================
// Internal: Fail reads waiting on a specific ReadIndex request
// ============================================================================

static void fail_reads_for_read_index(alr_t *alr, uint64_t read_index_id, lygus_err_t err) {
    for (uint16_t i = 0; i < alr->count; i++) {
        pending_read_t *r = ring_at(alr, i);

        if (r->state == READ_STATE_AWAITING_INDEX &&
            (read_index_id == 0 || r->read_index_id == read_index_id)) {

            if (r->conn != NULL) {
                alr->respond(r->conn, r->key, r->klen,
                             NULL, 0, err, alr->respond_ctx);
            }
            r->state = READ_STATE_CANCELLED;
            r->conn = NULL;
            alr->stats.reads_failed++;
        }
    }
}

// ============================================================================
// Internal: Promote reads waiting on ReadIndex to ready state
// ============================================================================

static void promote_reads_for_read_index(alr_t *alr, uint64_t read_index_id,
                                         uint64_t sync_index, uint64_t sync_term) {
    for (uint16_t i = 0; i < alr->count; i++) {
        pending_read_t *r = ring_at(alr, i);

        if (r->state == READ_STATE_AWAITING_INDEX &&
            r->read_index_id == read_index_id) {

            r->sync_index = sync_index;
            r->sync_term = sync_term;
            r->state = READ_STATE_READY;
        }
    }
}

// ============================================================================
// Lifecycle
// ============================================================================

alr_t *alr_create(const alr_config_t *cfg) {
    if (!cfg || !cfg->raft || !cfg->kv || !cfg->respond) {
        return NULL;
    }

    alr_t *alr = calloc(1, sizeof(alr_t));
    if (!alr) {
        return NULL;
    }

    alr->capacity = cfg->capacity > 0 ? cfg->capacity : ALR_DEFAULT_CAPACITY;
    alr->slab_size = cfg->slab_size > 0 ? cfg->slab_size : ALR_DEFAULT_SLAB_SIZE;
    alr->timeout_ms = cfg->timeout_ms > 0 ? cfg->timeout_ms : ALR_DEFAULT_TIMEOUT_MS;

    alr->reads = calloc(alr->capacity, sizeof(pending_read_t));
    if (!alr->reads) {
        free(alr);
        return NULL;
    }

    alr->slab = malloc(alr->slab_size);
    if (!alr->slab) {
        free(alr->reads);
        free(alr);
        return NULL;
    }

    alr->raft = cfg->raft;
    alr->kv = cfg->kv;
    alr->respond = cfg->respond;
    alr->respond_ctx = cfg->respond_ctx;

    return alr;
}

void alr_destroy(alr_t *alr) {
    if (!alr) return;
    free(alr->slab);
    free(alr->reads);
    free(alr);
}

// ============================================================================
// Core: Queue a read
// ============================================================================

lygus_err_t alr_read(alr_t *alr, const void *key, size_t klen, void *conn, uint64_t now_ms) {
    if (!alr || !key || klen == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (alr->count >= alr->capacity) {
        return LYGUS_ERR_BATCH_FULL;
    }

    // If we run out of memory, we must fail and wait for drain.
    if (alr->slab_cursor + klen > alr->slab_size) {
        return LYGUS_ERR_BATCH_FULL;
    }

    bool is_leader = raft_is_leader(alr->raft);
    uint64_t current_term = raft_get_term(alr->raft);

    uint64_t sync_index = 0;
    uint64_t sync_term = 0;
    uint64_t read_index_id = 0;
    read_state_t initial_state = READ_STATE_READY;

    uint64_t pending = raft_get_pending_index(alr->raft);

    if (is_leader && pending > 0 && pending > alr->last_applied) {
        // Piggyback on pending write
        sync_index = pending;
        sync_term = current_term;

        alr->last_issued_sync = pending;
        alr->last_issued_sync_term = current_term;
        alr->stats.piggybacks++;
    }
    else if (is_leader) {
        // FIX: Piggyback on uncommitted NOOP if one exists
        if (alr->last_issued_sync > alr->last_applied &&
            alr->last_issued_sync_term == current_term) {
            // Reuse existing in-flight NOOP
            sync_index = alr->last_issued_sync;
            sync_term = alr->last_issued_sync_term;
            alr->stats.piggybacks++;
        } else {
            // Need new NOOP
            if (raft_propose_noop(alr->raft, &sync_index) != 0) {
                return LYGUS_ERR_SYNC_FAILED;
            }
            sync_term = raft_get_term(alr->raft);

            alr->last_issued_sync = sync_index;
            alr->last_issued_sync_term = sync_term;
            alr->stats.syncs_issued++;
        }
    }
    else {
        // Follower path

        // Case 1: Piggyback on in-flight ReadIndex
        if (alr->active_read_index_id != 0 &&
            alr->active_read_index_term == current_term) {
            read_index_id = alr->active_read_index_id;
            initial_state = READ_STATE_AWAITING_INDEX;
            alr->stats.piggybacks++;
            }
        // Case 2: Must issue new ReadIndex
        else {
            uint64_t req_id = ++alr->read_index_seq;
            int err = raft_request_read_index_async(alr->raft, req_id);
            if (err != 0) {
                return LYGUS_ERR_TRY_LEADER;
            }

            alr->active_read_index_id = req_id;
            alr->active_read_index_term = current_term;

            read_index_id = req_id;
            initial_state = READ_STATE_AWAITING_INDEX;
            alr->stats.read_index_issued++;
        }
    }

    void *key_ptr = alr->slab + alr->slab_cursor;
    memcpy(key_ptr, key, klen);
    alr->slab_cursor += klen;

    if (alr->slab_cursor > alr->slab_high_water) {
        alr->slab_high_water = alr->slab_cursor;
    }

    pending_read_t *r = ring_tail(alr);
    r->conn = conn;
    r->key = key_ptr;
    r->klen = klen;
    r->sync_index = sync_index;
    r->sync_term = sync_term;
    r->deadline_ms = now_ms + alr->timeout_ms;
    r->read_index_id = read_index_id;
    r->state = initial_state;

    alr->count++;
    alr->stats.reads_total++;

    return LYGUS_OK;
}

// ============================================================================
// Core: ReadIndex response callback
// ============================================================================

void alr_on_read_index(alr_t *alr, uint64_t req_id, uint64_t index, int err) {
    if (!alr) return;

    uint64_t current_term = raft_get_term(alr->raft);

    if (req_id == alr->active_read_index_id) {
        //  If the response is from an old term, ignore it.
        if (alr->active_read_index_term < current_term) {
            fail_reads_for_read_index(alr, req_id, LYGUS_ERR_STALE_READ);
            alr->active_read_index_id = 0;
            alr->active_read_index_term = 0;
            return;
        }
        alr->active_read_index_id = 0;
        alr->active_read_index_term = 0;
    }

    if (err != 0) {
        fail_reads_for_read_index(alr, req_id, LYGUS_ERR_TRY_LEADER);
        return;
    }

    // Get the term of the entry at the read index
    uint64_t index_term = raft_log_term_at(alr->raft, index);
    if (index_term == 0) {
        fail_reads_for_read_index(alr, req_id, LYGUS_ERR_STALE_READ);
        return;
    }

    // Promote waiting reads to ready state
    promote_reads_for_read_index(alr, req_id, index, index_term);

    // Update cache if this is newer
    if (index > alr->last_issued_sync) {
        alr->last_issued_sync = index;
        alr->last_issued_sync_term = index_term;
    }

    // Try to complete any now-ready reads
    alr_notify(alr, alr->last_applied);
}

// ============================================================================
// Core: Notification that entries have been applied
// ============================================================================

void alr_notify(alr_t *alr, uint64_t applied_index) {
    if (!alr) return;

    // Safety: If index rolled back, fail everything
    if (applied_index < alr->last_applied) {
        for (uint16_t i = 0; i < alr->count; i++) {
            pending_read_t *r = ring_at(alr, i);
            if (r->state != READ_STATE_CANCELLED && r->conn) {
                alr->respond(r->conn, r->key, r->klen,
                             NULL, 0, LYGUS_ERR_STALE_READ, alr->respond_ctx);
                alr->stats.reads_stale++;
            }
        }
        alr->head = 0;
        alr->count = 0;
        alr->slab_cursor = 0;
        alr->last_applied = applied_index;
        return;
    }

    alr->last_applied = applied_index;

    if (alr->count == 0) return;

    uint8_t val_buf[ALR_MAX_VALUE_SIZE];

    while (alr->count > 0) {
        pending_read_t *r = ring_head(alr);

        if (r->state == READ_STATE_CANCELLED) {
            alr->head = ring_idx(alr, 1);
            alr->count--;
            continue;
        }

        if (r->state == READ_STATE_AWAITING_INDEX) {
            break;
        }

        // prevent reading until applied state catches up to sync_index
        if (r->sync_index > alr->last_applied) {
            break;
        }

        uint64_t term_at_sync = raft_log_term_at(alr->raft, r->sync_index);

        // Check for term mismatch
        if (term_at_sync == 0 || term_at_sync != r->sync_term) {
            if (r->conn != NULL) {
                alr->respond(r->conn, r->key, r->klen,
                             NULL, 0, LYGUS_ERR_STALE_READ, alr->respond_ctx);
            }
            alr->head = ring_idx(alr, 1);
            alr->count--;
            alr->stats.reads_stale++;
            continue;
        }

        ssize_t vlen = lygus_kv_get(alr->kv, r->key, r->klen,
                                     val_buf, sizeof(val_buf));

        if (r->conn != NULL) {
            if (vlen >= 0) {
                alr->respond(r->conn, r->key, r->klen,
                             val_buf, (size_t)vlen, LYGUS_OK, alr->respond_ctx);
            } else {
                alr->respond(r->conn, r->key, r->klen,
                             NULL, 0, (lygus_err_t)vlen, alr->respond_ctx);
            }
        }

        alr->head = ring_idx(alr, 1);
        alr->count--;
        alr->stats.reads_completed++;
    }

    // This is the only place we trust the cursor to reset, ensuring no overwrites.
    if (alr->count == 0) {
        alr->slab_cursor = 0;
    }
}

// ============================================================================
// Core: Term change notification
// ============================================================================

void alr_on_term_change(alr_t *alr, uint64_t new_term) {
    if (!alr) return;
    (void)new_term;  // We don't need the value, just the event

    // 1. Respond to everyone currently waiting with a retryable error
    for (uint16_t i = 0; i < alr->count; i++) {
        pending_read_t *r = ring_at(alr, i);

        if (r->state != READ_STATE_CANCELLED && r->conn != NULL) {
            alr->respond(r->conn, r->key, r->klen,
                         NULL, 0, LYGUS_ERR_STALE_READ, alr->respond_ctx);
            alr->stats.reads_stale++;
        }
    }

    // HARD RESET
    alr->head = 0;
    alr->count = 0;
    alr->slab_cursor = 0;
    alr->last_issued_sync = 0;
    alr->last_issued_sync_term = 0;
    alr->active_read_index_id = 0;
    alr->active_read_index_term = 0;
}

// ============================================================================
// Core: Timeout sweep
// ============================================================================

int alr_timeout_sweep(alr_t *alr, uint64_t now_ms) {
    if (!alr) return 0;

    int expired = 0;

    for (uint16_t i = 0; i < alr->count; i++) {
        pending_read_t *r = ring_at(alr, i);

        if (r->state == READ_STATE_CANCELLED) continue;
        if (r->deadline_ms == 0) continue;

        if (now_ms >= r->deadline_ms) {
            if (r->conn != NULL) {
                alr->respond(r->conn, r->key, r->klen,
                             NULL, 0, LYGUS_ERR_TIMEOUT, alr->respond_ctx);
            }
            r->state = READ_STATE_CANCELLED;
            r->conn = NULL;
            alr->stats.reads_timeout++;
            expired++;
        }
    }

    return expired;
}

// ============================================================================
// Core: Cancel reads for a connection
// ============================================================================

int alr_cancel_conn(alr_t *alr, void *conn) {
    if (!alr || !conn) return 0;

    int cancelled = 0;

    for (uint16_t i = 0; i < alr->count; i++) {
        pending_read_t *r = ring_at(alr, i);

        if (r->conn == conn && r->state != READ_STATE_CANCELLED) {
            alr->respond(conn, r->key, r->klen,
                         NULL, 0, LYGUS_ERR_NET, alr->respond_ctx);
            r->state = READ_STATE_CANCELLED;
            r->conn = NULL;
            cancelled++;
        }
    }

    return cancelled;
}

// ============================================================================
// Stats
// ============================================================================

void alr_get_stats(const alr_t *alr, alr_stats_t *out) {
    if (!alr || !out) return;

    *out = alr->stats;
    out->pending_count = alr->count;
    out->slab_used = alr->slab_cursor;
    out->slab_high_water = alr->slab_high_water;
    out->pending_read_index = alr->active_read_index_id != 0 ? 1 : 0;
}