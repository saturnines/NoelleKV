/**
 * raft_snapshot.c - Snapshot and log compaction
 *
 * Handles:
 * - Triggering snapshots when log gets large
 * - Async snapshot support (fork-based)
 * - Truncating log after successful snapshot
 * - InstallSnapshot RPC for slow followers
 * - Recovery from snapshot on startup
 */

#include "raft_internal.h"
#include "raft_errors.h"
#include <stdio.h>
#include <string.h>

// ============================================================================
// Async Snapshot Support
// ============================================================================

/**
 * Poll for async snapshot completion
 *
 * Called from raft_tick() when snapshot_in_progress is set.
 * If snapshot_poll callback exists and returns done, finishes the snapshot.
 */
void raft_snapshot_poll(raft_t *r) {
    if (!r || !r->snapshot_in_progress) return;
    if (!r->callbacks.snapshot_poll) return;

    int done = r->callbacks.snapshot_poll(r->callback_ctx);
    if (done) {
        raft_snapshot_finish(r);
    }
}

/**
 * Complete snapshot after async creation
 *
 * Updates snapshot metadata and truncates log.
 * Called either immediately (sync) or after poll returns done (async).
 */
void raft_snapshot_finish(raft_t *r) {
    if (!r || !r->snapshot_in_progress) return;

    // Update snapshot metadata from pending
    r->snapshot_last_index = r->snapshot_pending_index;
    r->snapshot_last_term = r->snapshot_pending_term;

    // Truncate log - remove entries covered by snapshot
    raft_log_truncate_before(&r->log, r->snapshot_last_index);

    r->snapshot_in_progress = 0;
    r->snapshot_pending_index = 0;
    r->snapshot_pending_term = 0;

    printf("[Node %d] Snapshot complete at index %lu, term %lu\n",
           r->my_id, r->snapshot_last_index, r->snapshot_last_term);
}

// ============================================================================
// Snapshot Creation (Leader or Follower)
// ============================================================================

/**
 * Check if we should create a snapshot
 *
 * Called periodically from raft_tick(). Triggers snapshot if:
 * - Log has grown beyond threshold
 * - We have applied entries to snapshot
 * - No snapshot is currently in progress
 */
int raft_maybe_snapshot(raft_t *r) {
    if (!r) return RAFT_ERR_INVALID_ARG;

    // Need the callback
    if (!r->callbacks.snapshot_create) {
        return RAFT_OK;  // Snapshots not supported, that's fine
    }

    // Don't snapshot if one is in progress
    if (r->snapshot_in_progress) {
        return RAFT_OK;
    }

    // Check if log is large enough to bother
    uint64_t log_size = raft_log_last_index(&r->log) - r->snapshot_last_index;
    if (log_size < r->config.snapshot_threshold) {
        return RAFT_OK;
    }

    // Only snapshot applied entries, must be committed + applied)
    if (r->last_applied <= r->snapshot_last_index) {
        return RAFT_OK;  // Nothing new to snapshot
    }

    return raft_snapshot_create(r);
}

/**
 * Trigger a snapshot at current applied index
 *
 * Can be called manually or from raft_maybe_snapshot().
 * The application callback does the actual serialization.
 *
 * Supports both sync and async snapshots:
 * - If snapshot_poll callback is NULL: sync, finish immediately
 * - If snapshot_poll callback exists: async, poll in raft_tick()
 */
int raft_snapshot_create(raft_t *r) {
    if (!r) return RAFT_ERR_INVALID_ARG;

    if (!r->callbacks.snapshot_create) {
        return RAFT_ERR_INVALID_ARG;
    }

    if (r->snapshot_in_progress) {
        return RAFT_ERR_SNAPSHOT_IN_PROGRESS;
    }

    // Snapshot up to last_applied (guaranteed committed + applied)
    uint64_t snap_index = r->last_applied;

    // Get term of that entry
    raft_entry_t *entry = raft_log_get(&r->log, snap_index);
    uint64_t snap_term;

    if (entry) {
        snap_term = entry->term;
    } else if (snap_index == r->snapshot_last_index) {
        snap_term = r->snapshot_last_term;
    } else {
        // Entry not in log and not at snapshot boundary, shouldn't happen
        printf("[Node %d] Cannot snapshot: entry %lu not found\n",
               r->my_id, snap_index);
        return RAFT_ERR_INVALID_ARG;
    }

    printf("[Node %d] Starting snapshot at index %lu, term %lu\n",
           r->my_id, snap_index, snap_term);

    // Mark in progress and save pending metadata
    r->snapshot_in_progress = 1;
    r->snapshot_pending_index = snap_index;
    r->snapshot_pending_term = snap_term;

    // Call application to start snapshot
    int ret = r->callbacks.snapshot_create(r->callback_ctx, snap_index, snap_term);

    if (ret != RAFT_OK) {
        r->snapshot_in_progress = 0;
        r->snapshot_pending_index = 0;
        r->snapshot_pending_term = 0;
        printf("[Node %d] Snapshot creation failed: %d\n", r->my_id, ret);
        return RAFT_ERR_SNAPSHOT_FAILED;
    }

    // If no poll callback, assume synchronous - finish immediately
    if (!r->callbacks.snapshot_poll) {
        raft_snapshot_finish(r);
    }
    // Otherwise, async - raft_tick() will poll for completion

    return RAFT_OK;
}

// ============================================================================
// Snapshot Recovery used on start up
// ============================================================================

/**
 * Load snapshot on startup
 *
 * Called during raft_create() if a snapshot exists.
 * Restores state machine and sets log base index.
 */
int raft_snapshot_restore(raft_t *r) {
    if (!r) return RAFT_ERR_INVALID_ARG;

    if (!r->callbacks.snapshot_load) {
        return RAFT_OK;  // No snapshot support
    }

    uint64_t snap_index = 0;
    uint64_t snap_term = 0;

    // Ask application to load snapshot and tell us the metadata
    int ret = r->callbacks.snapshot_load(r->callback_ctx, &snap_index, &snap_term);

    if (ret == RAFT_ERR_SNAPSHOT_NOT_FOUND) {
        return RAFT_OK;  // No snapshot, start fresh
    }

    if (ret != 0) {
        return ret;
    }

    printf("[Node %d] Restored snapshot at index %lu, term %lu\n",
           r->my_id, snap_index, snap_term);

    // Set snapshot state
    r->snapshot_last_index = snap_index;
    r->snapshot_last_term = snap_term;

    // Update indexes
    r->commit_index = snap_index;
    r->last_applied = snap_index;

    // Set log base so new entries start after snapshot
    r->log.base_index = snap_index;

    return RAFT_OK;
}

// ============================================================================
// InstallSnapshot RPC - Sending (Leader)
// ============================================================================

/**
 * Send snapshot to a slow follower
 *
 * Called from raft_send_heartbeats() when follower's next_index
 * is before our snapshot boundary.
 */
int raft_send_installsnapshot(raft_t *r, int peer_id) {
    if (!r || peer_id < 0 || peer_id >= r->num_nodes) {
        return RAFT_ERR_INVALID_ARG;
    }

    if (r->state != RAFT_STATE_LEADER) {
        return RAFT_ERR_NOT_LEADER;
    }

    if (!r->callbacks.send_installsnapshot || !r->callbacks.snapshot_read) {
        return RAFT_ERR_INVALID_ARG;
    }

    raft_snapshot_progress_t *prog = &r->snapshot_send[peer_id];

    // Read chunk from application
    uint8_t chunk[RAFT_SNAPSHOT_CHUNK_SIZE];
    size_t chunk_len = 0;
    int done = 0;

    int ret = r->callbacks.snapshot_read(
        r->callback_ctx,
        prog->offset,
        chunk,
        sizeof(chunk),
        &chunk_len,
        &done
    );

    if (ret != 0) {
        return ret;
    }

    // Build request
    raft_installsnapshot_req_t req = {
        .term = r->current_term,
        .leader_id = r->my_id,
        .last_index = r->snapshot_last_index,
        .last_term = r->snapshot_last_term,
        .offset = prog->offset,
        .data = chunk,
        .len = chunk_len,
        .done = done,
    };

    // Send it
    ret = r->callbacks.send_installsnapshot(r->callback_ctx, peer_id, &req);
    if (ret != 0) {
        return ret;
    }

    prog->in_progress = 1;
    prog->last_sent_ms = raft_get_time_ms();

    return RAFT_OK;
}

// ============================================================================
// InstallSnapshot RPC - Receiving (Follower)
// ============================================================================

/**
 * Handle InstallSnapshot from leader
 *
 * Receives snapshot chunks, writes them via callback,
 * and restores state machine when complete.
 */
int raft_recv_installsnapshot(raft_t *r,
                               const raft_installsnapshot_req_t *req,
                               raft_installsnapshot_resp_t *resp) {
    if (!r || !req || !resp) {
        return RAFT_ERR_INVALID_ARG;
    }

    resp->term = r->current_term;
    resp->success = 0;
    resp->last_offset = req->offset;

    // Reply false if term < currentTerm
    if (req->term < r->current_term) {
        printf("[Node %d] Rejecting InstallSnapshot: stale term %lu < %lu\n",
               r->my_id, req->term, r->current_term);
        return RAFT_OK;
    }

    // Update term and become follower if needed
    if (req->term > r->current_term || r->state != RAFT_STATE_FOLLOWER) {
        raft_become_follower(r, req->term);
    }

    r->leader_id = req->leader_id;
    r->last_heartbeat_ms = raft_get_time_ms();

    // If this is an old snapshot, ignore
    if (req->last_index <= r->snapshot_last_index) {
        printf("[Node %d] Ignoring InstallSnapshot: already have index %lu\n",
               r->my_id, r->snapshot_last_index);
        resp->success = 1;
        return RAFT_OK;
    }

    // Write chunk via callback
    if (r->callbacks.snapshot_write) {
        int ret = r->callbacks.snapshot_write(
            r->callback_ctx,
            req->offset,
            req->data,
            req->len,
            req->done
        );

        if (ret != 0) {
            printf("[Node %d] Failed to write snapshot chunk: %d\n", r->my_id, ret);
            return RAFT_OK;  // resp->success stays 0
        }
    }

    resp->success = 1;
    resp->last_offset = req->offset + req->len;
    resp->done = req->done;

    // If this is the last chunk, apply the snapshot
    if (req->done) {
        printf("[Node %d] Snapshot complete, restoring state\n", r->my_id);

        // Load snapshot into state machine
        if (r->callbacks.snapshot_restore) {
            int ret = r->callbacks.snapshot_restore(
                r->callback_ctx,
                req->last_index,
                req->last_term
            );

            if (ret != 0) {
                printf("[Node %d] Failed to restore snapshot: %d\n", r->my_id, ret);
                resp->success = 0;
                return RAFT_OK;
            }
        }

        // Update Raft state
        r->snapshot_last_index = req->last_index;
        r->snapshot_last_term = req->last_term;

        // Discard entire log (snapshot supersedes it)
        raft_log_clear(&r->log);
        r->log.base_index = req->last_index;

        // Update indexes
        if (req->last_index > r->commit_index) {
            r->commit_index = req->last_index;
        }
        if (req->last_index > r->last_applied) {
            r->last_applied = req->last_index;
        }

        printf("[Node %d] Snapshot applied, index=%lu term=%lu\n",
               r->my_id, req->last_index, req->last_term);
    }

    return RAFT_OK;
}

// ============================================================================
// InstallSnapshot Response Handler (Leader)
// ============================================================================

/**
 * Handle InstallSnapshot response from follower
 */
int raft_recv_installsnapshot_response(raft_t *r,
                                        int peer_id,
                                        const raft_installsnapshot_resp_t *resp) {
    if (!r || !resp || peer_id < 0 || peer_id >= r->num_nodes) {
        return RAFT_ERR_INVALID_ARG;
    }

    // Ignore if not leader
    if (r->state != RAFT_STATE_LEADER) {
        return RAFT_OK;
    }

    // Step down if response has higher term
    if (resp->term > r->current_term) {
        raft_become_follower(r, resp->term);
        return RAFT_OK;
    }

    raft_snapshot_progress_t *prog = &r->snapshot_send[peer_id];
    prog->in_progress = 0;

    if (resp->success) {
        prog->offset = resp->last_offset;

        if (resp->done) {
            r->peers[peer_id].next_index = r->snapshot_last_index + 1;
            r->peers[peer_id].match_index = r->snapshot_last_index;
            prog->in_progress = 0;
            prog->offset = 0;
        }
    } else {
        prog->offset = 0;
    }

    return RAFT_OK;
}

// ============================================================================
// Query Functions
// ============================================================================

/**
 * Get current snapshot metadata
 */
void raft_get_snapshot_info(const raft_t *r,
                            uint64_t *last_index,
                            uint64_t *last_term) {
    if (!r) {
        if (last_index) *last_index = 0;
        if (last_term) *last_term = 0;
        return;
    }

    if (last_index) *last_index = r->snapshot_last_index;
    if (last_term) *last_term = r->snapshot_last_term;
}

/**
 * Check if peer needs snapshot instead of AppendEntries
 */
int raft_peer_needs_snapshot(const raft_t *r, int peer_id) {
    if (!r || peer_id < 0 || peer_id >= r->num_nodes) {
        return 0;
    }

    uint64_t next_idx = r->peers[peer_id].next_index;
    uint64_t first_in_log = r->log.base_index + 1;



    if (next_idx < first_in_log) {
        return 1;
    }

    return 0;
}