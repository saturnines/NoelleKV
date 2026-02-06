/**
 * raft_replication.c - AppendEntries RPC logic
 */

#include "raft_internal.h"
#include "raft_errors.h"
#include <stdio.h>
#include <string.h>

// Max entries per message (stack buffer size)
#define REPLICATION_BATCH_MAX 64

// ============================================================================
// Send Heartbeats / AppendEntries to All Peers
// ============================================================================

void raft_send_heartbeats(raft_t *r) {
    if (!r || r->state != RAFT_STATE_LEADER) return;

    // Increment the heartbeat sequence number for this round
    // This is used by ReadIndex to reject stale ACKs from partitioned nodes
    r->heartbeat_seq++;

    // Use config limit, fall back to compile-time max
    size_t max_entries = r->config.max_entries_per_msg;
    if (max_entries == 0 || max_entries > REPLICATION_BATCH_MAX) {
        max_entries = REPLICATION_BATCH_MAX;
    }

    for (int i = 0; i < r->num_nodes; i++) {
        if (i == r->my_id) continue;

        if (raft_peer_needs_snapshot(r, i)) {
            // Peer is too far behind - send InstallSnapshot instead
            if (!r->snapshot_send[i].in_progress) {
                raft_send_installsnapshot(r, i);
            }
            continue;
        }

        uint64_t next_idx = r->peers[i].next_index;
        uint64_t prev_idx = next_idx - 1;
        uint64_t prev_term = 0;

        // Get prev_log_term
        if (prev_idx > 0) {
            raft_entry_t *prev_entry = raft_log_get(&r->log, prev_idx);
            if (prev_entry) {
                prev_term = prev_entry->term;
            } else if (prev_idx == r->snapshot_last_index) {
                // Entry was compacted, use snapshot term
                prev_term = r->snapshot_last_term;
            }
        }

        raft_appendentries_req_t req = {
            .term = r->current_term,
            .leader_id = r->my_id,
            .prev_log_index = prev_idx,
            .prev_log_term = prev_term,
            .leader_commit = r->commit_index,
            .seq = r->heartbeat_seq,
        };

        // Copy entries to stack buffer (deep copy)
        raft_entry_t batch[REPLICATION_BATCH_MAX];
        size_t n_entries = raft_log_copy_range(&r->log, next_idx,
                                                batch, max_entries);

        if (r->callbacks.send_appendentries) {
            r->callbacks.send_appendentries(r->callback_ctx, i, &req,
                                            n_entries > 0 ? batch : NULL,
                                            n_entries);
        }

        // Free copied entry data
        for (size_t j = 0; j < n_entries; j++) {
            raft_entry_free(&batch[j]);
        }
    }
}

// ============================================================================
// Handle Incoming AppendEntries
// ============================================================================

int raft_recv_appendentries(raft_t *r,
                            const raft_appendentries_req_t *req,
                            const raft_entry_t *entries,
                            size_t n_entries,
                            raft_appendentries_resp_t *resp) {
    if (!r || !req || !resp) {
        return RAFT_ERR_INVALID_ARG;
    }

    resp->term = r->current_term;
    resp->success = 0;
    resp->match_index = 0;
    resp->seq = req->seq; // DEBUG, added so followers can read from leader.
    resp->conflict_term = 0;
    resp->conflict_index = 0;

    // Rule 1: Reply false if term < currentTerm
    if (req->term < r->current_term) {
        printf("[Node %d] Rejecting AE from %d: stale term %lu < %lu\n",
               r->my_id, req->leader_id, req->term, r->current_term);
        return RAFT_OK;
    }

    // If RPC request contains term >= currentTerm, recognize leader
    if (req->term >= r->current_term) {
        if (req->term > r->current_term || r->state != RAFT_STATE_FOLLOWER) {
            raft_become_follower(r, req->term);
        }
        r->leader_id = req->leader_id;
        r->last_heartbeat_ms = raft_get_time_ms();
    }

    // Rule 2: Reply false if log doesn't contain an entry at prevLogIndex
    // whose term matches prevLogTerm
    if (req->prev_log_index > 0) {
        // Check if prev_log_index is covered by snapshot
        if (req->prev_log_index == r->snapshot_last_index) {
            // Use snapshot term for comparison
            if (r->snapshot_last_term != req->prev_log_term) {
                printf("[Node %d] Rejecting AE: snapshot term mismatch at %lu\n",
                       r->my_id, req->prev_log_index);
                resp->match_index = 0;
                return RAFT_OK;
            }
        } else if (req->prev_log_index < r->snapshot_last_index) {
            // prev_log_index is before snapshot - we can't verify
            // This shouldn't happen if leader is behaving correctly
            printf("[Node %d] Rejecting AE: prev_log_index %lu before snapshot %lu\n",
                   r->my_id, req->prev_log_index, r->snapshot_last_index);
            resp->match_index = r->snapshot_last_index;
            return RAFT_OK;
        } else {
            raft_entry_t *prev_entry = raft_log_get(&r->log, req->prev_log_index);

            if (!prev_entry) {
                printf("[Node %d] Rejecting AE: missing entry at prev_log_index %lu\n",
                       r->my_id, req->prev_log_index);
                resp->match_index = raft_log_last_index(&r->log);
                return RAFT_OK;
            }

            if (prev_entry->term != req->prev_log_term) {
                printf("[Node %d] Rejecting AE: term mismatch at %lu (got %lu, want %lu)\n",
                       r->my_id, req->prev_log_index, prev_entry->term, req->prev_log_term);

                // Set conflict info for fast rollback
                resp->conflict_term = prev_entry->term;
                resp->conflict_index = raft_log_find_first_of_term(&r->log, prev_entry->term);

                raft_log_truncate_after(&r->log, req->prev_log_index - 1);
                resp->match_index = req->prev_log_index - 1;
                return RAFT_OK;
            }
        }
    }

    // Rule 3 & 4: Append any new entries not already in the log
    uint64_t insert_index = req->prev_log_index + 1;

    for (size_t i = 0; i < n_entries; i++) {
        uint64_t entry_index = insert_index + i;
        raft_entry_t *existing = raft_log_get(&r->log, entry_index);

        if (existing) {
            // Rule 3: If an existing entry conflicts with a new one, delete the existing entry and all that follow it
            if (existing->term != entries[i].term) {
                raft_log_truncate_after(&r->log, entry_index - 1);
                existing = NULL;
            }
        }

        if (!existing) {
            // Append to in-memory log (preserves entry type)
            raft_log_append_entry(&r->log, &entries[i]);

            // Persist via callback
            if (r->callbacks.log_append) {
                r->callbacks.log_append(r->callback_ctx, entry_index,
                                       entries[i].term,
                                       entries[i].data, entries[i].len);
            }
        }
    }

    if (n_entries > 0 && r->callbacks.log_fsync) {
        r->callbacks.log_fsync(r->callback_ctx);
    }

    // Rule 5: If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
    if (req->leader_commit > r->commit_index) {
        uint64_t last_new_index = req->prev_log_index + n_entries;
        uint64_t last_log_idx = raft_log_last_index(&r->log);

        r->commit_index = req->leader_commit;
        if (last_new_index < r->commit_index) {
            r->commit_index = last_new_index;
        }
        if (last_log_idx < r->commit_index) {
            r->commit_index = last_log_idx;
        }
    }

    resp->success = 1;
    resp->match_index = raft_log_last_index(&r->log);

    return RAFT_OK;
}

// ============================================================================
// Handle AppendEntries Response
// ============================================================================

int raft_recv_appendentries_response(raft_t *r,
                                     int peer_id,
                                     const raft_appendentries_resp_t *resp) {
    if (!r || !resp) {
        return RAFT_ERR_INVALID_ARG;
    }

    // Ignore if we're not leader
    if (r->state != RAFT_STATE_LEADER) {
        return RAFT_OK;
    }

    // If response contains higher term, step down
    if (resp->term > r->current_term) {
        raft_become_follower(r, resp->term);
        return RAFT_OK;
    }

    // Ignore stale respones
    if (resp->term < r->current_term) {
        return RAFT_OK;
    }

    if (resp->success) {
        // Update next_index and match_index for follower
        if (resp->match_index > r->peers[peer_id].match_index) {
            r->peers[peer_id].match_index = resp->match_index;
            r->peers[peer_id].next_index = resp->match_index + 1;

            // Reset snapshot transfer state since peer is caught up
            r->snapshot_send[peer_id].in_progress = 0;
            r->snapshot_send[peer_id].offset = 0;
        }

        // Record ack for pending ReadIndex requests
        // Pass the sequence number to prevent stale reads during partitions
        raft_readindex_record_ack(r, peer_id, resp->seq);

        // Try to advance commit_index
        // Find the highest N such that a majority of matchIndex[i] >= N
        // and log[N].term == currentTerm
        for (uint64_t n = r->commit_index + 1; n <= raft_log_last_index(&r->log); n++) {
            raft_entry_t *entry = raft_log_get(&r->log, n);
            if (!entry || entry->term != r->current_term) {
                continue;  // Only commit entries from current term
            }

            int replication_count = 1;  // Count self
            for (int i = 0; i < r->num_nodes; i++) {
                if (i != r->my_id && r->peers[i].match_index >= n) {
                    replication_count++;
                }
            }

            if (replication_count >= raft_quorum_size(r->num_nodes)) {
                r->commit_index = n;
            }
        }
    } else {
        // AppendEntries failed - use conflict info for fast rollback (§5.3)
        if (resp->conflict_term > 0) {
            // Find our last entry with the conflicting term
            uint64_t leader_last = raft_log_find_last_of_term(&r->log, resp->conflict_term);
            if (leader_last > 0) {
                // We have this term - skip to end of it
                r->peers[peer_id].next_index = leader_last + 1;
            } else {
                // We don't have this term - jump to follower's first index of it
                r->peers[peer_id].next_index = resp->conflict_index;
            }
        } else if (resp->match_index > 0 && resp->match_index < r->peers[peer_id].next_index) {
            r->peers[peer_id].next_index = resp->match_index + 1;
        } else if (r->peers[peer_id].next_index > 1) {
            r->peers[peer_id].next_index--;
        }
    }

    return RAFT_OK;
}

// ============================================================================
// Propose (Client Write)
// ============================================================================

int raft_propose(raft_t *r, const void *data, size_t len) {
    if (!r) {
        return RAFT_ERR_INVALID_ARG;
    }

    if (r->shutdown) {
        return RAFT_ERR_SHUTDOWN;
    }

    if (r->state != RAFT_STATE_LEADER) {
        return RAFT_ERR_NOT_LEADER;
    }

    // Append to in mem log
    int ret = raft_log_append(&r->log, r->current_term, data, len);
    if (ret != RAFT_OK) {
        return ret;
    }

    uint64_t index = raft_log_last_index(&r->log);

    // Persist via callback
    if (r->callbacks.log_append) {
        ret = r->callbacks.log_append(r->callback_ctx, index,
                                      r->current_term, data, len);
        if (ret != 0) {
            // Rollback in-memory on persist failure
            raft_log_truncate_after(&r->log, index - 1);
            return RAFT_ERR_INVALID_ARG;
        }
    }

    // Fsync helper
    if (r->callbacks.log_fsync) {
        r->callbacks.log_fsync(r->callback_ctx);
    }

    // Update own match_index
    r->peers[r->my_id].match_index = index;
    r->peers[r->my_id].next_index = index + 1;


    // single node clusters, just commit since we are quorum
    if (r->num_nodes == 1) {
        r->commit_index = index;
    }

    // Send AppendEntries immediately
    raft_send_heartbeats(r);

    return RAFT_OK;
}

int raft_propose_batch(raft_t *r,
                       const void **entries,
                       const size_t *lengths,
                       size_t count) {
    if (!r || !entries || !lengths || count == 0) {
        return RAFT_ERR_INVALID_ARG;
    }

    if (r->shutdown) {
        return RAFT_ERR_SHUTDOWN;
    }

    if (r->state != RAFT_STATE_LEADER) {
        return RAFT_ERR_NOT_LEADER;
    }

    // Append all entries
    for (size_t i = 0; i < count; i++) {
        int ret = raft_log_append(&r->log, r->current_term, entries[i], lengths[i]);
        if (ret != RAFT_OK) {
            return ret;
        }
    }

    uint64_t last_index = raft_log_last_index(&r->log);
    uint64_t first_index = last_index - count + 1;

    // Persist all entries
    if (r->callbacks.log_append_batch) {
        raft_entry_t *batch = raft_log_get(&r->log, first_index);
        int ret = r->callbacks.log_append_batch(r->callback_ctx, batch, count);
        if (ret != 0) {
            raft_log_truncate_after(&r->log, first_index - 1);
            return RAFT_ERR_INVALID_ARG;
        }
    } else if (r->callbacks.log_append) {
        for (size_t i = 0; i < count; i++) {
            raft_entry_t *e = raft_log_get(&r->log, first_index + i);
            int ret = r->callbacks.log_append(r->callback_ctx,
                                              e->index, e->term, e->data, e->len);
            if (ret != 0) {
                raft_log_truncate_after(&r->log, first_index - 1);
                return RAFT_ERR_INVALID_ARG;
            }
        }
    }

    // Fsync once for entire batch
    if (r->callbacks.log_fsync) {
        r->callbacks.log_fsync(r->callback_ctx);
    }

    // Update own match_index
    r->peers[r->my_id].match_index = last_index;
    r->peers[r->my_id].next_index = last_index + 1;

    // Replicate
    raft_send_heartbeats(r);

    return RAFT_OK;
}

// ============================================================================
// NOOP for Lazy ALR Read Synchronization
// ============================================================================

int raft_propose_noop(raft_t *r, uint64_t *sync_index) {
    if (!r || !sync_index) {
        return RAFT_ERR_INVALID_ARG;
    }

    if (r->shutdown) {
        return RAFT_ERR_SHUTDOWN;
    }

    if (r->state != RAFT_STATE_LEADER) {
        return RAFT_ERR_NOT_LEADER;
    }

    // Append NOOP to in memory log
    int ret = raft_log_append_noop(&r->log, r->current_term);
    if (ret != RAFT_OK) {
        return ret;
    }

    uint64_t index = raft_log_last_index(&r->log);

    // Persist via callback (NOOP has no data)
    if (r->callbacks.log_append) {
        ret = r->callbacks.log_append(r->callback_ctx, index,
                                      r->current_term, NULL, 0);
        if (ret != 0) {
            // Rollback in-memory on persist failure
            raft_log_truncate_after(&r->log, index - 1);
            return RAFT_ERR_INVALID_ARG;
        }
    }

    // Fsync
    if (r->callbacks.log_fsync) {
        r->callbacks.log_fsync(r->callback_ctx);
    }

    // Update own match_index
    r->peers[r->my_id].match_index = index;
    r->peers[r->my_id].next_index = index + 1;

    // Single node cluster
    if (r->num_nodes == 1) {
        r->commit_index = index;
    }

    // Send AppendEntries to replicate NOOP
    raft_send_heartbeats(r);

    *sync_index = index;

    return RAFT_OK;
}