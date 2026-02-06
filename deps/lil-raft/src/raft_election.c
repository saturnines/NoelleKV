/**
 * raft_election.c - RequestVote RPC logic (including pre-vote)
 */

#include "raft_internal.h"
#include "raft_errors.h"
#include <stdio.h>
#include <string.h>

// ============================================================================
// Pre-Vote
// ============================================================================

/**
 * Start pre-vote phase before becoming a candidate
 */
void raft_start_prevote(raft_t *r) {
    if (!r) return;

    // Don't start prevote if already leader
    if (r->state == RAFT_STATE_LEADER) return;

    r->prevote_in_progress = 1;
    r->prevotes_received = 1;  // Count self
    r->last_heartbeat_ms = raft_get_time_ms();
    r->election_timeout_ms = raft_random_election_timeout(r);

    // Reset pre-vote tracking, count self
    if (r->prevotes_for_me) {
        memset(r->prevotes_for_me, 0, r->num_nodes * sizeof(int));
        r->prevotes_for_me[r->my_id] = 1;
    }

    // Get last log info (snapshot-aware)
    uint64_t last_index = raft_log_last_index(&r->log);
    uint64_t last_term = raft_log_last_term(&r->log);
    if (last_term == 0 && r->snapshot_last_term > 0) {
        last_term = r->snapshot_last_term;
    }

    // Send PreVote with term+1
    raft_requestvote_req_t req = {
        .term = r->current_term + 1,
        .candidate_id = r->my_id,
        .last_log_index = last_index,
        .last_log_term = last_term,
        .is_prevote = 1,
    };

    for (int i = 0; i < r->num_nodes; i++) {
        if (i == r->my_id) continue;

        if (r->callbacks.send_requestvote) {
            r->callbacks.send_requestvote(r->callback_ctx, i, &req);
        }
    }

    // Check if we already have majority for a single node cluster
    if (r->prevotes_received >= raft_quorum_size(r->num_nodes)) {
        r->prevote_in_progress = 0;
        raft_become_candidate(r);
    }
}

// ============================================================================
// Send RequestVote to All Peers
// ============================================================================

void raft_send_requestvote_all(raft_t *r) {
    if (!r || r->state != RAFT_STATE_CANDIDATE) return;

    // Get last log info
    uint64_t last_index = raft_log_last_index(&r->log);
    uint64_t last_term = raft_log_last_term(&r->log);
    if (last_term == 0 && r->snapshot_last_term > 0) {
        last_term = r->snapshot_last_term;
    }

    raft_requestvote_req_t req = {
        .term = r->current_term,
        .candidate_id = r->my_id,
        .last_log_index = last_index,
        .last_log_term = last_term,
        .is_prevote = 0,
    };

    for (int i = 0; i < r->num_nodes; i++) {
        if (i == r->my_id) continue;

        if (r->callbacks.send_requestvote) {
            r->callbacks.send_requestvote(r->callback_ctx, i, &req);
        }
    }
}

// ============================================================================
// Log Up-to-date Check (shared by real vote and pre-vote)
// ============================================================================

/**
 * Check if candidate's log is at least as up-to-date as ours
 * Section 5.4.1: If logs end with different terms, later term wins.
 *                If same term, longer log wins.
 * if our log is empty but we have a snapshot, we use the snapshot's term for comparison.
 */
static int log_is_up_to_date(raft_t *r,
                             uint64_t candidate_last_term,
                             uint64_t candidate_last_index) {
    uint64_t my_last_index = raft_log_last_index(&r->log);
    uint64_t my_last_term = raft_log_last_term(&r->log);

    // If log is empty, but we have a snapshot, use snapshot term
    if (my_last_term == 0 && r->snapshot_last_term > 0) {
        my_last_term = r->snapshot_last_term;
    }

    return (candidate_last_term > my_last_term) ||
           (candidate_last_term == my_last_term &&
            candidate_last_index >= my_last_index);
}

// ============================================================================
// Handle Incoming RequestVote (real or pre-vote)
// ============================================================================

int raft_recv_requestvote(raft_t *r,
                          const raft_requestvote_req_t *req,
                          raft_requestvote_resp_t *resp) {
    if (!r || !req || !resp) {
        return RAFT_ERR_INVALID_ARG;
    }

    resp->term = r->current_term;
    resp->vote_granted = 0;
    resp->is_prevote = req->is_prevote;

    // ========================================================================
    // Pre-vote handling
    // ========================================================================
    if (req->is_prevote) {
        // Pre-vote: don't change our state, just check if we WOULD vote

        // Reject if their (hypothetical) term is behind our current term
        if (req->term < r->current_term) {
            return RAFT_OK;
        }

        // Check log up-to-date
        if (!log_is_up_to_date(r, req->last_log_term, req->last_log_index)) {
            return RAFT_OK;
        }

        // Reject if we have a live leader (prevent unnecessary elections)
        uint64_t elapsed = raft_get_time_ms() - r->last_heartbeat_ms;
        if (r->leader_id != -1 && elapsed < r->election_timeout_ms) {
            return RAFT_OK;
        }

        // Grant pre-vote ( not state change
        resp->vote_granted = 1;
        return RAFT_OK;
    }

    // ========================================================================
    // Real vote handling
    // ========================================================================

    // Reply false if term < currentTerm
    if (req->term < r->current_term) {
        return RAFT_OK;
    }

    // If RPC request contains term > currentTerm, update and convert to follower
    if (req->term > r->current_term) {
        raft_become_follower(r, req->term);
    }

    // Grant vote if votedFor is null or candidateId, and log is up-to-date
    int can_vote = (r->voted_for == -1 || r->voted_for == req->candidate_id);

    if (!can_vote) {
        return RAFT_OK;
    }

    if (!log_is_up_to_date(r, req->last_log_term, req->last_log_index)) {
        return RAFT_OK;
    }

    // Grant vote
    r->voted_for = req->candidate_id;
    r->last_heartbeat_ms = raft_get_time_ms();  // Reset election timeout

    // Persist vote
    if (r->callbacks.persist_vote) {
        r->callbacks.persist_vote(r->callback_ctx, r->current_term, r->voted_for);
    }

    resp->vote_granted = 1;
    resp->term = r->current_term;

    printf("[Node %d] Granted vote to %d for term %lu\n",
           r->my_id, req->candidate_id, req->term);

    return RAFT_OK;
}

// ============================================================================
// Handle RequestVote Response (real or pre-vote)
// ============================================================================

int raft_recv_requestvote_response(raft_t *r,
                                   int peer_id,
                                   const raft_requestvote_resp_t *resp) {
    if (!r || !resp) {
        return RAFT_ERR_INVALID_ARG;
    }

    // ========================================================================
    // Pre-vote response handling
    // ========================================================================
    if (resp->is_prevote) {
        // Ignore if we're not doing pre-vote anymore
        if (!r->prevote_in_progress) {
            return RAFT_OK;
        }

        // If response term is higher, step down and abandon pre-vote
        if (resp->term > r->current_term) {
            raft_become_follower(r, resp->term);
            return RAFT_OK;
        }

        // Count pre-vote (with deduplication)
        if (resp->vote_granted && !r->prevotes_for_me[peer_id]) {
            r->prevotes_for_me[peer_id] = 1;
            r->prevotes_received++;

            // Check if we have majority
            if (r->prevotes_received >= raft_quorum_size(r->num_nodes)) {
                r->prevote_in_progress = 0;
                raft_become_candidate(r);
            }
        }

        return RAFT_OK;
    }

    // ========================================================================
    // Real vote response handling
    // ========================================================================

    // Ignore if we're not a candidate anymore
    if (r->state != RAFT_STATE_CANDIDATE) {
        return RAFT_OK;
    }

    // If response contains higher term, step down
    if (resp->term > r->current_term) {
        raft_become_follower(r, resp->term);
        return RAFT_OK;
    }

    // Ignore stale responses
    if (resp->term < r->current_term) {
        return RAFT_OK;
    }

    // Count vote
    if (resp->vote_granted && !r->votes_for_me[peer_id]) {
        r->votes_for_me[peer_id] = 1;
        r->votes_received++;

        // majority check
        if (r->votes_received >= raft_quorum_size(r->num_nodes)) {
            raft_become_leader(r);
        }
    }

    return RAFT_OK;
}