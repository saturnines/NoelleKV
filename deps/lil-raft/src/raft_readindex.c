#include "raft_internal.h"
#include "raft_errors.h"
#include <string.h>
#include <stdio.h>

// ============================================================================
// ReadIndex Queue Management
// ============================================================================

int raft_readindex_init(raft_t *r) {
    if (!r) return RAFT_ERR_INVALID_ARG;

    r->pending_reads = calloc(RAFT_MAX_PENDING_READ_INDEX,
                               sizeof(raft_pending_read_t));
    if (!r->pending_reads) {
        return RAFT_ERR_NOMEM;
    }

    r->pending_reads_count = 0;
    return RAFT_OK;
}

void raft_readindex_free(raft_t *r) {
    if (!r) return;
    free(r->pending_reads);
    r->pending_reads = NULL;
    r->pending_reads_count = 0;
}

// ============================================================================
// Leader: Queue a ReadIndex request
// ============================================================================

static raft_pending_read_t* queue_read_index(raft_t *r, uint64_t req_id,
                                              int from_node) {
    if (r->pending_reads_count >= RAFT_MAX_PENDING_READ_INDEX) {
        return NULL;
    }

    // Find empty slot
    for (int i = 0; i < RAFT_MAX_PENDING_READ_INDEX; i++) {
        if (!r->pending_reads[i].active) {
            raft_pending_read_t *p = &r->pending_reads[i];

            p->req_id = req_id;
            p->from_node = from_node;
            p->commit_index = r->commit_index;
            p->term = r->current_term;
            p->ack_mask = (1ULL << r->my_id);
            // Only accept ACKs from heartbeats sent AFTER this read was queued
            // This prevents stale reads during partitions
            p->required_seq = r->heartbeat_seq + 1;

            p->active = 1;
            r->pending_reads_count++;
            return p;
        }
    }
    return NULL;
}

// ============================================================================
// Helper: Count bits in mask
// ============================================================================

static inline int popcount64(uint64_t x) {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_popcountll(x);
#else
    // Fallback for non-GCC/Clang
    int count = 0;
    while (x) {
        count += x & 1;
        x >>= 1;
    }
    return count;
#endif
}

// ============================================================================
// Leader: Check if pending ReadIndex requests can be satisfied
// ============================================================================

void raft_readindex_check_quorum(raft_t *r) {
    if (!r || r->state != RAFT_STATE_LEADER) return;
    if (!r->pending_reads) return;

    int quorum = raft_quorum_size(r->num_nodes);

    for (int i = 0; i < RAFT_MAX_PENDING_READ_INDEX; i++) {
        raft_pending_read_t *p = &r->pending_reads[i];
        if (!p->active) continue;

        // Stale term - fail it
        if (p->term != r->current_term) {
            if (p->from_node >= 0 && r->callbacks.send_readindex_resp) {
                r->callbacks.send_readindex_resp(r->callback_ctx,
                                                  p->from_node,
                                                  p->req_id,
                                                  0,
                                                  RAFT_ERR_STALE_TERM);
            } else if (p->from_node == -1 && r->callbacks.on_readindex_complete) {
                r->callbacks.on_readindex_complete(r->callback_ctx,
                                                    p->req_id,
                                                    0,
                                                    RAFT_ERR_STALE_TERM);
            }
            p->active = 0;
            r->pending_reads_count--;
            continue;
        }

        // Count unique acks via bitmask
        int ack_count = popcount64(p->ack_mask);

        // Have quorum?
        if (ack_count >= quorum) {
            if (p->from_node >= 0 && r->callbacks.send_readindex_resp) {
                // Remote follower request, send response
                r->callbacks.send_readindex_resp(r->callback_ctx,
                                                  p->from_node,
                                                  p->req_id,
                                                  r->commit_index,
                                                  0);
            } else if (p->from_node == -1 && r->callbacks.on_readindex_complete) {
                // Local leader request, callback
                r->callbacks.on_readindex_complete(r->callback_ctx,
                                                    p->req_id,
                                                    r->commit_index,
                                                    0);
            }
            p->active = 0;
            r->pending_reads_count--;
        }
    }
}

// ============================================================================
// Leader: Record heartbeat ACK for ReadIndex quorum tracking
// ============================================================================

void raft_readindex_record_ack(raft_t *r, int peer_id, uint64_t seq) {
    if (!r || r->state != RAFT_STATE_LEADER) return;
    if (!r->pending_reads) return;
    if (peer_id < 0 || peer_id >= 64) return;  // Bitmask limit

    uint64_t peer_bit = (1ULL << peer_id);

    // Set bit for this peer on all pending reads from current term
    for (int i = 0; i < RAFT_MAX_PENDING_READ_INDEX; i++) {
        raft_pending_read_t *p = &r->pending_reads[i];
        if (p->active && p->term == r->current_term && seq >= p->required_seq) {
            p->ack_mask |= peer_bit;
        }
    }

    // Check if any can now complete
    raft_readindex_check_quorum(r);
}

// ============================================================================
// Leader: Handle incoming ReadIndex request from follower
// ============================================================================

int raft_recv_readindex(raft_t *r,
                        const raft_readindex_req_t *req,
                        raft_readindex_resp_t *resp) {
    if (!r || !req || !resp) {
        return RAFT_ERR_INVALID_ARG;
    }

    resp->req_id = req->req_id;
    resp->read_index = 0;
    resp->err = 0;

    // Not leader - reject
    if (r->state != RAFT_STATE_LEADER) {
        resp->err = RAFT_ERR_NOT_LEADER;
        return RAFT_OK;
    }

    // Single node cluster - respond immediately (we are the quorum)
    if (r->num_nodes == 1) {
        resp->read_index = r->commit_index;
        return RAFT_OK;
    }

    // Queue and wait for heartbeat confirmations
    raft_pending_read_t *p = queue_read_index(r, req->req_id, req->from_node);
    if (!p) {
        resp->err = RAFT_ERR_NOMEM;
        return RAFT_OK;
    }

    // Response will be sent asynchronously after heartbeat quorum
    return RAFT_OK;
}

// ============================================================================
// Follower: Request ReadIndex from leader (async)
// ============================================================================

int raft_request_read_index_async(raft_t *r, uint64_t req_id) {
    if (!r) {
        return RAFT_ERR_INVALID_ARG;
    }

    // If we're leader, handle locally
    if (r->state == RAFT_STATE_LEADER) {
        // Single node - complete immediately via callback
        if (r->num_nodes == 1) {
            if (r->callbacks.on_readindex_complete) {
                r->callbacks.on_readindex_complete(r->callback_ctx,
                                                    req_id,
                                                    r->commit_index,
                                                    0);
            }
            return RAFT_OK;
        }

        // Multi-node leader, queue for heartbeat confirmation
        raft_pending_read_t *p = queue_read_index(r, req_id, -1);  // -1 = local
        if (!p) {
            return RAFT_ERR_NOMEM;
        }

        // Trigger immediate heartbeat to speed up quorum collection
        raft_send_heartbeats(r);

        // Will complete via on_readindex_complete callback after quorum
        return RAFT_OK;
    }

    if (r->leader_id < 0) {
        return RAFT_ERR_NOT_LEADER;  // No known leader
    }

    if (!r->callbacks.send_readindex) {
        return RAFT_ERR_INVALID_ARG;  // No callback configured
    }

    raft_readindex_req_t req = {
        .req_id = req_id,
        .from_node = r->my_id,
    };

    return r->callbacks.send_readindex(r->callback_ctx, r->leader_id, &req);
}

// ============================================================================
// Follower: Handle ReadIndex response from leader
// ============================================================================

int raft_recv_readindex_response(raft_t *r,
                                  const raft_readindex_resp_t *resp) {
    if (!r || !resp) {
        return RAFT_ERR_INVALID_ARG;
    }

    // Forward to ALR layer via callback
    if (r->callbacks.on_readindex_complete) {
        r->callbacks.on_readindex_complete(r->callback_ctx,
                                            resp->req_id,
                                            resp->read_index,
                                            resp->err);
    }

    return RAFT_OK;
}

// ============================================================================
// Cleanup on term change / leader step down
// ============================================================================

void raft_readindex_clear_pending(raft_t *r) {
    if (!r || !r->pending_reads) return;

    for (int i = 0; i < RAFT_MAX_PENDING_READ_INDEX; i++) {
        raft_pending_read_t *p = &r->pending_reads[i];
        if (p->active) {
            // Fail all pending requests
            if (p->from_node >= 0 && r->callbacks.send_readindex_resp) {
                r->callbacks.send_readindex_resp(r->callback_ctx,
                                                  p->from_node,
                                                  p->req_id,
                                                  0,
                                                  RAFT_ERR_NOT_LEADER);
            } else if (p->from_node == -1 && r->callbacks.on_readindex_complete) {
                r->callbacks.on_readindex_complete(r->callback_ctx,
                                                    p->req_id,
                                                    0,
                                                    RAFT_ERR_NOT_LEADER);
            }
            p->active = 0;
        }
    }
    r->pending_reads_count = 0;
}

// ============================================================================
// Helper: Get term at log index
// ============================================================================

uint64_t raft_log_term_at(const raft_t *r, uint64_t index) {
    if (!r || index == 0) {
        return 0;
    }

    // Check if it's the snapshot boundary
    if (index == r->snapshot_last_index) {
        return r->snapshot_last_term;
    }

    // Check if before snapshot (compacted)
    if (index < r->snapshot_last_index) {
        return 0;  // Entry no longer exists
    }

    // Look up in log
    raft_entry_t *entry = raft_log_get((raft_log_t*)&r->log, index);
    if (!entry) {
        return 0;
    }

    return entry->term;
}