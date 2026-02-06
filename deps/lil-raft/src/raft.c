/**
 * raft.c - Core Raft state machine
 */

#define _POSIX_C_SOURCE 199309L

#include "raft_internal.h"
#include "raft_errors.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdio.h>
#include <unistd.h>

// ============================================================================
// Helpers
// ============================================================================

/**
 * Get current time in milliseconds
 */
uint64_t raft_get_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/**
 * Per-node PRNG (xorshift32)
 */
static inline uint32_t raft_rand(raft_t *r) {
    uint32_t x = r->prng_state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    r->prng_state = x;
    return x;
}

/**
 * Get random election timeout from config range
 */
uint64_t raft_random_election_timeout(raft_t *r) {
    uint32_t min = r->config.election_timeout_min_ms;
    uint32_t max = r->config.election_timeout_max_ms;
    uint32_t range = max - min;
    return min + (raft_rand(r) % range);
}

// ============================================================================
// State Transitions
// ============================================================================

void raft_become_follower(raft_t *r, uint64_t term) {
    if (!r) return;

    printf("[Node %d] Became FOLLOWER for term %lu\n", r->my_id, term);

    r->state = RAFT_STATE_FOLLOWER;
    r->current_term = term;
    r->voted_for = -1;
    r->leader_id = -1;
    r->last_heartbeat_ms = raft_get_time_ms();
    r->election_timeout_ms = raft_random_election_timeout(r);

    // Clear votes
    r->votes_received = 0;
    if (r->votes_for_me) {
        memset(r->votes_for_me, 0, r->num_nodes * sizeof(int));
    }

    // Clear state for prevoting
    r->prevote_in_progress = 0;
    r->prevotes_received = 0;

    // Persist term and vote
    if (r->callbacks.persist_vote) {
        r->callbacks.persist_vote(r->callback_ctx, r->current_term, r->voted_for);
    }
}

void raft_become_candidate(raft_t *r) {
    if (!r) return;

    r->prevote_in_progress = 0;
    r->prevotes_received = 0;

    // increase term
    r->current_term++;
    r->state = RAFT_STATE_CANDIDATE;
    r->voted_for = r->my_id;  // Vote for self
    r->leader_id = -1;
    r->last_heartbeat_ms = raft_get_time_ms();
    r->election_timeout_ms = raft_random_election_timeout(r);

    // Reset votes
    r->votes_received = 1;  // Vote for self
    if (r->votes_for_me) {
        memset(r->votes_for_me, 0, r->num_nodes * sizeof(int));
        r->votes_for_me[r->my_id] = 1;
    }

    printf("[Node %d] Became CANDIDATE for term %lu\n", r->my_id, r->current_term);

    // Persist term and vote
    if (r->callbacks.persist_vote) {
        r->callbacks.persist_vote(r->callback_ctx, r->current_term, r->voted_for);
    }

    // Send RequestVote to all peers, used in raft_election.c
    raft_send_requestvote_all(r);

    if (r->votes_received >= raft_quorum_size(r->num_nodes)) {
        raft_become_leader(r);
    }
}

void raft_become_leader(raft_t *r) {
    if (!r) return;

    r->state = RAFT_STATE_LEADER;
    r->leader_id = r->my_id;
    r->last_heartbeat_ms = raft_get_time_ms();

    r->prevote_in_progress = 0;
    r->prevotes_received = 0;

    printf("[Node %d] Became LEADER for term %lu\n", r->my_id, r->current_term);

    // Initialize leader state
    uint64_t last_index = raft_log_last_index(&r->log);
    for (int i = 0; i < r->num_nodes; i++) {
        r->peers[i].next_index = last_index + 1;
        r->peers[i].match_index = 0;
        r->peers[i].ae_inflight = 0;

        // Reset snapshot transfer state
        r->snapshot_send[i].in_progress = 0;
        r->snapshot_send[i].offset = 0;
    }

    // Send immediate heartbeat (in raft_replication.c)
    raft_send_heartbeats(r);

    uint64_t noop_idx;
    raft_propose_noop(r, &noop_idx);
}

// ============================================================================
// Lifecycle
// ============================================================================

// Default config (static so we can return pointer to it)
static const raft_config_t default_config = RAFT_CONFIG_DEFAULT;

raft_t* raft_create(int my_id,
                    int num_nodes,
                    const raft_config_t *config,
                    const raft_callbacks_t *callbacks,
                    void *callback_ctx) {
    if (!callbacks || num_nodes <= 0 || my_id < 0 || my_id >= num_nodes) {
        return NULL;
    }

    raft_t *r = calloc(1, sizeof(raft_t));
    if (!r) {
        return NULL;
    }

    // config
    r->my_id = my_id;
    r->num_nodes = num_nodes;
    r->config = config ? *config : default_config;
    r->callbacks = *callbacks;
    r->callback_ctx = callback_ctx;

    // Seed PRNG with entropy
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    r->prng_state = (uint32_t)(
        ts.tv_nsec ^
        (ts.tv_sec << 20) ^
        (my_id * 2654435761u) ^
        (my_id << 16) ^
        getpid() ^
        (uintptr_t)r
    );

    // Warm up PRNG
    for (int i = 0; i < my_id + 5; i++) {
        raft_rand(r);
    }

    if (raft_log_init(&r->log) != RAFT_OK) {
        free(r);
        return NULL;
    }

    // Allocate peer state
    r->peers = calloc(num_nodes, sizeof(raft_peer_t));
    r->votes_for_me = calloc(num_nodes, sizeof(int));
    r->prevotes_for_me = calloc(num_nodes, sizeof(int));
    r->snapshot_send = calloc(num_nodes, sizeof(raft_snapshot_progress_t));

    if (!r->peers || !r->votes_for_me || !r->prevotes_for_me || !r->snapshot_send) {
        raft_log_free(&r->log);
        free(r->peers);
        free(r->votes_for_me);
        free(r->prevotes_for_me);
        free(r->snapshot_send);
        free(r);
        return NULL;
    }

    // Initialize snapshot state
    r->snapshot_last_index = 0;
    r->snapshot_last_term = 0;
    r->snapshot_in_progress = 0;
    r->snapshot_pending_index = 0;
    r->snapshot_pending_term = 0;

    // Initialize ReadIndex state
    if (raft_readindex_init(r) != RAFT_OK) {
        raft_log_free(&r->log);
        free(r->peers);
        free(r->votes_for_me);
        free(r->prevotes_for_me);
        free(r->snapshot_send);
        free(r);
        return NULL;
    }

    // Load persistent state
    uint64_t term = 0;
    int voted_for = -1;
    if (callbacks->load_vote) {
        callbacks->load_vote(callback_ctx, &term, &voted_for);
    }

    r->current_term = term;
    r->voted_for = voted_for;

    // Initialize volatile state
    r->state = RAFT_STATE_FOLLOWER;
    r->commit_index = 0;
    r->last_applied = 0;
    r->leader_id = -1;

    // Prevote state
    r->prevote_in_progress = 0;
    r->prevotes_received = 0;

    // Load snapshot if one exists
    raft_snapshot_restore(r);

    // Timing
    r->election_timeout_ms = raft_random_election_timeout(r);
    r->last_heartbeat_ms = raft_get_time_ms();
    clock_gettime(CLOCK_MONOTONIC, &r->last_tick);

    return r;
}

void raft_destroy(raft_t *r) {
    if (!r) return;

    raft_log_free(&r->log);
    free(r->peers);
    free(r->votes_for_me);
    free(r->prevotes_for_me);
    free(r->snapshot_send);
    free(r);
}

int raft_shutdown(raft_t *r) {
    if (!r) {
        return RAFT_ERR_INVALID_ARG;
    }

    r->shutdown = 1;

    // TODO: Flush pending operations
    // TODO: Wait for in-flight RPCs

    return RAFT_OK;
}

// ============================================================================
// Configuration
// ============================================================================

void raft_set_flags(raft_t *r, uint32_t flags) {
    if (!r) return;
    r->config.flags = flags;
}

uint32_t raft_get_flags(const raft_t *r) {
    return r ? r->config.flags : 0;
}

const raft_config_t* raft_get_config(const raft_t *r) {
    return r ? &r->config : NULL;
}

// ============================================================================
// Tick for progress, elections, heartbeats
// ============================================================================

void raft_tick(raft_t *r) {
    if (!r || r->shutdown) return;

    // Poll for async snapshot completion
    if (r->snapshot_in_progress) {
        raft_snapshot_poll(r);
    }

    uint64_t now = raft_get_time_ms();
    uint64_t elapsed = now - r->last_heartbeat_ms;

    // Follower or Candidate: Check election timeout
    if (r->state == RAFT_STATE_FOLLOWER || r->state == RAFT_STATE_CANDIDATE) {
        if (elapsed >= r->election_timeout_ms) {
            r->leader_id = -1;
            if (r->config.flags & RAFT_FLAG_PREVOTE_ENABLED) {
                raft_start_prevote(r);
            } else {
                raft_become_candidate(r);
            }
        }
    }

    // Leader: Send heartbeats
    if (r->state == RAFT_STATE_LEADER) {
        if (elapsed >= r->config.heartbeat_interval_ms) {
            raft_send_heartbeats(r);
            r->last_heartbeat_ms = now;
        }
    }

    // Apply committed entries
    while (r->last_applied < r->commit_index) {
        raft_entry_t *entry = raft_log_get(&r->log, r->last_applied + 1);
        if (!entry) {
            break; // allow snap to popup
        }

        r->last_applied++;

        if (r->callbacks.apply_entry) {
            r->callbacks.apply_entry(r->callback_ctx,
                                    entry->index,
                                    entry->term,
                                    entry->type,
                                    entry->data,
                                    entry->len);
        }
    }

    // Check if we should create a snapshot
    if (r->config.flags & RAFT_FLAG_AUTO_SNAPSHOT) {
        raft_maybe_snapshot(r);
    }
}
// ============================================================================
// State Queries
// ============================================================================

int raft_is_leader(const raft_t *r) {
    return r && r->state == RAFT_STATE_LEADER;
}

uint64_t raft_get_term(const raft_t *r) {
    return r ? r->current_term : 0;
}

uint64_t raft_get_commit_index(const raft_t *r) {
    return r ? r->commit_index : 0;
}

uint64_t raft_get_last_applied(const raft_t *r) {
    return r ? r->last_applied : 0;
}

int raft_get_leader_id(const raft_t *r) {
    return r ? r->leader_id : -1;
}

int raft_get_node_id(const raft_t *r) {
    return r ? r->my_id : -1;
}

// ============================================================================
// ALR's Read Operations
// ============================================================================

int raft_is_applied(const raft_t *r, uint64_t index) {
    if (!r) return 0;
    return r->last_applied >= index;
}

void raft_get_indexes(const raft_t *r,
                      uint64_t *commit_index,
                      uint64_t *applied_index) {
    if (!r) {
        if (commit_index) *commit_index = 0;
        if (applied_index) *applied_index = 0;
        return;
    }

    if (commit_index) *commit_index = r->commit_index;
    if (applied_index) *applied_index = r->last_applied;
}

uint64_t raft_get_pending_index(const raft_t *r) {
    if (!r) return 0;

    uint64_t last_idx = raft_log_last_index(&r->log);
    uint64_t last_term = raft_log_last_term(&r->log);

    // EXPERIMENTAL UNSURE IF WORKS
    if (last_idx > r->commit_index && last_term == r->current_term) {
        return last_idx;
    }

    return 0;
}