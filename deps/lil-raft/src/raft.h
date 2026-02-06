/**
 * raft.h - Public API for lil-raft
 */

#ifndef RAFT_H
#define RAFT_H

#include "raft_types.h"
#include "raft_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

// Opaque Raft handle
typedef struct raft raft_t;

// ============================================================================
// Lifecycle
// ============================================================================

raft_t* raft_create(int my_id,
                    int num_nodes,
                    const raft_config_t *config,
                    const raft_callbacks_t *callbacks,
                    void *callback_ctx);

void raft_destroy(raft_t *r);
int raft_shutdown(raft_t *r);

// ============================================================================
// Configuration
// ============================================================================

void raft_set_flags(raft_t *r, uint32_t flags);
uint32_t raft_get_flags(const raft_t *r);
const raft_config_t* raft_get_config(const raft_t *r);

// ============================================================================
// Periodic Tick
// ============================================================================

void raft_tick(raft_t *r);

// ============================================================================
// Client Operations
// ============================================================================

int raft_propose(raft_t *r, const void *data, size_t len);
int raft_propose_batch(raft_t *r, const void **entries, const size_t *lengths, size_t count);

// ============================================================================
// Read Synchronization (NOOP-based)
// ============================================================================

int raft_propose_noop(raft_t *r, uint64_t *sync_index);
int raft_is_applied(const raft_t *r, uint64_t index);
void raft_get_indexes(const raft_t *r, uint64_t *commit_index, uint64_t *applied_index);
uint64_t raft_get_pending_index(const raft_t *r);

// ============================================================================
// ReadIndex (for linearizable reads from followers - ALR support)
// ============================================================================

/**
 * Request read index asynchronously
 *
 * For leader: queues request, completes after heartbeat quorum via callback
 * For follower: sends ReadIndex RPC to leader, completes via callback
 *
 * Completion is signaled via on_readindex_complete callback.
 *
 * @param req_id  Caller-provided request ID for correlation
 * @return RAFT_OK if request sent/queued
 *         RAFT_ERR_NOT_LEADER if follower with no known leader
 *         RAFT_ERR_NOMEM if request queue full
 */
int raft_request_read_index_async(raft_t *r, uint64_t req_id);

/**
 * Get term of log entry at given index
 * @param index  Log index to query
 * @return Term at index, or 0 if index doesn't exist (compacted/beyond log)
 */
uint64_t raft_log_term_at(const raft_t *r, uint64_t index);

// ============================================================================
// Snapshot Operations
// ============================================================================

int raft_snapshot_create(raft_t *r);
void raft_get_snapshot_info(const raft_t *r, uint64_t *last_index, uint64_t *last_term);

// ============================================================================
// RPC Handlers
// ============================================================================

int raft_recv_requestvote(raft_t *r,
                          const raft_requestvote_req_t *req,
                          raft_requestvote_resp_t *resp);

int raft_recv_appendentries(raft_t *r,
                            const raft_appendentries_req_t *req,
                            const raft_entry_t *entries,
                            size_t n_entries,
                            raft_appendentries_resp_t *resp);

int raft_recv_requestvote_response(raft_t *r,
                                   int peer_id,
                                   const raft_requestvote_resp_t *resp);

int raft_recv_appendentries_response(raft_t *r,
                                     int peer_id,
                                     const raft_appendentries_resp_t *resp);

int raft_recv_installsnapshot(raft_t *r,
                               const raft_installsnapshot_req_t *req,
                               raft_installsnapshot_resp_t *resp);

int raft_recv_installsnapshot_response(raft_t *r,
                                        int peer_id,
                                        const raft_installsnapshot_resp_t *resp);


int raft_recv_readindex(raft_t *r,
                        const raft_readindex_req_t *req,
                        raft_readindex_resp_t *resp);

int raft_recv_readindex_response(raft_t *r,
                                  const raft_readindex_resp_t *resp);

// ============================================================================
// State Queries
// ============================================================================

int raft_is_leader(const raft_t *r);
uint64_t raft_get_term(const raft_t *r);
uint64_t raft_get_commit_index(const raft_t *r);
uint64_t raft_get_last_applied(const raft_t *r);
int raft_get_leader_id(const raft_t *r);
int raft_get_node_id(const raft_t *r);

// ============================================================================
// Helpers
// ============================================================================

/**
 * Get current monotonic time in milliseconds
 * @return Time in milliseconds since unspecified epoch (monotonic clock)
 */
uint64_t raft_get_time_ms(void);

#ifdef __cplusplus
}
#endif

#endif // RAFT_H