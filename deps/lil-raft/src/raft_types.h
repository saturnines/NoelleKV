/**
 * raft_types.h - Public types for lil-raft
 */

#ifndef RAFT_TYPES_H
#define RAFT_TYPES_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Feature Flags (for testing/ablation)
// ============================================================================

#define RAFT_FLAG_PREVOTE_ENABLED   (1 << 0)  // Pre-vote prevents disruptive rejoins
#define RAFT_FLAG_CHECKQUORUM       (1 << 1)  // Future: leader steps down without quorum
#define RAFT_FLAG_LEARNER_SUPPORT   (1 << 2)  // Future: non-voting members
#define RAFT_FLAG_AUTO_SNAPSHOT     (1 << 3)  // Automatically snapshot when log grows

// Default flags
#define RAFT_FLAGS_DEFAULT          (RAFT_FLAG_PREVOTE_ENABLED)

// ============================================================================
// Configuration
// ============================================================================

typedef struct {
    // Feature flags
    uint32_t flags;

    // Election timeout range (ms)
    // Actual timeout is randomly chosen in [min, max) for each election
    // Note: most production timeouts are arbitrary, try to use 100x the p90 recovery time if you decide to actually use this in prod
    uint32_t election_timeout_min_ms;
    uint32_t election_timeout_max_ms;

    // Heartbeat interval (ms)
    // Leader sends heartbeats at this interval to maintain authority
    uint32_t heartbeat_interval_ms;

    // Max entries per AppendEntries RPC
    // Limits memory usage and network message size
    // 0 = unlimited
    uint32_t max_entries_per_msg;

    // Max bytes per AppendEntries RPC (approximate)
    // 0 = unlimited
    uint32_t max_bytes_per_msg;

    // Snapshot threshold - trigger snapshot when log has this many entries
    // beyond the last snapshot. 0 = manual snapshots only.
    uint32_t snapshot_threshold;

} raft_config_t;

// Sensible defaults (Raft paper recommendations)
// Previous interval ms was 50, changing to 5
#define RAFT_CONFIG_DEFAULT { \
    .flags                   = RAFT_FLAGS_DEFAULT, \
    .election_timeout_min_ms = 1000, \
    .election_timeout_max_ms = 2000, \
    .heartbeat_interval_ms   = 100, \
    .max_entries_per_msg     = 64, \
    .max_bytes_per_msg       = 0, \
    .snapshot_threshold      = 10000, \
}

// ============================================================================
// RequestVote RPC
// ============================================================================

typedef struct {
    uint64_t term;           // Candidate's term
    int candidate_id;        // Candidate requesting vote
    uint64_t last_log_index; // Index of candidate's last log entry
    uint64_t last_log_term;  // Term of candidate's last log entry
    int is_prevote;          // 1 if this is a pre-vote (doesn't increment term)
} raft_requestvote_req_t;

typedef struct {
    uint64_t term;        // Current term, for candidate to update itself
    int vote_granted;     // 1 if candidate received vote, 0 otherwise
    int is_prevote;       // 1 if this is a pre-vote response
} raft_requestvote_resp_t;

// ============================================================================
// AppendEntries RPC
// ============================================================================

typedef struct {
    uint64_t term;           // Leader's term
    int leader_id;           // So follower can redirect clients
    uint64_t prev_log_index; // Index of log entry immediately preceding new ones
    uint64_t prev_log_term;  // Term of prev_log_index entry
    uint64_t leader_commit;  // Leader's commit_index
    uint64_t seq;            // Heartbeat Sequence ID
} raft_appendentries_req_t;

typedef struct {
    uint64_t term;        // Current term, for leader to update itself
    int success;          // 1 if follower contained entry matching prev_log_index/term
    uint64_t conflict_term; // term of conflict entry
    uint64_t conflict_index;  // First idx of conflict term
    uint64_t match_index; // Highest index known to be replicated (for fast rollback)
    uint64_t seq ;         // Echo back to Leader
} raft_appendentries_resp_t;

// ============================================================================
// InstallSnapshot RPC
// ============================================================================

#define RAFT_SNAPSHOT_CHUNK_SIZE 65536  // 64KB chunks

typedef struct {
    uint64_t term;           // Leader's term
    int leader_id;           // So follower can redirect clients
    uint64_t last_index;     // Last index included in snapshot
    uint64_t last_term;      // Term of last_index
    uint64_t offset;         // Byte offset in snapshot file
    const void *data;        // Snapshot chunk data
    size_t len;              // Chunk length
    int done;                // 1 if this is the last chunk
} raft_installsnapshot_req_t;

typedef struct {
    uint64_t term;           // Current term, for leader to update itself
    int success;             // 1 if chunk was received successfully
    uint64_t last_offset;    // Offset after this chunk (for next chunk)
    int done;                // Used for Snapshotting (edgecase of where leader thinks peer needs a snapshot)
} raft_installsnapshot_resp_t;

// ============================================================================
// ReadIndex RPC (for linearizable reads - ALR support)
// ============================================================================

typedef struct {
    uint64_t req_id;     // Unique request ID (for correlating response)
    int      from_node;  // Requesting node ID
} raft_readindex_req_t;

typedef struct {
    uint64_t req_id;      // Echoed from request
    uint64_t read_index;  // Commit index safe for linearizable reads (0 if queued/error)
    int      err;         // 0 on success, RAFT_ERR_* on failure
} raft_readindex_resp_t;

// ============================================================================
// Log Entry
// ============================================================================

/**
 * Entry types for distinguishing regular writes from control entries
 */
typedef enum {
    RAFT_ENTRY_DATA = 0,    // Normal client data
    RAFT_ENTRY_NOOP = 1,    // Noop for read synchronization
    RAFT_ENTRY_CONFIG = 2,  // Configuration change
} raft_entry_type_t;

typedef struct {
    uint64_t index;          // Raft log index
    uint64_t term;           // Term when entry was received by leader
    raft_entry_type_t type;  // Entry type (DATA, NOOP, CONFIG)
    void *data;              // User data (payload, NULL for NOOP)
    size_t len;              // Data length (0 for NOOP)
} raft_entry_t;

// ============================================================================
// Callbacks
// ============================================================================

typedef struct {
    /**
     * Persist current term and voted_for to stable storage
     * Called before responding to RequestVote or starting election
     *
     * @return 0 on success, non-zero on error
     */
    int (*persist_vote)(void *ctx, uint64_t term, int voted_for);

    /**
     * Load current term and voted_for from stable storage
     * Called during raft_create()
     *
     * @return 0 on success, non-zero on error
     */
    int (*load_vote)(void *ctx, uint64_t *term, int *voted_for);

    /**
     * Append entry to persistent log
     * Called when leader receives client request or follower receives AppendEntries
     *
     * @return 0 on success, non-zero on error
     */
    int (*log_append)(void *ctx, uint64_t index, uint64_t term,
                      const void *data, size_t len);

    /**
     * Append multiple entries atomically (optional optimization)
     * If NULL, raft will call log_append() for each entry
     * More efficient for batched writes
     *
     * @param entries Array of entries to append
     * @param count   Number of entries
     * @return 0 on success, non-zero on error
     */
    int (*log_append_batch)(void *ctx,
                           const raft_entry_t *entries,
                           size_t count);

    /**
     * Get entry from persistent log
     *
     * @param buf Output buffer (NULL to query size)
     * @param len Input: buffer size, Output: actual entry size
     * @return 0 on success, non-zero if not found or error
     */
    int (*log_get)(void *ctx, uint64_t index, void *buf, size_t *len);

    /**
     * Remove entries after (and including) given index
     * Called when leader detects log conflict
     *
     * @return 0 on success, non-zero on error
     */
    int (*log_truncate_after)(void *ctx, uint64_t index);

    /**
     * Get first index in log (after compaction)
     * @return First index, or 0 if log is empty
     */
    uint64_t (*log_first_index)(void *ctx);

    /**
     * Get last index in log
     * @return Last index, or 0 if log is empty
     */
    uint64_t (*log_last_index)(void *ctx);

    /**
     * Get term of last entry in log
     * @return Last term, or 0 if log is empty
     */
    uint64_t (*log_last_term)(void *ctx);


    int (*log_fsync)(void *ctx);  // Fsync after batch

    // --- State Machine Application ---

    /**
     * Apply committed entry to state machine
     * Called when entry is committed (in order, once per entry)
     *
     * For NOOP entries (type == RAFT_ENTRY_NOOP), data is NULL and len is 0.
     * The state machine should not modify state for NOOPs but may use them
     * as synchronization points (e.g., for Lazy-ALR read optimization).
     *
     * @return 0 on success, non-zero on error
     */
    int (*apply_entry)(void *ctx, uint64_t index, uint64_t term,
                      raft_entry_type_t type, const void *data, size_t len);

    /**
     * Apply multiple committed entries (optional optimization)
     * If NULL, raft will call apply_entry() for each entry
     * More efficient when committing many entries at once
     *
     * @param entries Array of entries to apply
     * @param count   Number of entries
     * @return 0 on success, non-zero on error
     */
    int (*apply_batch)(void *ctx,
                      const raft_entry_t *entries,
                      size_t count);

    // --- Network Communication ---

    /**
     * Send RequestVote RPC to peer
     * Implementation should be non-blocking
     * Response comes back via raft_recv_requestvote_response()
     *
     * @return 0 if sent, non-zero on error
     */
    int (*send_requestvote)(void *ctx, int peer_id,
                           const raft_requestvote_req_t *req);

    /**
     * Send AppendEntries RPC to peer
     * Implementation should be non-blocking
     * Response comes back via raft_recv_appendentries_response()
     *
     * @param entries Array of entries to send (can be NULL for heartbeat)
     * @param n_entries Number of entries
     * @return 0 if sent, non-zero on error
     */
    int (*send_appendentries)(void *ctx, int peer_id,
                             const raft_appendentries_req_t *req,
                             const raft_entry_t *entries,
                             size_t n_entries);

    // --- Snapshots ---

    /**
     * Create a snapshot of current state machine
     *
     * Application should serialize its state to stable storage,
     * tagged with the given index and term.
     *
     * Can be async (e.g., fork-based) - if async, return immediately
     * and implement snapshot_poll() to signal completion.
     * Raft will truncate the log after snapshot completes.
     *
     * @param index Raft index this snapshot covers (all entries up to here)
     * @param term  Term of the entry at index
     * @return 0 on success (or async started), non-zero on error
     */
    int (*snapshot_create)(void *ctx, uint64_t index, uint64_t term);

    /**
     * Poll for async snapshot completion
     *
     * If snapshot_create() is async (e.g., fork-based, returns immediately),
     * this callback checks if the snapshot is done.
     *
     * If NULL, snapshot_create() is assumed to be synchronous.
     *
     * @return 1 if snapshot complete (success or failure), 0 if still in progress
     */
    int (*snapshot_poll)(void *ctx);

    /**
     * Load latest snapshot on startup
     *
     * Application should restore state from snapshot and return
     * the snapshot's metadata. Called during raft_create().
     *
     * @param out_index Output: last index in snapshot
     * @param out_term  Output: term of last index
     * @return 0 on success
     *         RAFT_ERR_SNAPSHOT_NOT_FOUND if no snapshot exists (not an error)
     *         Other negative value on error
     */
    int (*snapshot_load)(void *ctx, uint64_t *out_index, uint64_t *out_term);

    /**
     * Read a chunk from the current snapshot (for sending to followers)
     *
     * Leader calls this to get data to send via InstallSnapshot RPC.
     *
     * @param offset   Byte offset to read from
     * @param buf      Output buffer
     * @param buf_len  Buffer size
     * @param out_len  Output: actual bytes read
     * @param out_done Output: 1 if this is the last chunk
     * @return 0 on success, non-zero on error
     */
    int (*snapshot_read)(void *ctx, uint64_t offset, void *buf, size_t buf_len,
                         size_t *out_len, int *out_done);

    /**
     * Write a chunk of snapshot (received from leader)
     *
     * Follower calls this to write chunks received via InstallSnapshot.
     *
     * @param offset Byte offset to write at
     * @param data   Chunk data
     * @param len    Chunk length
     * @param done   1 if this is the last chunk
     * @return 0 on success, non-zero on error
     */
    int (*snapshot_write)(void *ctx, uint64_t offset, const void *data,
                          size_t len, int done);

    /**
     * Restore state machine from received snapshot
     *
     * Called after all chunks are received via InstallSnapshot.
     * Application should load the snapshot into its state machine.
     *
     * @param index Last index in snapshot
     * @param term  Term of last index
     * @return 0 on success, non-zero on error
     */
    int (*snapshot_restore)(void *ctx, uint64_t index, uint64_t term);

    /**
     * Send InstallSnapshot RPC to peer
     *
     * @param peer_id Target peer
     * @param req     Request data (includes chunk)
     * @return 0 if sent, non-zero on error
     */
    int (*send_installsnapshot)(void *ctx, int peer_id,
                                const raft_installsnapshot_req_t *req);

    // --- ReadIndex (for linearizable reads) ---

    /**
     * Send ReadIndex request to leader (follower -> leader)
     *
     * @param peer_id  Leader's node ID
     * @param req      Request data
     * @return 0 if sent, non-zero on error
     */
    int (*send_readindex)(void *ctx, int peer_id,
                          const raft_readindex_req_t *req);

    /**
     * Send ReadIndex response to follower (leader -> follower)
     *
     * @param peer_id  Follower's node ID
     * @param req_id   Request ID being responded to
     * @param index    Read index (0 if error)
     * @param err      0 on success, RAFT_ERR_* on failure
     * @return 0 if sent, non-zero on error
     */
    int (*send_readindex_resp)(void *ctx, int peer_id,
                                uint64_t req_id, uint64_t index, int err);

    /**
     * ReadIndex completion callback (for local ALR layer)
     *
     * Called when a ReadIndex request completes, either:
     * - On leader: after heartbeat quorum confirms leadership
     * - On follower: when response received from leader
     *
     * The ALR layer should call alr_on_read_index() from this callback.
     *
     * @param req_id  Request ID
     * @param index   Read index (0 if error)
     * @param err     0 on success, error code on failure
     */
    void (*on_readindex_complete)(void *ctx, uint64_t req_id,
                                   uint64_t index, int err);

} raft_callbacks_t;

#ifdef __cplusplus
}
#endif

#endif // RAFT_TYPES_H