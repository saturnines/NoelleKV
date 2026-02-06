/**
 * raft_glue.h - Glue layer between lil-raft and lygus storage
 *
 * Bridges the Raft consensus library with the storage manager.
 * Handles entry serialization and callback wiring.
 */

#ifndef RAFT_GLUE_H
#define RAFT_GLUE_H

#include "raft.h"
#include "storage/storage_mgr.h"
#include "network/network.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Entry Serialization Format
// ============================================================================
//
// Raft entries are serialized as:
//   [type:1][klen:4][vlen:4][key:klen][value:vlen]
//
// Types:
//   GLUE_ENTRY_PUT    = 1
//   GLUE_ENTRY_DEL    = 2
//   GLUE_ENTRY_NOOP   = 3  (klen=0, vlen=0, no key/value)
//
// ============================================================================

#define GLUE_ENTRY_PUT   1
#define GLUE_ENTRY_DEL   2
#define GLUE_ENTRY_NOOP  3

#define GLUE_ENTRY_HEADER_SIZE  9  // 1 + 4 + 4

// ============================================================================
// Context
// ============================================================================

typedef struct raft_glue_ctx {
    storage_mgr_t *storage;
    network_t     *network;
    char           data_dir[256];

    // Snapshot receive buffer (for InstallSnapshot chunks)
    uint8_t       *snapshot_recv_buf;
    size_t         snapshot_recv_len;
    size_t         snapshot_recv_cap;
} raft_glue_ctx_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Initialize glue context
 *
 * Opens the storage manager and network layer.
 *
 * @param ctx         Context to initialize
 * @param data_dir    Base directory for all data
 * @param node_id     This node's ID
 * @param peers_file  Path to cluster.peers file
 * @return 0 on success, negative on error
 */
int glue_ctx_init(raft_glue_ctx_t *ctx, const char *data_dir,
                  int node_id, const char *peers_file);

/**
 * Destroy glue context
 */
void glue_ctx_destroy(raft_glue_ctx_t *ctx);

/**
 * Start network layer (call after init)
 */
int glue_ctx_start_network(raft_glue_ctx_t *ctx);

/**
 * Stop network layer
 */
void glue_ctx_stop_network(raft_glue_ctx_t *ctx);

// ============================================================================
// Entry Serialization Helpers
// ============================================================================

/**
 * Serialize a PUT entry
 *
 * @param buf      Output buffer
 * @param buf_cap  Buffer capacity
 * @param key      Key data
 * @param klen     Key length
 * @param val      Value data
 * @param vlen     Value length
 * @return Bytes written, or negative if buffer too small
 */
ssize_t glue_serialize_put(void *buf, size_t buf_cap,
                           const void *key, size_t klen,
                           const void *val, size_t vlen);

/**
 * Serialize a DELETE entry
 */
ssize_t glue_serialize_del(void *buf, size_t buf_cap,
                           const void *key, size_t klen);

/**
 * Serialize a NOOP entry
 */
ssize_t glue_serialize_noop(void *buf, size_t buf_cap);

/**
 * Parse entry header (doesn't copy key/value)
 *
 * @param data     Serialized entry
 * @param len      Data length
 * @param type_out Output: entry type
 * @param klen_out Output: key length
 * @param vlen_out Output: value length
 * @param key_out  Output: pointer to key within data
 * @param val_out  Output: pointer to value within data
 * @return 0 on success, negative on error
 */
int glue_parse_entry(const void *data, size_t len,
                     uint8_t *type_out,
                     uint32_t *klen_out, uint32_t *vlen_out,
                     const void **key_out, const void **val_out);

// ============================================================================
// Storage Callbacks (for raft_callbacks_t)
// ============================================================================

// Vote persistence
int glue_persist_vote(void *ctx, uint64_t term, int voted_for);
int glue_load_vote(void *ctx, uint64_t *term, int *voted_for);

// Log operations
int glue_log_append(void *ctx, uint64_t index, uint64_t term,
                    const void *data, size_t len);
int glue_log_get(void *ctx, uint64_t index, void *buf, size_t *len);
int glue_log_truncate_after(void *ctx, uint64_t index);
uint64_t glue_log_first_index(void *ctx);
uint64_t glue_log_last_index(void *ctx);
uint64_t glue_log_last_term(void *ctx);
int glue_restore_raft_log(raft_glue_ctx_t *ctx, raft_t *raft);

// State machine
int glue_apply_entry(void *ctx, uint64_t index, uint64_t term,
                     raft_entry_type_t type, const void *data, size_t len);

// ============================================================================
// Snapshot Callbacks (for raft_callbacks_t)
// ============================================================================

/**
 * Create a snapshot (async - kicks off fork, returns immediately)
 */
int glue_snapshot_create(void *ctx, uint64_t index, uint64_t term);

/**
 * Poll for async snapshot completion
 * @return 1 if done, 0 if still in progress
 */
int glue_snapshot_poll(void *ctx);

/**
 * Load snapshot metadata on startup
 */
int glue_snapshot_load(void *ctx, uint64_t *out_index, uint64_t *out_term);

/**
 * Read a chunk from snapshot (for sending to followers)
 */
int glue_snapshot_read(void *ctx, uint64_t offset, void *buf, size_t buf_len,
                       size_t *out_len, int *out_done);

/**
 * Write a chunk of snapshot (received from leader)
 */
int glue_snapshot_write(void *ctx, uint64_t offset, const void *data,
                        size_t len, int done);

/**
 * Restore state machine from received snapshot
 */
int glue_snapshot_restore(void *ctx, uint64_t index, uint64_t term);

// ============================================================================
// Network Callbacks (used by raft_callbacks_t)
// ============================================================================

int glue_send_requestvote(void *ctx, int peer_id,
                          const raft_requestvote_req_t *req);
int glue_send_appendentries(void *ctx, int peer_id,
                            const raft_appendentries_req_t *req,
                            const raft_entry_t *entries,
                            size_t n_entries);
int glue_send_installsnapshot(void *ctx, int peer_id,
                              const raft_installsnapshot_req_t *req);

int glue_log_fsync(void *ctx);

// ============================================================================
// Network Processing (call from main loop)
// ============================================================================

/**
 * Process incoming network messages
 *
 * Receives Raft messages from network and dispatches to raft_recv_* handlers.
 * Call this from your main loop alongside raft_tick().
 *
 * @param ctx   Glue context
 * @param raft  Raft instance
 * @return Number of messages processed
 */
int glue_process_network(raft_glue_ctx_t *ctx, raft_t *raft);

/**
 * Process incoming INV broadcasts
 *
 * @param ctx       Glue context
 * @param on_inv    Callback for each invalidation
 * @param user_data User data for callback
 * @return Number of INVs processed
 */
int glue_process_inv(raft_glue_ctx_t *ctx,
                     void (*on_inv)(const void *key, size_t klen, void *user_data),
                     void *user_data);

/**
 * Broadcast key invalidation to all peers
 */
int glue_broadcast_inv(raft_glue_ctx_t *ctx, const void *key, size_t klen);

/**
 * Read index from raft
 */
int glue_send_readindex(void *ctx, int peer_id, const raft_readindex_req_t *req);


int glue_send_readindex_resp(void *ctx, int peer_id, uint64_t req_id,
                             uint64_t index, int err);


// ============================================================================
// Helper: Build callbacks struct
// ============================================================================

static inline raft_callbacks_t glue_make_callbacks(void) {
    return (raft_callbacks_t){
        // Persistent state
        .persist_vote       = glue_persist_vote,
        .load_vote          = glue_load_vote,

        // Log operations
        .log_append         = glue_log_append,
        .log_append_batch   = NULL,  // Use single append
        .log_get            = glue_log_get,
        .log_truncate_after = glue_log_truncate_after,
        .log_first_index    = glue_log_first_index,
        .log_last_index     = glue_log_last_index,
        .log_last_term      = glue_log_last_term,

        // State machine
        .apply_entry        = glue_apply_entry,
        .apply_batch        = NULL,  // Use single apply

        // Network
        .send_requestvote   = glue_send_requestvote,
        .send_appendentries = glue_send_appendentries,
        .send_readindex     = glue_send_readindex,
        .send_readindex_resp = glue_send_readindex_resp,

        // Snapshots
        .snapshot_create      = glue_snapshot_create,
        .snapshot_poll        = glue_snapshot_poll,
        .snapshot_load        = glue_snapshot_load,
        .snapshot_read        = glue_snapshot_read,
        .snapshot_write       = glue_snapshot_write,
        .snapshot_restore     = glue_snapshot_restore,
        .send_installsnapshot = glue_send_installsnapshot,

        // Fsync
        .log_fsync          = glue_log_fsync,
    };
}

#ifdef __cplusplus
}
#endif

#endif // RAFT_GLUE_H