#ifndef LYGUS_SNAPSHOT_H
#define LYGUS_SNAPSHOT_H

#include <stdint.h>
#include <stddef.h>

#include "state/kv_store.h"
#include "public/lygus_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Constants
// ============================================================================

#define SNAPSHOT_MAGIC   0x504E4153U  // "SNAP"
#define SNAPSHOT_VERSION 1

// ============================================================================
// Snapshot Header (on-disk format)
// ============================================================================

#ifdef _MSC_VER
    #pragma pack(push, 1)
    typedef struct {
        uint32_t magic;        // SNAPSHOT_MAGIC
        uint16_t version;      // SNAPSHOT_VERSION
        uint16_t flags;        // Reserved for future use
        uint64_t last_index;   // Raft index this snapshot covers
        uint64_t last_term;    // Raft term of last_index
        uint64_t entry_count;  // Number of KV entries
    } snapshot_hdr_t;
    #pragma pack(pop)
#else
    typedef struct {
        uint32_t magic;        // SNAPSHOT_MAGIC
        uint16_t version;      // SNAPSHOT_VERSION
        uint16_t flags;        // Reserved for future use
        uint64_t last_index;   // Raft index this snapshot covers
        uint64_t last_term;    // Raft term of last_index
        uint64_t entry_count;  // Number of KV entries
    } __attribute__((packed)) snapshot_hdr_t;

    _Static_assert(sizeof(snapshot_hdr_t) == 32, "Snapshot header must be 32 bytes");
#endif

// ============================================================================
// Async Snapshot State (unified - no platform #ifdefs)
// ============================================================================

// Forward declarations for opaque platform types
typedef struct lygus_async_proc lygus_async_proc_t;

typedef struct {
    lygus_async_proc_t *proc;   // Platform async handle
    void               *ctx;    // Worker context (freed on cleanup)
    uint64_t            index;  // Snapshot index
    uint64_t            term;   // Snapshot term
    char                path[256];  // Path to snapshot file
} snapshot_async_t;

// ============================================================================
// Snapshot Write (called from child process after fork, or inline on Windows)
// ============================================================================

/**
 * Write KV state to snapshot file
 *
 * Serializes all KV entries to disk with header and CRC footer.
 * On POSIX, this is called from a forked child process to avoid
 * blocking the parent. On Windows, called inline (blocking).
 *
 * File format:
 *   [header 32 bytes]
 *   [entries: klen varint, vlen varint, key bytes, value bytes]...
 *   [crc32c 4 bytes]
 *
 * Writes to a temp file first, then renames atomically on success.
 *
 * @param kv          KV store to snapshot
 * @param last_index  Raft index this snapshot covers
 * @param last_term   Raft term of last_index
 * @param path        Output file path
 * @return            LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL kv or path
 *   LYGUS_ERR_OPEN_FILE    - Failed to create file
 *   LYGUS_ERR_WRITE        - Write failed
 *   LYGUS_ERR_FSYNC        - Fsync failed
 *   LYGUS_ERR_IO           - Rename failed
 */
int snapshot_write(lygus_kv_t *kv,
                   uint64_t last_index,
                   uint64_t last_term,
                   const char *path);

// ============================================================================
// Snapshot Load (recovery or InstallSnapshot)
// ============================================================================

/**
 * Load snapshot into KV store
 *
 * Clears existing KV data, then populates from snapshot file.
 * Validates magic, version, and CRC before loading.
 *
 * @param path       Snapshot file path
 * @param kv         KV store to populate (will be cleared first)
 * @param out_index  Output: last_index from snapshot
 * @param out_term   Output: last_term from snapshot
 * @return           LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG   - NULL pointers
 *   LYGUS_ERR_OPEN_FILE     - Failed to open file
 *   LYGUS_ERR_READ          - Read failed
 *   LYGUS_ERR_BAD_SNAPSHOT  - Bad magic or version
 *   LYGUS_ERR_CORRUPT       - CRC mismatch
 *   LYGUS_ERR_MALFORMED     - Invalid entry format
 *   LYGUS_ERR_NOMEM         - Out of memory
 */
int snapshot_load(const char *path,
                  lygus_kv_t *kv,
                  uint64_t *out_index,
                  uint64_t *out_term);

// ============================================================================
// Snapshot Metadata (peek without loading)
// ============================================================================

/**
 * Read snapshot header without loading entries
 *
 * Useful for finding latest snapshot, checking if snapshot exists,
 * or comparing snapshot index to WAL index.
 *
 * @param path        Snapshot file path
 * @param out_index   Output: last_index (can be NULL)
 * @param out_term    Output: last_term (can be NULL)
 * @param out_count   Output: entry_count (can be NULL)
 * @return            LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG   - NULL path
 *   LYGUS_ERR_OPEN_FILE     - Failed to open file
 *   LYGUS_ERR_READ          - Read failed
 *   LYGUS_ERR_BAD_SNAPSHOT  - Bad magic or version
 */
int snapshot_read_header(const char *path,
                         uint64_t *out_index,
                         uint64_t *out_term,
                         uint64_t *out_count);

// ============================================================================
// Async Snapshot (fork on POSIX, synchronous on Windows)
// ============================================================================

/**
 * Create snapshot asynchronously
 *
 * On POSIX: Forks a child process that writes the snapshot. Parent returns
 * immediately and can continue serving requests. Child process sees
 * copy-on-write memory, so it has a consistent view of KV state.
 *
 * On Windows: Falls back to synchronous write (blocks until complete).
 * The API remains the same for both platforms.
 *
 * Call snapshot_poll() or snapshot_wait() to check completion status.
 * Call snapshot_async_cleanup() when done to free resources.
 *
 * @param kv          KV store to snapshot
 * @param last_index  Raft index this snapshot covers
 * @param last_term   Raft term of last_index
 * @param path        Output file path
 * @param out_async   Output: async state (caller must track this)
 * @return            LYGUS_OK on success, negative on error
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL pointers
 *   LYGUS_ERR_NOMEM        - Failed to allocate context
 *   LYGUS_ERR_INTERNAL     - fork() failed (POSIX only)
 */
int snapshot_create_async(lygus_kv_t *kv,
                          uint64_t last_index,
                          uint64_t last_term,
                          const char *path,
                          snapshot_async_t *out_async);

/**
 * Poll for async snapshot completion (non-blocking)
 *
 * @param async       Async state from snapshot_create_async
 * @param out_done    Output: 1 if finished, 0 if still running
 * @param out_success Output: 1 if succeeded, 0 if failed (only valid if done)
 * @return            LYGUS_OK, or negative error code
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL async or proc not initialized
 */
int snapshot_poll(snapshot_async_t *async,
                  int *out_done,
                  int *out_success);

/**
 * Wait for async snapshot to complete (blocking)
 *
 * @param async       Async state from snapshot_create_async
 * @param out_success Output: 1 if succeeded, 0 if failed
 * @return            LYGUS_OK, or negative error code
 */
int snapshot_wait(snapshot_async_t *async,
                  int *out_success);

/**
 * Cleanup async snapshot resources
 *
 * Frees the async process handle and worker context.
 * Safe to call multiple times or with NULL.
 *
 * @param async  Async state to cleanup
 */
void snapshot_async_cleanup(snapshot_async_t *async);

// ============================================================================
// Snapshot Directory Management
// ============================================================================

/**
 * Generate snapshot filename from index and term
 *
 * Format: {dir}/snap-{index:016x}-{term:016x}.dat
 *
 * @param dir       Directory path
 * @param index     Raft index
 * @param term      Raft term
 * @param out       Output buffer
 * @param out_len   Output buffer size
 * @return          LYGUS_OK, or LYGUS_ERR_INVALID_ARG if buffer too small
 */
int snapshot_path_from_index(const char *dir,
                             uint64_t index,
                             uint64_t term,
                             char *out,
                             size_t out_len);

/**
 * Find latest snapshot in directory
 *
 * Scans directory for snap-*.dat files and returns the one
 * with the highest index.
 *
 * @param dir         Directory to scan
 * @param out_path    Output: path to latest snapshot (must be >= 256 bytes)
 * @param out_index   Output: index of latest snapshot (can be NULL)
 * @param out_term    Output: term of latest snapshot (can be NULL)
 * @return            LYGUS_OK if found, LYGUS_ERR_KEY_NOT_FOUND if no snapshots
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG   - NULL dir or out_path
 *   LYGUS_ERR_IO            - Failed to read directory
 *   LYGUS_ERR_KEY_NOT_FOUND - No snapshots found
 */
int snapshot_find_latest(const char *dir,
                         char *out_path,
                         uint64_t *out_index,
                         uint64_t *out_term);

/**
 * Delete old snapshots, keeping the N most recent
 *
 * @param dir    Directory containing snapshots
 * @param keep   Number of snapshots to keep
 * @return       LYGUS_OK on success, negative on error
 */
int snapshot_purge_old(const char *dir, int keep);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_SNAPSHOT_H