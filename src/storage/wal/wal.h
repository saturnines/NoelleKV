#ifndef LYGUS_WAL_H
#define LYGUS_WAL_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#include "block_format.h"
#include "recovery.h"
#include "../../public/lygus_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// WAL Handle
// ============================================================================

/**
 * Opaque WAL handle
 *
 * Manages write-ahead log with:
 * - Automatic recovery on open
 * - Block buffering and compression
 * - Segment rotation
 * - Durability guarantees
 * - In-memory index for O(1) entry lookup
 */
typedef struct wal wal_t;

// ============================================================================
// Options
// ============================================================================

typedef struct {
    const char *data_dir;           // Directory for WAL segments
    int         zstd_level;         // Compression level (3-5 recommended, 0 = default)

    // Group commit tuning
    size_t      fsync_bytes;        // Auto-fsync after N bytes (0 = manual)
    uint64_t    fsync_interval_us;  // Auto-fsync after N microseconds (0 = manual)

    // Callbacks
    wal_entry_callback_t on_recover; // Called for each entry during recovery
    void                *user_data;  // Passed to on_recover callback
} wal_opts_t;

// ============================================================================
// Statistics
// ============================================================================

typedef struct {
    // Recovery stats (from last open)
    uint64_t recovered_entries;
    uint64_t recovered_blocks;
    uint64_t highest_index;
    uint64_t highest_term;
    int      had_corruption;

    // Current writer state
    uint64_t segment_num;
    uint64_t write_offset;
    uint64_t block_seq;
    size_t   block_fill;

    // Index state
    uint64_t first_index;      // First index in log (after compaction)
    uint64_t last_index;       // Last index in log

    // Runtime counters
    uint64_t appends_total;
    uint64_t flushes_total;
    uint64_t fsyncs_total;
    uint64_t bytes_written;
} wal_stats_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Open or create WAL
 *
 * 1. Runs recovery on all existing segments
 * 2. Builds in-memory index for O(1) lookup
 * 3. Invokes on_recover callback for each entry
 * 4. Opens latest segment for appending (or creates new one)
 * 5. Ready for writes
 *
 * @param opts  Configuration options
 * @return      WAL handle, or NULL on error (check errno)
 *
 * Note: Recovery can take time on large WALs. Consider showing progress.
 */
wal_t* wal_open(const wal_opts_t *opts);

/**
 * Close WAL and flush all pending data
 *
 * Marks last block, flushes, fsyncs, and closes segment.
 *
 * @param w  WAL handle
 * @return   LYGUS_OK on success, negative error code otherwise
 */
int wal_close(wal_t *w);

// ============================================================================
// Write Operations
// ============================================================================

/**
 * Append PUT entry
 *
 * @param w      WAL handle
 * @param index  Raft log index
 * @param term   Raft term
 * @param key    Key bytes
 * @param klen   Key length
 * @param val    Value bytes
 * @param vlen   Value length
 * @return       LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG   - NULL handle, invalid params
 *   LYGUS_ERR_KEY_TOO_LARGE - Key exceeds 64 KB
 *   LYGUS_ERR_VAL_TOO_LARGE - Value exceeds limit
 *   LYGUS_ERR_WAL_FULL      - Segment full, need rotate
 *   LYGUS_ERR_IO            - Write failed
 *   LYGUS_ERR_DISK_FULL     - No space left
 */
int wal_put(wal_t *w, uint64_t index, uint64_t term,
            const void *key, size_t klen,
            const void *val, size_t vlen);

/**
 * Append DELETE entry
 *
 * @param w      WAL handle
 * @param index  Raft log index
 * @param term   Raft term
 * @param key    Key bytes
 * @param klen   Key length
 * @return       LYGUS_OK on success, negative error code otherwise
 */
int wal_del(wal_t *w, uint64_t index, uint64_t term,
            const void *key, size_t klen);

/**
 * Append NOOP_SYNC entry (for ALR)
 *
 * @param w      WAL handle
 * @param index  Raft log index
 * @param term   Raft term
 * @return       LYGUS_OK on success, negative error code otherwise
 */
int wal_noop_sync(wal_t *w, uint64_t index, uint64_t term);

/**
 * Append SNAP_MARK entry (snapshot marker)
 *
 * @param w      WAL handle
 * @param index  Raft log index (snapshot index)
 * @param term   Raft term
 * @return       LYGUS_OK on success, negative error code otherwise
 */
int wal_snap_mark(wal_t *w, uint64_t index, uint64_t term);

// ============================================================================
// Durability Control
// ============================================================================

/**
 * Flush current block to disk
 *
 * Forces current block buffer to be written. Does NOT fsync unless
 * sync parameter is true.
 *
 * @param w     WAL handle
 * @param sync  If true, also call fdatasync()
 * @return      LYGUS_OK on success, negative error code otherwise
 */
int wal_flush(wal_t *w, int sync);

/**
 * Force fsync (durability checkpoint)
 *
 * All data written before this call is guaranteed durable after success.
 * This is the only way to ensure crash safety.
 *
 * @param w  WAL handle
 * @return   LYGUS_OK on success, negative error code otherwise
 */
int wal_fsync(wal_t *w);

// ============================================================================
// Segment Management
// ============================================================================

/**
 * Rotate to new segment
 *
 * Call after creating a snapshot to start fresh WAL.
 * Also call if current segment is getting too large.
 *
 * @param w  WAL handle
 * @return   LYGUS_OK on success, negative error code otherwise
 */
int wal_rotate(wal_t *w);

/**
 * Check if segment should be rotated
 *
 * Returns true if segment is approaching max size.
 *
 * @param w  WAL handle
 * @return   1 if rotation recommended, 0 otherwise
 */
int wal_should_rotate(const wal_t *w);

// ============================================================================
// Log Truncation
// ============================================================================

/**
 * Truncate log after index (remove newer entries)
 *
 * Used by Raft when leader overwrites conflicting entries on follower.
 *
 * Keeps entries up to and including `index`, removes everything after.
 * Physically truncates the segment file to reclaim space.
 *
 * IMPORTANT: This fsyncs before returning to ensure durability.
 *
 * @param w      WAL handle
 * @param index  Last index to keep (entries > index are removed)
 * @return       LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG   - NULL handle
 *   LYGUS_ERR_KEY_NOT_FOUND - Index not in log
 *   LYGUS_ERR_IO            - Truncate failed
 *   LYGUS_ERR_FSYNC         - Fsync after truncate failed
 */
int wal_truncate_after(wal_t *w, uint64_t index);

/**
 * Purge log entries before index (remove older entries)
 *
 * Used after snapshotting to reclaim disk space.
 * Deletes entire segments that contain only entries < index.
 * Does NOT modify segments that contain entries >= index.
 *
 * @param w      WAL handle
 * @param index  First index to keep (segments fully before this are deleted)
 * @return       LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL handle
 *   LYGUS_ERR_IO           - Delete failed
 */
int wal_purge_before(wal_t *w, uint64_t index);

/**
 * Clear all log entries
 *
 * Deletes all segments and resets to empty state.
 * Used when loading a snapshot that replaces the entire log.
 * @param w  WAL handle
 * @return   LYGUS_OK on success, negative error code otherwise
 */
int wal_clear(wal_t *w);

// ============================================================================
// Index Queries
// ============================================================================

/**
 * Get first index in log
 *
 * @param w  WAL handle
 * @return   First index, or 0 if empty
 */
uint64_t wal_first_index(const wal_t *w);

/**
 * Get last index in log
 *
 * @param w  WAL handle
 * @return   Last index, or 0 if empty
 */
uint64_t wal_last_index(const wal_t *w);

/**
 * Check if log contains an index
 *
 * @param w      WAL handle
 * @param index  Index to check
 * @return       1 if present, 0 if not
 */
int wal_contains_index(const wal_t *w, uint64_t index);

/**
 * Read a single entry from WAL by index.
 *
 * @param w        WAL handle
 * @param index    Raft log index to read
 * @param entry    Decoded entry metadata/output
 * @param buf      Scratch buffer for decompressed block data
 * @param buf_cap  Size of scratch buffer
 *
 * @return LYGUS_OK on success, error code otherwise
 */
int wal_read_entry(wal_t *w, uint64_t index, wal_entry_t *entry,
                   uint8_t *buf, size_t buf_cap);
// ============================================================================
// Observability
// ============================================================================

/**
 * Get WAL statistics
 *
 * @param w     WAL handle
 * @param stats Output statistics structure
 * @return      LYGUS_OK on success, negative error code otherwise
 */
int wal_get_stats(const wal_t *w, wal_stats_t *stats);

/**
 * Get recovery result from last open
 *
 * @param w      WAL handle
 * @param result Output recovery result structure
 * @return       LYGUS_OK on success, negative error code otherwise
 */
int wal_get_recovery_result(const wal_t *w, wal_recovery_result_t *result);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_WAL_H