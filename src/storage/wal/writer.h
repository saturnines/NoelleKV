#ifndef LYGUS_WAL_WRITER_H
#define LYGUS_WAL_WRITER_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#include "block_format.h"
#include "../../public/lygus_errors.h"
#include "platform/platform.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Constants
// ============================================================================

#define WAL_SEGMENT_MAX_SIZE  (128 * 1024 * 1024)  // 128 MiB per segment
#define WAL_SEGMENT_NAME_LEN  32                    // "WAL-NNNNNN.log"

// ============================================================================
// Writer State
// ============================================================================

typedef struct wal_writer wal_writer_t;

// ============================================================================
// Options
// ============================================================================

typedef struct {
    const char *data_dir;      // Directory for WAL segments
    int         zstd_level;    // Compression level (3-5 for online)
    size_t      block_size;    // Block size (0 = default 64 KiB)

    // Group commit / fsync tuning
    uint64_t    fsync_interval_us;  // Microseconds between forced fsyncs (0 = immediate)
    size_t      fsync_bytes;        // Bytes written before forced fsync (0 = every block)
} wal_writer_opts_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Open or create WAL writer
 *
 * If segments exist in data_dir, opens the latest one for appending.
 * Otherwise, creates WAL-000001.log.
 *
 * @param opts  Writer options
 * @return      Writer handle, or NULL on error (check errno)
 */
wal_writer_t* wal_writer_open(const wal_writer_opts_t *opts);

/**
 * Close writer and flush pending data
 *
 * @param w  Writer handle
 * @return   LYGUS_OK on success, negative error code otherwise
 */
int wal_writer_close(wal_writer_t *w);

// ============================================================================
// Write Operations
// ============================================================================

/**
 * Append entry to WAL
 *
 * Encodes entry into block buffer. If the entry doesn't fit, flushes
 * the current block first. Does NOT fsync unless fsync_interval triggers.
 *
 * @param w      Writer handle
 * @param type   Entry type (PUT, DEL, NOOP_SYNC, SNAP_MARK)
 * @param index  Raft log index
 * @param term   Raft term
 * @param key    Key bytes (NULL for NOOP_SYNC)
 * @param klen   Key length
 * @param val    Value bytes (NULL for DEL/NOOP)
 * @param vlen   Value length
 * @return       LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG   - NULL writer, invalid type
 *   LYGUS_ERR_IO            - Write failed
 *   LYGUS_ERR_DISK_FULL     - No space left
 *   LYGUS_ERR_WAL_FULL      - Segment full (need rotate)
 *   LYGUS_ERR_COMPRESS      - Compression failed
 *   LYGUS_ERR_KEY_TOO_LARGE
 *   LYGUS_ERR_VAL_TOO_LARGE
 */
int wal_writer_append(wal_writer_t *w,
                      wal_entry_type_t type,
                      uint64_t index, uint64_t term,
                      const void *key, size_t klen,
                      const void *val, size_t vlen);

/**
 * Flush current block to disk
 *
 * Forces current block buffer to be compressed and written, even if not full.
 * Does NOT fsync unless explicitly requested.
 *
 * @param w      Writer handle
 * @param sync   If true, also call fdatasync()
 * @return       LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL writer
 *   LYGUS_ERR_IO           - Write failed
 *   LYGUS_ERR_FSYNC        - fsync failed
 */
int wal_writer_flush(wal_writer_t *w, int sync);

/**
 * Fsync current segment
 *
 * Forces all written data to durable storage.
 * This is the durability checkpoint - data is NOT durable until this succeeds.
 *
 * @param w  Writer handle
 * @return   LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL writer
 *   LYGUS_ERR_FSYNC        - fdatasync() failed
 */
int wal_writer_fsync(wal_writer_t *w);

// ============================================================================
// Segment Management
// ============================================================================

/**
 * Rotate to new segment
 *
 * Flushes and fsyncs current segment, closes it, and opens a new one.
 * New segment number is current_segment + 1.
 *
 * Call this after:
 * - Creating a snapshot (to start fresh WAL)
 * - Current segment reaching max size
 *
 * @param w  Writer handle
 * @return   LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL writer
 *   LYGUS_ERR_IO           - Failed to create new segment
 *   LYGUS_ERR_FSYNC        - Failed to fsync old segment
 */
int wal_writer_rotate(wal_writer_t *w);

/**
 * Mark current block as last in segment
 *
 * Sets WAL_FLAG_LAST_BLOCK on next flush. Used before rotation or shutdown.
 *
 * @param w  Writer handle
 * @return   LYGUS_OK on success
 */
int wal_writer_mark_last_block(wal_writer_t *w);

// ============================================================================
// Observability
// ============================================================================

/**
 * Get current segment number
 *
 * @param w  Writer handle
 * @return   Segment number (1-based), or 0 if invalid
 */
uint64_t wal_writer_segment_num(const wal_writer_t *w);

/**
 * Get current write offset in segment
 *
 * @param w  Writer handle
 * @return   Byte offset (including headers + payloads)
 */
uint64_t wal_writer_offset(const wal_writer_t *w);

/**
 * Get current block sequence number
 *
 * Global monotonic counter across all segments.
 *
 * @param w  Writer handle
 * @return   Block sequence number
 */
uint64_t wal_writer_block_seq(const wal_writer_t *w);

/**
 * Get bytes in current block buffer
 *
 * @param w  Writer handle
 * @return   Bytes used in block buffer (0-65536)
 */
size_t wal_writer_block_fill(const wal_writer_t *w);

/**
 * Check if block buffer is empty
 *
 * @param w  Writer handle
 * @return   1 if empty, 0 otherwise
 */
int wal_writer_block_is_empty(const wal_writer_t *w);

/**
 * Check if all appended data has been fsynced
 *
 * @param w  Writer handle
 * @return   1 if everything is durable, 0 if there's unflushed data
 */
int wal_writer_is_durable(const wal_writer_t *w);


    /**
 * Get file descriptor for current segment
 *
 * @param w  Writer handle
 * @return   File descriptor, or LYGUS_INVALID_FD if invalid
 */
lygus_fd_t wal_writer_get_fd(const wal_writer_t *w);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_WAL_WRITER_H