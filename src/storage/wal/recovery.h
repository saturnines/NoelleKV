#ifndef LYGUS_WAL_RECOVERY_H
#define LYGUS_WAL_RECOVERY_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#include "block_format.h"
#include "../../public/lygus_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Recovery Result
// ============================================================================

/**
 * Recovery state returned after scanning WAL
 */
typedef struct {
    uint64_t highest_index;      // Highest Raft index seen
    uint64_t highest_term;       // Highest Raft term seen
    uint64_t num_segments;       // Number of segments scanned
    uint64_t num_blocks;         // Number of blocks read
    uint64_t num_entries;        // Number of entries decoded
    uint64_t bytes_scanned;      // Total bytes scanned
    uint64_t corruptions;        // Number of corruptions detected
    int      truncated;          // 1 if truncation occurred
} wal_recovery_result_t;

// ============================================================================
// Entry Callback
// ============================================================================

/**
 * Callback invoked for each entry during recovery
 *
 * The entry pointers are valid only during the callback.
 * If you need to keep the data, copy it.
 *
 * @param entry      Decoded entry
 * @param user_data  User-provided context
 * @return           LYGUS_OK to continue, negative error to abort
 */
typedef int (*wal_entry_callback_t)(const wal_entry_t *entry, void *user_data);

// ============================================================================
// Recovery API
// ============================================================================

/**
 * Recover WAL from directory
 *
 * Scans all WAL-*.log segments in order, validates blocks, decodes entries,
 * and invokes callback for each entry. On corruption, truncates the segment
 * at the first bad block boundary and stops.
 *
 * @param data_dir   Directory containing WAL-*.log files
 * @param zctx       Zstd decompression context
 * @param callback   Function to call for each entry (can be NULL)
 * @param user_data  Context passed to callback
 * @param result     Output: recovery statistics
 * @return           LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG    - NULL data_dir or result
 *   LYGUS_ERR_IO             - Failed to read segment
 *   LYGUS_ERR_CORRUPT        - Corruption detected (after truncation)
 *   LYGUS_ERR_WAL_RECOVERY   - Recovery failed
 *
 * Note: Corruption triggers automatic truncation and returns LYGUS_OK
 *       unless truncation itself fails.
 */
int wal_recover(const char *data_dir,
                void *zctx,
                wal_entry_callback_t callback,
                void *user_data,
                wal_recovery_result_t *result);

// ============================================================================
// Segment Scanner (lower-level API)
// ============================================================================

typedef struct wal_scanner wal_scanner_t;

/**
 * Open segment for scanning
 *
 * @param path  Path to WAL segment file
 * @param zctx  Zstd decompression context
 * @return      Scanner handle, or NULL on error
 */
wal_scanner_t* wal_scanner_open(const char *path, void *zctx);

/**
 * Read next block from segment
 *
 * Reads block header, validates magic/version, decompresses payload.
 *
 * @param scanner  Scanner handle
 * @param hdr      Output: block header
 * @param raw_buf  Output buffer for decompressed data (must be >= WAL_BLOCK_SIZE)
 * @param raw_cap  Output buffer capacity
 * @return         Bytes decompressed, 0 on EOF, negative on error
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL pointers
 *   LYGUS_ERR_IO           - Read failed
 *   LYGUS_ERR_BAD_BLOCK    - Bad header
 *   LYGUS_ERR_CORRUPT      - CRC mismatch
 *   LYGUS_ERR_DECOMPRESS   - Decompression failed
 *   LYGUS_ERR_TRUNCATED    - Partial block at EOF
 */
ssize_t wal_scanner_next_block(wal_scanner_t *scanner,
                                wal_block_hdr_t *hdr,
                                uint8_t *raw_buf, size_t raw_cap);

/**
 * Get current file offset
 *
 * @param scanner  Scanner handle
 * @return         Byte offset in file
 */
uint64_t wal_scanner_offset(const wal_scanner_t *scanner);

/**
 * Close scanner
 *
 * @param scanner  Scanner handle
 */
void wal_scanner_close(wal_scanner_t *scanner);

// ============================================================================
// Entry Iterator (for iterating entries within a block)
// ============================================================================

/**
 * Iterate entries in a raw block buffer
 *
 * Call repeatedly to decode all entries in a block.
 *
 * @param buf       Raw block data (decompressed)
 * @param buf_len   Buffer length
 * @param offset    Input/output: current offset in buffer (start at 0)
 * @param entry     Output: decoded entry
 * @return          LYGUS_OK if entry decoded, LYGUS_ERR_INCOMPLETE if done,
 *                  negative error code on corruption
 */
int wal_block_next_entry(const uint8_t *buf, size_t buf_len,
                         size_t *offset,
                         wal_entry_t *entry);

// ============================================================================
// Segment Listing
// ============================================================================

/**
 * List all WAL segments in directory (sorted by number)
 *
 * Allocates array of segment numbers. Caller must free().
 *
 * @param data_dir   Directory to scan
 * @param out_segs   Output: allocated array of segment numbers (sorted)
 * @param out_count  Output: number of segments
 * @return           LYGUS_OK on success, negative error code otherwise
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL pointers
 *   LYGUS_ERR_NOMEM        - Allocation failed
 *   LYGUS_ERR_IO           - Failed to read directory
 */
int wal_list_segments(const char *data_dir,
                      uint64_t **out_segs,
                      size_t *out_count);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_WAL_RECOVERY_H