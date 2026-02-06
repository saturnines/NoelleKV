#ifndef LYGUS_BLOCK_FORMAT_H
#define LYGUS_BLOCK_FORMAT_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#include "../../public/lygus_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Constants
// ============================================================================

#define WAL_BLOCK_MAGIC   0x50444342U  // 'PDCB'
#define WAL_BLOCK_VERSION 1
#define WAL_BLOCK_SIZE    65536        // 64 KiB

// Block flags
#define WAL_FLAG_LAST_BLOCK   0x0001   // Last block in segment
#define WAL_FLAG_UNCOMPRESSED 0x0002   // Block not compressed (data bigger than input)

// Entry size limits
#define WAL_ENTRY_MAX_KEY_SIZE   (64 * 1024)      // 64 KB
#define WAL_ENTRY_MAX_VALUE_SIZE (1024 * 1024)    // 1 MB

// ============================================================================
// Entry Types
// ============================================================================

typedef enum {
    WAL_ENTRY_PUT       = 1,  // Key-value write
    WAL_ENTRY_DEL       = 2,  // Key deletion
    WAL_ENTRY_NOOP_SYNC = 3,  // ALR sync marker (no key/value)
    WAL_ENTRY_SNAP_MARK = 4,  // Snapshot marker
} wal_entry_type_t;

// ============================================================================
// Entry Structure (decoded form)
// ============================================================================

typedef struct {
    wal_entry_type_t type;
    uint64_t index;
    uint64_t term;

    const uint8_t *key;   // Points into source buffer (don't free!)
    size_t klen;

    const uint8_t *val;   // Points into source buffer (don't free!)
    size_t vlen;

    uint32_t crc;         // Stored CRC (for debugging)

    // Location info (populated during recovery for index rebuilding)
    uint64_t segment_num;   // Which segment file (0 if not set)
    uint64_t block_offset;  // Byte offset of block start in segment
    uint64_t entry_offset;  // Byte offset of entry within decompressed block
} wal_entry_t;

// ============================================================================
// Block Header (on-disk format)
// ============================================================================

typedef struct {
    uint32_t magic;      // 0x50444342 'PDCB'
    uint16_t version;    // 1
    uint16_t flags;      // WAL_FLAG_*
    uint64_t seq_no;     // Block sequence number (monotonic)
    uint32_t raw_len;    // Bytes before compression
    uint32_t comp_len;   // Bytes after compression (0 if uncompressed)
    uint32_t crc32c;     // CRC over compressed payload
    uint32_t hdr_crc32c; // CRC over header bytes 0-27 (excludes this field)
} __attribute__((packed)) wal_block_hdr_t;

_Static_assert(sizeof(wal_block_hdr_t) == 32, "Block header must be 32 bytes");

// Offset of hdr_crc32c field (bytes covered by header checksum)
#define WAL_BLOCK_HDR_CRC_OFFSET 28

// ============================================================================
// Entry API
// ============================================================================

/**
 * Calculate encoded size of an entry
 *
 * Wire format: [type u8][index varint][term varint][klen varint][vlen varint]
 *              [key bytes][value bytes][crc32c u32]
 *
 * @param type   Entry type (1-4)
 * @param index  Raft log index
 * @param term   Raft term
 * @param klen   Key length (0 for NOOP_SYNC)
 * @param vlen   Value length (0 for DEL/NOOP)
 * @return       Total bytes needed, or negative error code
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG    - Invalid type
 *   LYGUS_ERR_KEY_TOO_LARGE  - klen exceeds limit
 *   LYGUS_ERR_VAL_TOO_LARGE  - vlen exceeds limit
 */
ssize_t wal_entry_size(wal_entry_type_t type, uint64_t index, uint64_t term,
                       size_t klen, size_t vlen);

/**
 * Encode entry to buffer
 *
 * CRC covers: [type through value], does NOT include the CRC itself
 *
 * @param type     Entry type
 * @param index    Raft index
 * @param term     Raft term
 * @param key      Key bytes (NULL for NOOP_SYNC)
 * @param klen     Key length
 * @param val      Value bytes (NULL for DEL/NOOP)
 * @param vlen     Value length
 * @param out      Output buffer
 * @param out_len  Output buffer capacity
 * @return         Bytes written, or negative error code
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG    - NULL out, invalid type
 *   LYGUS_ERR_INCOMPLETE     - Buffer too small
 *   LYGUS_ERR_KEY_TOO_LARGE
 *   LYGUS_ERR_VAL_TOO_LARGE
 */
ssize_t wal_entry_encode(wal_entry_type_t type, uint64_t index, uint64_t term,
                         const void *key, size_t klen,
                         const void *val, size_t vlen,
                         uint8_t *out, size_t out_len);

/**
 * Decode entry from buffer
 *
 * The returned entry's key/val pointers reference the input buffer directly.
 * They remain valid as long as buf is not modified or freed.
 *
 * Note: Location fields (segment_num, block_offset, entry_offset) are NOT
 * populated by this function - caller must set them if needed.
 *
 * @param buf      Input buffer
 * @param buf_len  Buffer length
 * @param entry    Output entry structure
 * @return         Bytes consumed, or negative error code
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL pointers
 *   LYGUS_ERR_INCOMPLETE   - Buffer too short
 *   LYGUS_ERR_CORRUPT      - CRC mismatch
 *   LYGUS_ERR_MALFORMED    - Invalid type, varint overflow, bad lengths
 *   LYGUS_ERR_OVERFLOW     - Varint overflow
 */
ssize_t wal_entry_decode(const uint8_t *buf, size_t buf_len, wal_entry_t *entry);

// ============================================================================
// Block API
// ============================================================================

/**
 * Compress block and prepare header
 *
 * Takes raw block data, compresses it with Zstd, and fills in the block header.
 * If compression makes data larger, stores uncompressed with WAL_FLAG_UNCOMPRESSED.
 *
 * @param raw_data   Raw block data (concatenated entries)
 * @param raw_len    Raw data length
 * @param seq_no     Block sequence number
 * @param zctx       Zstd compression context
 * @param out_hdr    Output: filled block header
 * @param out_data   Output: compressed data buffer (must be >= WAL_BLOCK_SIZE + 1024)
 * @param out_cap    Output buffer capacity
 * @return           Compressed size (or raw_len if uncompressed), or negative error
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL pointers
 *   LYGUS_ERR_INCOMPLETE   - Output buffer too small
 *   LYGUS_ERR_COMPRESS     - Compression failed
 */
ssize_t wal_block_compress(const uint8_t *raw_data, size_t raw_len,
                           uint64_t seq_no, void *zctx,
                           wal_block_hdr_t *out_hdr,
                           uint8_t *out_data, size_t out_cap);

/**
 * Decompress block
 *
 * Reads block header, validates magic/CRC, and decompresses payload.
 *
 * @param hdr       Block header
 * @param comp_data Compressed (or raw) block data
 * @param comp_len  Compressed data length
 * @param zctx      Zstd decompression context
 * @param out_raw   Output: decompressed data (must be >= WAL_BLOCK_SIZE)
 * @param out_cap   Output buffer capacity
 * @return          Decompressed size, or negative error
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL pointers
 *   LYGUS_ERR_BAD_BLOCK    - Bad magic, version, or header values
 *   LYGUS_ERR_CORRUPT      - CRC mismatch
 *   LYGUS_ERR_DECOMPRESS   - Decompression failed
 *   LYGUS_ERR_INCOMPLETE   - Output buffer too small
 */
    ssize_t wal_block_decompress(const wal_block_hdr_t *hdr,
                                 const uint8_t *comp_data,
                                 void *zctx,
                                 uint8_t *out_raw, size_t out_cap);

/**
 * Validate block header
 *
 * Checks magic number, version, and reasonable field values.
 * Does NOT verify CRC (done in wal_block_decompress).
 *
 * @param hdr  Block header to validate
 * @return     LYGUS_OK or negative error code
 *
 * Errors:
 *   LYGUS_ERR_INVALID_ARG  - NULL pointer
 *   LYGUS_ERR_BAD_BLOCK    - Bad magic/version/fields
 */
int wal_block_validate_header(const wal_block_hdr_t *hdr);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_BLOCK_FORMAT_H