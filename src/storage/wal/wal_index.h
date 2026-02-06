#ifndef LYGUS_WAL_INDEX_H
#define LYGUS_WAL_INDEX_H

#include <stdint.h>
#include <stddef.h>

#include "../../public/lygus_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// WAL Index - Maps Raft index to file location
// ============================================================================

/**
 * Entry location in WAL
 */
typedef struct {
    uint64_t segment_num;    // Which segment file
    uint64_t block_offset;   // Byte offset of block start in segment
    uint64_t entry_offset;   // Byte offset of entry within decompressed block
} wal_entry_loc_t;

/**
 * In-memory index handle
 *
 * Simple dynamic array indexed by (raft_index - first_index).
 * Supports O(1) lookup, append, and truncation.
 */
typedef struct wal_index wal_index_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create empty index
 *
 * @return Index handle, or NULL on OOM
 */
wal_index_t* wal_index_create(void);

/**
 * Destroy index
 */
void wal_index_destroy(wal_index_t *idx);

/**
 * Clear all entries (reset to empty)
 */
void wal_index_clear(wal_index_t *idx);

// ============================================================================
// Operations
// ============================================================================

/**
 * Add entry to index
 *
 * Entries must be added in order. If index != last_index + 1,
 * returns error (unless index is empty, then any starting index is fine).
 *
 * @param idx          Index handle
 * @param raft_index   Raft log index
 * @param segment_num  Segment number containing entry
 * @param block_offset Byte offset of block in segment
 * @param entry_offset Byte offset of entry in decompressed block
 * @return             LYGUS_OK or error
 */
int wal_index_append(wal_index_t *idx,
                     uint64_t raft_index,
                     uint64_t segment_num,
                     uint64_t block_offset,
                     uint64_t entry_offset);

/**
 * Lookup entry location
 *
 * @param idx         Index handle
 * @param raft_index  Raft log index to find
 * @param out_loc     Output: entry location
 * @return            LYGUS_OK if found, LYGUS_ERR_KEY_NOT_FOUND if not
 */
int wal_index_lookup(const wal_index_t *idx,
                     uint64_t raft_index,
                     wal_entry_loc_t *out_loc);

/**
 * Truncate index after given raft index (keep entries <= index)
 *
 * @param idx         Index handle
 * @param raft_index  Last index to keep (entries after this are removed)
 * @return            LYGUS_OK or error
 */
int wal_index_truncate_after(wal_index_t *idx, uint64_t raft_index);

/**
 * Remove entries before given raft index (keep entries >= index)
 *
 * @param idx         Index handle
 * @param raft_index  First index to keep (entries before this are removed)
 * @return            LYGUS_OK or error
 */
int wal_index_truncate_before(wal_index_t *idx, uint64_t raft_index);

// ============================================================================
// Queries
// ============================================================================

/**
 * Get first raft index in the index
 * @return First index, or 0 if empty
 */
uint64_t wal_index_first(const wal_index_t *idx);

/**
 * Get last raft index in the index
 * @return Last index, or 0 if empty
 */
uint64_t wal_index_last(const wal_index_t *idx);

/**
 * Get number of entries
 */
size_t wal_index_count(const wal_index_t *idx);

/**
 * Check if index is empty
 */
int wal_index_is_empty(const wal_index_t *idx);

/**
 * Find the minimum segment number that contains entries >= raft_index
 * Used for purge_before to know which segments can be deleted.
 *
 * @param idx         Index handle
 * @param raft_index  Threshold index
 * @return            Segment number, or 0 if not found
 */
uint64_t wal_index_min_segment_from(const wal_index_t *idx, uint64_t raft_index);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_WAL_INDEX_H