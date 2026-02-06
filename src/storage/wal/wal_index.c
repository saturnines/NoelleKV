#include "wal_index.h"
#include <stdlib.h>
#include <string.h>

// ============================================================================
// Internal Structure
// ============================================================================

#define INITIAL_CAPACITY 1024

struct wal_index {
    wal_entry_loc_t *entries;    // Dynamic array of locations
    size_t           capacity;   // Allocated slots
    size_t           count;      // Used slots
    uint64_t         first_index; // Raft index of entries[0]
};

// ============================================================================
// Lifecycle
// ============================================================================

wal_index_t* wal_index_create(void) {
    wal_index_t *idx = calloc(1, sizeof(wal_index_t));
    if (!idx) return NULL;

    idx->entries = malloc(INITIAL_CAPACITY * sizeof(wal_entry_loc_t));
    if (!idx->entries) {
        free(idx);
        return NULL;
    }

    idx->capacity = INITIAL_CAPACITY;
    idx->count = 0;
    idx->first_index = 0;

    return idx;
}

void wal_index_destroy(wal_index_t *idx) {
    if (!idx) return;
    free(idx->entries);
    free(idx);
}

void wal_index_clear(wal_index_t *idx) {
    if (!idx) return;
    idx->count = 0;
    idx->first_index = 0;
}

// ============================================================================
// Internal Helpers
// ============================================================================

static int ensure_capacity(wal_index_t *idx, size_t needed) {
    if (needed <= idx->capacity) return LYGUS_OK;

    size_t new_cap = idx->capacity * 2;
    while (new_cap < needed) {
        new_cap *= 2;
    }

    wal_entry_loc_t *new_entries = realloc(idx->entries,
                                            new_cap * sizeof(wal_entry_loc_t));
    if (!new_entries) return LYGUS_ERR_NOMEM;

    idx->entries = new_entries;
    idx->capacity = new_cap;
    return LYGUS_OK;
}

// ============================================================================
// Operations
// ============================================================================

int wal_index_append(wal_index_t *idx,
                     uint64_t raft_index,
                     uint64_t segment_num,
                     uint64_t block_offset,
                     uint64_t entry_offset)
{
    if (!idx) return LYGUS_ERR_INVALID_ARG;

    // First entry sets the base index
    if (idx->count == 0) {
        idx->first_index = raft_index;
    } else {
        // Must be sequential
        uint64_t expected = idx->first_index + idx->count;
        if (raft_index != expected) {
            return LYGUS_ERR_INVALID_ARG;
        }
    }

    // Grow if needed
    int ret = ensure_capacity(idx, idx->count + 1);
    if (ret < 0) return ret;

    // Add entry
    idx->entries[idx->count].segment_num = segment_num;
    idx->entries[idx->count].block_offset = block_offset;
    idx->entries[idx->count].entry_offset = entry_offset;
    idx->count++;

    return LYGUS_OK;
}

int wal_index_lookup(const wal_index_t *idx,
                     uint64_t raft_index,
                     wal_entry_loc_t *out_loc)
{
    if (!idx || !out_loc) return LYGUS_ERR_INVALID_ARG;

    if (idx->count == 0) return LYGUS_ERR_KEY_NOT_FOUND;

    // Check bounds
    if (raft_index < idx->first_index) return LYGUS_ERR_KEY_NOT_FOUND;

    uint64_t offset = raft_index - idx->first_index;
    if (offset >= idx->count) return LYGUS_ERR_KEY_NOT_FOUND;

    *out_loc = idx->entries[offset];
    return LYGUS_OK;
}

int wal_index_truncate_after(wal_index_t *idx, uint64_t raft_index) {
    if (!idx) return LYGUS_ERR_INVALID_ARG;

    if (idx->count == 0) return LYGUS_OK;

    // If truncating before first entry, clear everything
    if (raft_index < idx->first_index) {
        wal_index_clear(idx);
        return LYGUS_OK;
    }

    // Calculate new count (keep entries up to and including raft_index)
    uint64_t keep = raft_index - idx->first_index + 1;

    if (keep < idx->count) {
        idx->count = keep;
    }

    return LYGUS_OK;
}

int wal_index_truncate_before(wal_index_t *idx, uint64_t raft_index) {
    if (!idx) return LYGUS_ERR_INVALID_ARG;

    if (idx->count == 0) return LYGUS_OK;

    // If raft_index is at or before first, nothing to do
    if (raft_index <= idx->first_index) return LYGUS_OK;

    // If raft_index is past the end, clear everything
    uint64_t last = idx->first_index + idx->count - 1;
    if (raft_index > last) {
        wal_index_clear(idx);
        return LYGUS_OK;
    }

    // Shift entries left
    uint64_t remove_count = raft_index - idx->first_index;
    size_t new_count = idx->count - remove_count;

    memmove(idx->entries,
            idx->entries + remove_count,
            new_count * sizeof(wal_entry_loc_t));

    idx->first_index = raft_index;
    idx->count = new_count;

    return LYGUS_OK;
}

// ============================================================================
// Queries
// ============================================================================

uint64_t wal_index_first(const wal_index_t *idx) {
    if (!idx || idx->count == 0) return 0;
    return idx->first_index;
}

uint64_t wal_index_last(const wal_index_t *idx) {
    if (!idx || idx->count == 0) return 0;
    return idx->first_index + idx->count - 1;
}

size_t wal_index_count(const wal_index_t *idx) {
    return idx ? idx->count : 0;
}

int wal_index_is_empty(const wal_index_t *idx) {
    return (!idx || idx->count == 0);
}

uint64_t wal_index_min_segment_from(const wal_index_t *idx, uint64_t raft_index) {
    if (!idx || idx->count == 0) return 0;

    // If asking for index before our range, return first segment
    if (raft_index <= idx->first_index) {
        return idx->entries[0].segment_num;
    }

    // If asking for index past our range, nothing found
    uint64_t last = idx->first_index + idx->count - 1;
    if (raft_index > last) return 0;

    // Look up the segment for this index
    uint64_t offset = raft_index - idx->first_index;
    return idx->entries[offset].segment_num;
}