#include "recovery.h"
#include "platform/platform.h"
#include "../../util/logging.h"
#include "../../util/timing.h"
#include "../compression/zstd_engine.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

// ============================================================================
// Scanner State
// ============================================================================

struct wal_scanner {
    lygus_fd_t            fd;          // File descriptor
    uint64_t              offset;      // Current read position
    lygus_zstd_ctx_t     *zctx;        // Decompression context (borrowed)
    char                  path[512];   // File path (for truncation)
};

// ============================================================================
// Helpers
// ============================================================================

/**
 * Compare function for qsort (segment numbers)
 */
static int compare_uint64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

// ============================================================================
// Public API - Segment Listing
// ============================================================================

int wal_list_segments(const char *data_dir,
                      uint64_t **out_segs,
                      size_t *out_count)
{
    if (!data_dir || !out_segs || !out_count) {
        return LYGUS_ERR_INVALID_ARG;
    }

    lygus_dir_t *dir = lygus_dir_open(data_dir);
    if (!dir) {
        return LYGUS_ERR_IO;
    }

    // First pass: count segments
    size_t count = 0;
    const char *name;
    while ((name = lygus_dir_read(dir)) != NULL) {
        uint64_t seg_num;
        if (sscanf(name, "WAL-%" SCNu64 ".log", &seg_num) == 1) {
            count++;
        }
    }

    if (count == 0) {
        lygus_dir_close(dir);
        *out_segs = NULL;
        *out_count = 0;
        return LYGUS_OK;
    }

    // Allocate array
    uint64_t *segs = malloc(count * sizeof(uint64_t));
    if (!segs) {
        lygus_dir_close(dir);
        return LYGUS_ERR_NOMEM;
    }

    // Reopen dir to restart iteration
    lygus_dir_close(dir);
    dir = lygus_dir_open(data_dir);
    if (!dir) {
        free(segs);
        return LYGUS_ERR_IO;
    }

    // Second pass: collect segment numbers
    size_t idx = 0;
    while ((name = lygus_dir_read(dir)) != NULL && idx < count) {
        uint64_t seg_num;
        if (sscanf(name, "WAL-%" SCNu64 ".log", &seg_num) == 1) {
            segs[idx++] = seg_num;
        }
    }
    lygus_dir_close(dir);

    // Sort segments
    qsort(segs, count, sizeof(uint64_t), compare_uint64);

    *out_segs = segs;
    *out_count = count;
    return LYGUS_OK;
}

// ============================================================================
// Public API - Scanner
// ============================================================================

wal_scanner_t* wal_scanner_open(const char *path, void *zctx) {
    if (!path || !zctx) {
        return NULL; /// Callers check returen value not errno.
    }

    lygus_fd_t fd = lygus_file_open(path, LYGUS_O_RDWR, 0);
    if (fd == LYGUS_INVALID_FD) {
        return NULL;
    }

    wal_scanner_t *scanner = malloc(sizeof(wal_scanner_t));
    if (!scanner) {
        lygus_file_close(fd);
        return NULL;
    }

    scanner->fd = fd;
    scanner->offset = 0;
    scanner->zctx = (lygus_zstd_ctx_t *)zctx;
    strncpy(scanner->path, path, sizeof(scanner->path) - 1);
    scanner->path[sizeof(scanner->path) - 1] = '\0';

    return scanner;
}

ssize_t wal_scanner_next_block(wal_scanner_t *scanner,
                                wal_block_hdr_t *hdr,
                                uint8_t *raw_buf, size_t raw_cap)
{
    if (!scanner || !hdr || !raw_buf) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (raw_cap < WAL_BLOCK_SIZE) {
        return LYGUS_ERR_INCOMPLETE;
    }

    // CRITICAL: Save offset before reading anything
    uint64_t block_start_offset = scanner->offset;

    // Read block header
    int64_t n = lygus_file_read(scanner->fd, hdr, sizeof(*hdr));
    if (n == 0) {
        return 0;  // EOF
    }
    if (n != sizeof(*hdr)) {
        // Partial header at EOF - DON'T update offset, truncate at block start
        return LYGUS_ERR_TRUNCATED;
    }

    scanner->offset += sizeof(*hdr);

    int ret = wal_block_validate_header(hdr);
    if (ret < 0) {
        // Bad header - will truncate at block_start_offset
        return ret;
    }

    // Allocate buffer for compressed data
    uint8_t comp_buf[WAL_BLOCK_SIZE + 1024];
    size_t comp_len = hdr->comp_len;

    if (comp_len > sizeof(comp_buf)) {
        return LYGUS_ERR_BAD_BLOCK;
    }

    // Read compressed payload
    n = lygus_file_read(scanner->fd, comp_buf, comp_len);
    if (n != (int64_t)comp_len) {
        // Partial block at EOF, will truncate at block_start_offset
        return LYGUS_ERR_TRUNCATED;
    }

    scanner->offset += comp_len;

    // Decompress block
    ssize_t decompressed = wal_block_decompress(hdr, comp_buf,
                                                 scanner->zctx,
                                                 raw_buf, raw_cap);
    if (decompressed < 0) {
        // Corruption still will truncate at block_start_offset
        return decompressed;
    }

    return decompressed;
}

uint64_t wal_scanner_offset(const wal_scanner_t *scanner) {
    return scanner ? scanner->offset : 0;
}

void wal_scanner_close(wal_scanner_t *scanner) {
    if (!scanner) return;

    if (scanner->fd != LYGUS_INVALID_FD) {
        lygus_file_close(scanner->fd);
    }
    free(scanner);
}

// ============================================================================
// Public API - Entry Iterator
// ============================================================================

int wal_block_next_entry(const uint8_t *buf, size_t buf_len,
                         size_t *offset,
                         wal_entry_t *entry)
{
    if (!buf || !offset || !entry) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Check if we're at the end
    if (*offset >= buf_len) {
        return LYGUS_ERR_INCOMPLETE;
    }

    // Try to decode entry
    ssize_t consumed = wal_entry_decode(&buf[*offset], buf_len - *offset, entry);
    if (consumed < 0) {
        return (int)consumed;
    }

    *offset += consumed;
    return LYGUS_OK;
}

int wal_scanner_truncate_at(wal_scanner_t *scanner, uint64_t offset) {
    if (!scanner) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Seek to the truncation point
    if (lygus_file_seek(scanner->fd, (int64_t)offset, LYGUS_SEEK_SET) < 0) {
        return LYGUS_ERR_IO;
    }

    // Truncate at this offset
    if (lygus_file_truncate(scanner->fd, offset) < 0) {
        return LYGUS_ERR_IO;
    }

    // Fsync
    if (lygus_file_sync(scanner->fd) < 0) {
        return LYGUS_ERR_FSYNC;
    }

    scanner->offset = offset;

    LOG_WARN(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION, 0, 0, NULL, 0);

    return LYGUS_OK;
}

// ============================================================================
// Public API - Recovery
// ============================================================================

int wal_recover(const char *data_dir,
                void *zctx,
                wal_entry_callback_t callback,
                void *user_data,
                wal_recovery_result_t *result) {
    if (!data_dir || !zctx || !result) {
        return LYGUS_ERR_INVALID_ARG;
    }

    uint64_t start_ns = lygus_monotonic_ns();

    // Initialize result
    memset(result, 0, sizeof(*result));

    // List all segments
    uint64_t *segments = NULL;
    size_t num_segments = 0;
    int ret = wal_list_segments(data_dir, &segments, &num_segments);
    if (ret < 0) {
        return ret;
    }

    if (num_segments == 0) {
        // No WAL segments, fresh start
        return LYGUS_OK;
    }

    result->num_segments = num_segments;

    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY, 0, 0);

    // Scan each segment in order
    for (size_t i = 0; i < num_segments; i++) {
        uint64_t seg_num = segments[i];

        // Build segment path
        char seg_filename[64];
        snprintf(seg_filename, sizeof(seg_filename), "WAL-%06" PRIu64 ".log", seg_num);

        char seg_path[512];
        lygus_path_join(seg_path, sizeof(seg_path), data_dir, seg_filename);

        // Open scanner
        wal_scanner_t *scanner = wal_scanner_open(seg_path, zctx);

        if (!scanner) {
            free(segments);
            return LYGUS_ERR_IO;
        }

        // Allocate block buffer
        uint8_t block_buf[WAL_BLOCK_SIZE];
        wal_block_hdr_t hdr;

        // Read blocks from this segment
        int corrupted = 0;

        while (1) {
            uint64_t block_start = wal_scanner_offset(scanner);  // Before reading

            ssize_t decompressed = wal_scanner_next_block(scanner, &hdr,
                                                          block_buf, sizeof(block_buf));

            if (decompressed == 0) {
                // EOF normal termination
                break;
            }

            if (decompressed < 0) {
                result->corruptions++;
                wal_scanner_truncate_at(scanner, block_start);
                result->truncated = 1;
                corrupted = 1;
                break;
            }

            // Block read successfully
            result->num_blocks++;
            result->bytes_scanned += sizeof(hdr) + hdr.comp_len;

            // Iterate entries in this block
            size_t entry_offset = 0;
            while (entry_offset < (size_t)decompressed) {
                // Save entry offset BEFORE decoding (this is the entry's position)
                size_t this_entry_offset = entry_offset;

                wal_entry_t entry;
                ret = wal_block_next_entry(block_buf, decompressed,
                                          &entry_offset, &entry);

                if (ret == LYGUS_ERR_INCOMPLETE) {
                    break;
                }

                if (ret < 0) {
                    result->corruptions++;
                    wal_scanner_truncate_at(scanner, block_start);
                    result->truncated = 1;
                    corrupted = 1;
                    break;
                }

                result->num_entries++;

                // Track highest index/term
                if (entry.index > result->highest_index) {
                    result->highest_index = entry.index;
                }
                if (entry.term > result->highest_term) {
                    result->highest_term = entry.term;
                }

                // Populate location info before callback
                entry.segment_num = seg_num;
                entry.block_offset = block_start;
                entry.entry_offset = this_entry_offset;

                if (callback) {
                    ret = callback(&entry, user_data);
                    if (ret < 0) {
                        wal_scanner_close(scanner);
                        free(segments);
                        return ret;
                    }
                }
            }

            if (corrupted) {
                break;
            }

            // Check if this was the last block in segment
            if (hdr.flags & WAL_FLAG_LAST_BLOCK) {
                break;
            }
        }

        wal_scanner_close(scanner);

        // If we hit corruption, DELETE subsequent segments and STOP
        if (corrupted) {
            LOG_WARN(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION,
                     0, seg_num, NULL, 0);

            // Delete all segments after the corrupted one
            for (size_t j = i + 1; j < num_segments; j++) {
                char dead_filename[64];
                snprintf(dead_filename, sizeof(dead_filename), "WAL-%06" PRIu64 ".log", segments[j]);

                char dead_path[512];
                lygus_path_join(dead_path, sizeof(dead_path), data_dir, dead_filename);

                if (lygus_unlink(dead_path) == 0) {
                    LOG_INFO_SIMPLE(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY,
                                   0, segments[j]);
                } else if (lygus_path_exists(dead_path)) {
                    // File exists but couldn't delete
                    LOG_ERROR(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_CORRUPTION,
                             0, segments[j], NULL, 0);
                    free(segments);
                    return LYGUS_ERR_IO;
                }
            }

            break;
        }
    }

    free(segments);

    uint64_t end_ns = lygus_monotonic_ns();

    // Log recovery stats with timing
    struct {
        uint64_t highest_index;
        uint64_t num_entries;
        uint64_t num_blocks;
        uint32_t recovery_time_ms;
    } log_data = {
        .highest_index = result->highest_index,
        .num_entries = result->num_entries,
        .num_blocks = result->num_blocks,
        .recovery_time_ms = (uint32_t)((end_ns - start_ns) / 1000000),
    };
    LOG_INFO(LYGUS_MODULE_WAL, LYGUS_EVENT_WAL_RECOVERY,
             result->highest_term, result->highest_index,
             &log_data, sizeof(log_data));

    return LYGUS_OK;
}