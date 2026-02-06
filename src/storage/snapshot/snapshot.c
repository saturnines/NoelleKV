#include "snapshot.h"
#include "platform/platform.h"
#include "util/varint.h"
#include "util/crc32c.h"
#include "util/logging.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

// ============================================================================
// Internal Helpers
// ============================================================================

/**
 * Write all bytes to fd (handles partial writes)
 */
static int write_all(lygus_fd_t fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t remaining = len;

    while (remaining > 0) {
        int64_t n = lygus_file_write(fd, p, remaining);
        if (n < 0) {
            return -1;
        }
        if (n == 0) {
            return -1;  // Can't make progress
        }
        p += n;
        remaining -= (size_t)n;
    }

    return 0;
}

/**
 * Read all bytes from fd (handles partial reads)
 */
static int read_all(lygus_fd_t fd, void *buf, size_t len) {
    uint8_t *p = (uint8_t *)buf;
    size_t remaining = len;

    while (remaining > 0) {
        int64_t n = lygus_file_read(fd, p, remaining);
        if (n < 0) {
            return -1;
        }
        if (n == 0) {
            return -1;  // Unexpected EOF
        }
        p += n;
        remaining -= (size_t)n;
    }

    return 0;
}

// ============================================================================
// Snapshot Write
// ============================================================================

int snapshot_write(lygus_kv_t *kv,
                   uint64_t last_index,
                   uint64_t last_term,
                   const char *path)
{
    if (!kv || !path) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Build temp path
    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);

    // Create temp file
    lygus_fd_t fd = lygus_file_open(tmp_path,
                                    LYGUS_O_WRONLY | LYGUS_O_CREAT | LYGUS_O_TRUNC,
                                    0644);
    if (fd == LYGUS_INVALID_FD) {
        return LYGUS_ERR_OPEN_FILE;
    }

    int ret = LYGUS_OK;
    uint32_t crc = CRC32C_INIT;

    // Prepare header
    snapshot_hdr_t hdr = {
        .magic = SNAPSHOT_MAGIC,
        .version = SNAPSHOT_VERSION,
        .flags = 0,
        .last_index = last_index,
        .last_term = last_term,
        .entry_count = lygus_kv_count(kv),
    };

    // Write header
    if (write_all(fd, &hdr, sizeof(hdr)) < 0) {
        ret = LYGUS_ERR_WRITE;
        goto cleanup;
    }
    crc = crc32c_update(crc, (const uint8_t *)&hdr, sizeof(hdr));

    // Write entries
    lygus_kv_iter_t it;
    lygus_kv_iter_init(&it, kv);

    const void *key, *val;
    size_t klen, vlen;

    while (lygus_kv_iter_next(&it, &key, &klen, &val, &vlen)) {
        // Encode klen and vlen as varints
        uint8_t varint_buf[VARINT_MAX_LEN * 2];
        size_t pos = 0;

        ssize_t n = varint_encode(klen, &varint_buf[pos], sizeof(varint_buf) - pos);
        if (n < 0) {
            ret = LYGUS_ERR_INTERNAL;
            goto cleanup;
        }
        pos += (size_t)n;

        n = varint_encode(vlen, &varint_buf[pos], sizeof(varint_buf) - pos);
        if (n < 0) {
            ret = LYGUS_ERR_INTERNAL;
            goto cleanup;
        }
        pos += (size_t)n;

        // Write varints
        if (write_all(fd, varint_buf, pos) < 0) {
            ret = LYGUS_ERR_WRITE;
            goto cleanup;
        }
        crc = crc32c_update(crc, varint_buf, pos);

        // Write key
        if (klen > 0) {
            if (write_all(fd, key, klen) < 0) {
                ret = LYGUS_ERR_WRITE;
                goto cleanup;
            }
            crc = crc32c_update(crc, (const uint8_t *)key, klen);
        }

        // Write value
        if (vlen > 0) {
            if (write_all(fd, val, vlen) < 0) {
                ret = LYGUS_ERR_WRITE;
                goto cleanup;
            }
            crc = crc32c_update(crc, (const uint8_t *)val, vlen);
        }
    }

    // Finalize CRC
    crc ^= 0xFFFFFFFF;

    // Write CRC footer (little-endian)
    uint8_t crc_bytes[4] = {
        (crc >> 0) & 0xFF,
        (crc >> 8) & 0xFF,
        (crc >> 16) & 0xFF,
        (crc >> 24) & 0xFF,
    };

    if (write_all(fd, crc_bytes, 4) < 0) {
        ret = LYGUS_ERR_WRITE;
        goto cleanup;
    }

    // Fsync before rename
    if (lygus_file_sync(fd) < 0) {
        ret = LYGUS_ERR_FSYNC;
        goto cleanup;
    }

    lygus_file_close(fd);
    fd = LYGUS_INVALID_FD;

    // Atomic rename (platform layer handles Windows quirks)
    if (lygus_rename(tmp_path, path) < 0) {
        ret = LYGUS_ERR_IO;
        goto cleanup;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_SNAPSHOT, LYGUS_EVENT_SNAPSHOT_DONE,
                    last_term, last_index);

    return LYGUS_OK;

cleanup:
    if (fd != LYGUS_INVALID_FD) {
        lygus_file_close(fd);
    }
    lygus_unlink(tmp_path);
    return ret;
}

// ============================================================================
// Snapshot Load
// ============================================================================

int snapshot_load(const char *path,
                  lygus_kv_t *kv,
                  uint64_t *out_index,
                  uint64_t *out_term)
{
    if (!path || !kv) {
        return LYGUS_ERR_INVALID_ARG;
    }

    lygus_fd_t fd = lygus_file_open(path, LYGUS_O_RDONLY, 0);
    if (fd == LYGUS_INVALID_FD) {
        return LYGUS_ERR_OPEN_FILE;
    }

    int ret = LYGUS_OK;
    uint32_t crc = CRC32C_INIT;

    // Read header
    snapshot_hdr_t hdr;
    if (read_all(fd, &hdr, sizeof(hdr)) < 0) {
        ret = LYGUS_ERR_READ;
        goto cleanup;
    }
    crc = crc32c_update(crc, (const uint8_t *)&hdr, sizeof(hdr));

    // Validate header
    if (hdr.magic != SNAPSHOT_MAGIC) {
        ret = LYGUS_ERR_BAD_SNAPSHOT;
        goto cleanup;
    }
    if (hdr.version != SNAPSHOT_VERSION) {
        ret = LYGUS_ERR_BAD_SNAPSHOT;
        goto cleanup;
    }

    // Clear existing KV data
    lygus_kv_clear(kv);

    // Read entries
    for (uint64_t i = 0; i < hdr.entry_count; i++) {
        // Read varints for klen and vlen
        uint8_t varint_buf[VARINT_MAX_LEN];
        uint64_t klen = 0, vlen = 0;

        // Read klen varint
        size_t varint_pos = 0;
        while (varint_pos < VARINT_MAX_LEN) {
            if (read_all(fd, &varint_buf[varint_pos], 1) < 0) {
                ret = LYGUS_ERR_READ;
                goto cleanup;
            }
            varint_pos++;

            ssize_t decoded = varint_decode(varint_buf, varint_pos, &klen);
            if (decoded > 0) {
                crc = crc32c_update(crc, varint_buf, varint_pos);
                break;
            }
            if (decoded < 0 && decoded != LYGUS_ERR_BUFFER_TOO_SMALL) {
                ret = LYGUS_ERR_MALFORMED;
                goto cleanup;
            }
        }

        // Read vlen varint
        varint_pos = 0;
        while (varint_pos < VARINT_MAX_LEN) {
            if (read_all(fd, &varint_buf[varint_pos], 1) < 0) {
                ret = LYGUS_ERR_READ;
                goto cleanup;
            }
            varint_pos++;

            ssize_t decoded = varint_decode(varint_buf, varint_pos, &vlen);
            if (decoded > 0) {
                crc = crc32c_update(crc, varint_buf, varint_pos);
                break;
            }
            if (decoded < 0 && decoded != LYGUS_ERR_BUFFER_TOO_SMALL) {
                ret = LYGUS_ERR_MALFORMED;
                goto cleanup;
            }
        }

        // Allocate and read key
        uint8_t *key = NULL;
        if (klen > 0) {
            key = malloc(klen);
            if (!key) {
                ret = LYGUS_ERR_NOMEM;
                goto cleanup;
            }
            if (read_all(fd, key, klen) < 0) {
                free(key);
                ret = LYGUS_ERR_READ;
                goto cleanup;
            }
            crc = crc32c_update(crc, key, klen);
        }

        // Allocate and read value
        uint8_t *val = NULL;
        if (vlen > 0) {
            val = malloc(vlen);
            if (!val) {
                free(key);
                ret = LYGUS_ERR_NOMEM;
                goto cleanup;
            }
            if (read_all(fd, val, vlen) < 0) {
                free(key);
                free(val);
                ret = LYGUS_ERR_READ;
                goto cleanup;
            }
            crc = crc32c_update(crc, val, vlen);
        }

        // Insert into KV
        ret = lygus_kv_put(kv, key, klen, val, vlen);

        free(key);
        free(val);

        if (ret < 0) {
            goto cleanup;
        }
    }

    // Read and verify CRC
    uint8_t crc_bytes[4];
    if (read_all(fd, crc_bytes, 4) < 0) {
        ret = LYGUS_ERR_READ;
        goto cleanup;
    }

    uint32_t stored_crc = ((uint32_t)crc_bytes[0] << 0)
                        | ((uint32_t)crc_bytes[1] << 8)
                        | ((uint32_t)crc_bytes[2] << 16)
                        | ((uint32_t)crc_bytes[3] << 24);

    // Finalize computed CRC
    crc ^= 0xFFFFFFFF;

    if (stored_crc != crc) {
        lygus_kv_clear(kv);
        ret = LYGUS_ERR_CORRUPT;
        goto cleanup;
    }

    // Return metadata
    if (out_index) *out_index = hdr.last_index;
    if (out_term) *out_term = hdr.last_term;

    LOG_INFO_SIMPLE(LYGUS_MODULE_SNAPSHOT, LYGUS_EVENT_SNAPSHOT_LOAD,
                    hdr.last_term, hdr.last_index);

    lygus_file_close(fd);
    return LYGUS_OK;

cleanup:
    lygus_kv_clear(kv);
    lygus_file_close(fd);
    return ret;
}

// ============================================================================
// Snapshot Header Read
// ============================================================================

int snapshot_read_header(const char *path,
                         uint64_t *out_index,
                         uint64_t *out_term,
                         uint64_t *out_count)
{
    if (!path) {
        return LYGUS_ERR_INVALID_ARG;
    }

    lygus_fd_t fd = lygus_file_open(path, LYGUS_O_RDONLY, 0);
    if (fd == LYGUS_INVALID_FD) {
        return LYGUS_ERR_OPEN_FILE;
    }

    snapshot_hdr_t hdr;
    int ret = LYGUS_OK;

    if (read_all(fd, &hdr, sizeof(hdr)) < 0) {
        ret = LYGUS_ERR_READ;
        goto cleanup;
    }

    if (hdr.magic != SNAPSHOT_MAGIC) {
        ret = LYGUS_ERR_BAD_SNAPSHOT;
        goto cleanup;
    }

    if (hdr.version != SNAPSHOT_VERSION) {
        ret = LYGUS_ERR_BAD_SNAPSHOT;
        goto cleanup;
    }

    if (out_index) *out_index = hdr.last_index;
    if (out_term) *out_term = hdr.last_term;
    if (out_count) *out_count = hdr.entry_count;

cleanup:
    lygus_file_close(fd);
    return ret;
}

// ============================================================================
// Async Snapshot (unified implementation using platform layer)
// ============================================================================

/**
 * Context passed to the forked snapshot worker
 */
typedef struct {
    lygus_kv_t *kv;
    uint64_t    last_index;
    uint64_t    last_term;
    char        path[256];
} snapshot_worker_ctx_t;

/**
 * Worker function that runs in forked child (POSIX) or inline (Windows)
 */
static int snapshot_worker(void *ctx) {
    snapshot_worker_ctx_t *sctx = (snapshot_worker_ctx_t *)ctx;
    int ret = snapshot_write(sctx->kv, sctx->last_index, sctx->last_term, sctx->path);
    return (ret == LYGUS_OK) ? 0 : 1;
}

int snapshot_create_async(lygus_kv_t *kv,
                          uint64_t last_index,
                          uint64_t last_term,
                          const char *path,
                          snapshot_async_t *out_async)
{
    if (!kv || !path || !out_async) {
        return LYGUS_ERR_INVALID_ARG;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_SNAPSHOT, LYGUS_EVENT_SNAPSHOT_START,
                    last_term, last_index);

    // Prepare context for worker
    snapshot_worker_ctx_t *ctx = malloc(sizeof(snapshot_worker_ctx_t));
    if (!ctx) {
        return LYGUS_ERR_NOMEM;
    }

    ctx->kv = kv;
    ctx->last_index = last_index;
    ctx->last_term = last_term;
    strncpy(ctx->path, path, sizeof(ctx->path) - 1);
    ctx->path[sizeof(ctx->path) - 1] = '\0';

    // Fork (or run synchronously on Windows)
    int is_child = 0;
    lygus_async_proc_t *proc = lygus_async_fork(snapshot_worker, ctx, &is_child);

    if (is_child) {
        // This only happens on POSIX, and lygus_async_fork already called _exit()
        // We never reach here, but just in case:
        free(ctx);
        _exit(1);
    }

    if (!proc) {
        free(ctx);
        return LYGUS_ERR_INTERNAL;
    }

    // Store async state
    out_async->proc = proc;
    out_async->ctx = ctx;
    out_async->index = last_index;
    out_async->term = last_term;
    strncpy(out_async->path, path, sizeof(out_async->path) - 1);
    out_async->path[sizeof(out_async->path) - 1] = '\0';

    return LYGUS_OK;
}

// Maybe..
int snapshot_wait_and_cleanup(snapshot_async_t *a, int *ok) {
    int r = snapshot_wait(a, ok);
    snapshot_async_cleanup(a);
    return r;
}


int snapshot_poll(snapshot_async_t *async,
                  int *out_done,
                  int *out_success)
{
    if (!async || !out_done || !out_success) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (!async->proc) {
        return LYGUS_ERR_INVALID_ARG;
    }

    return lygus_async_poll(async->proc, out_done, out_success);
}

int snapshot_wait(snapshot_async_t *async,
                  int *out_success)
{
    if (!async || !out_success) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (!async->proc) {
        return LYGUS_ERR_INVALID_ARG;
    }

    return lygus_async_wait(async->proc, out_success);
}

void snapshot_async_cleanup(snapshot_async_t *async)
{
    if (!async) return;

    if (async->proc) {
        lygus_async_free(async->proc);
        async->proc = NULL;
    }

    if (async->ctx) {
        free(async->ctx);
        async->ctx = NULL;
    }
}

// ============================================================================
// Snapshot Path Management
// ============================================================================

int snapshot_path_from_index(const char *dir,
                             uint64_t index,
                             uint64_t term,
                             char *out,
                             size_t out_len)
{
    if (!dir || !out || out_len == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Build filename
    char filename[128];
    snprintf(filename, sizeof(filename), "snap-%016" PRIx64 "-%016" PRIx64 ".dat",
             index, term);

    // Join with directory
    if (lygus_path_join(out, out_len, dir, filename) < 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    return LYGUS_OK;
}

// ============================================================================
// Directory Scanning (unified implementation)
// ============================================================================

int snapshot_find_latest(const char *dir,
                         char *out_path,
                         uint64_t *out_index,
                         uint64_t *out_term)
{
    if (!dir || !out_path) {
        return LYGUS_ERR_INVALID_ARG;
    }

    lygus_dir_t *d = lygus_dir_open(dir);
    if (!d) {
        return LYGUS_ERR_IO;
    }

    uint64_t best_index = 0;
    uint64_t best_term = 0;
    char best_name[256] = {0};

    const char *name;
    while ((name = lygus_dir_read(d)) != NULL) {
        uint64_t index, term;

        // Parse snap-{index}-{term}.dat
        if (sscanf(name, "snap-%" SCNx64 "-%" SCNx64 ".dat",
                   &index, &term) == 2) {
            // Higher index wins, or same index with higher term
            if (index > best_index ||
                (index == best_index && term > best_term)) {
                best_index = index;
                best_term = term;
                strncpy(best_name, name, sizeof(best_name) - 1);
            }
        }
    }

    lygus_dir_close(d);

    if (best_name[0] == '\0') {
        return LYGUS_ERR_KEY_NOT_FOUND;
    }

    lygus_path_join(out_path, 256, dir, best_name);

    if (out_index) *out_index = best_index;
    if (out_term) *out_term = best_term;

    return LYGUS_OK;
}

int snapshot_purge_old(const char *dir, int keep)
{
    if (!dir || keep < 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    lygus_dir_t *d = lygus_dir_open(dir);
    if (!d) {
        return LYGUS_ERR_IO;
    }

    // First pass: count snapshots
    int count = 0;
    const char *name;
    while ((name = lygus_dir_read(d)) != NULL) {
        uint64_t index, term;
        if (sscanf(name, "snap-%" SCNx64 "-%" SCNx64 ".dat",
                   &index, &term) == 2) {
            count++;
        }
    }

    if (count <= keep) {
        lygus_dir_close(d);
        return LYGUS_OK;  // Nothing to purge
    }

    // Collect all snapshots (reopen dir to restart iteration)
    lygus_dir_close(d);
    d = lygus_dir_open(dir);
    if (!d) {
        return LYGUS_ERR_IO;
    }

    typedef struct {
        uint64_t index;
        uint64_t term;
        char name[256];
    } snap_info_t;

    snap_info_t *snaps = malloc((size_t)count * sizeof(snap_info_t));
    if (!snaps) {
        lygus_dir_close(d);
        return LYGUS_ERR_NOMEM;
    }

    int idx = 0;
    while ((name = lygus_dir_read(d)) != NULL && idx < count) {
        uint64_t index, term;
        if (sscanf(name, "snap-%" SCNx64 "-%" SCNx64 ".dat",
                   &index, &term) == 2) {
            snaps[idx].index = index;
            snaps[idx].term = term;
            strncpy(snaps[idx].name, name, sizeof(snaps[idx].name) - 1);
            idx++;
        }
    }

    lygus_dir_close(d);

    // Sort by index descending (simple bubble sort, count is small)
    for (int i = 0; i < idx - 1; i++) {
        for (int j = 0; j < idx - i - 1; j++) {
            if (snaps[j].index < snaps[j + 1].index) {
                snap_info_t tmp = snaps[j];
                snaps[j] = snaps[j + 1];
                snaps[j + 1] = tmp;
            }
        }
    }

    // Delete all but the first 'keep' snapshots
    for (int i = keep; i < idx; i++) {
        char path[512];
        lygus_path_join(path, sizeof(path), dir, snaps[i].name);
        lygus_unlink(path);
    }

    free(snaps);
    return LYGUS_OK;
}