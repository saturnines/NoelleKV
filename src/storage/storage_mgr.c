/**
 * storage_mgr.c - Storage Manager Implementation
 *
 * Cross-platform implementation:
 *   - POSIX: Async fork-based snapshots with COW
 *   - Windows: Synchronous snapshots (no fork)
 */

#include "storage_mgr.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#ifdef _WIN32
    #include <windows.h>
    #include <direct.h>
    #define mkdir_p(path) _mkdir(path)
    #define PATH_SEP "\\"
#else
    #include <unistd.h>
    #include <sys/stat.h>
    #include <sys/wait.h>
    #define mkdir_p(path) mkdir(path, 0755)
    #define PATH_SEP "/"
#endif

#include "../storage/wal/wal.h"
#include "../storage/wal/block_format.h"
#include "../storage/snapshot/snapshot.h"
#include "../state/kv_store.h"
#include "../public/lygus_errors.h"
#include "../util/logging.h"
#include "../state/kv_op.h"

// ============================================================================
// Internal Structure
// ============================================================================

struct storage_mgr {
    // Components
    lygus_kv_t *kv;
    wal_t      *wal;

    // Directories
    char *data_dir;
    char *wal_dir;
    char *snapshot_dir;

    // Configuration
    size_t snapshot_threshold;
    int    snapshots_to_keep;

    // State tracking
    uint64_t applied_index;
    uint64_t applied_term;
    uint64_t logged_index;
    uint64_t logged_term;

    // Snapshot state (for truncate validation)
    uint64_t snapshot_index;

    // WAL size tracking
    size_t wal_bytes_written;

    // Async snapshot state (POSIX only)
#ifndef _WIN32
    int      snapshot_in_progress;
    pid_t    snapshot_pid;
    uint64_t pending_snapshot_index;
    uint64_t pending_snapshot_term;
    char     snapshot_path[512];
#endif
};

// ============================================================================
// Recovery Context - Model B: Index only, no apply
// ============================================================================

typedef struct {
    storage_mgr_t *mgr;
    uint64_t       snapshot_index;
    uint64_t       highest_index;
    uint64_t       highest_term;
    int            error;
} recovery_ctx_t;

/**
 * WAL recovery callback
 *
 * Only tracks highest index/term seen in WAL.
 * Does NOT apply entries to KV, that's Raft's job!
 */
static int recovery_callback(const wal_entry_t *entry, void *user_data)
{
    recovery_ctx_t *ctx = (recovery_ctx_t *)user_data;

    // Skip entries covered by snapshot (they're already in KV)
    if (entry->index <= ctx->snapshot_index) {
        return LYGUS_OK;
    }

    // Track highest index/term for logged_index/logged_term
    if (entry->index > ctx->highest_index) {
        ctx->highest_index = entry->index;
        ctx->highest_term = entry->term;
    }

    // No KV operations - just indexing
    return LYGUS_OK;
}

// ============================================================================
// Configuration
// ============================================================================

void storage_mgr_config_init(storage_mgr_config_t *cfg)
{
    if (!cfg) return;

    memset(cfg, 0, sizeof(*cfg));
    cfg->snapshot_threshold = STORAGE_MGR_DEFAULT_SNAPSHOT_THRESHOLD;
    cfg->snapshots_to_keep = STORAGE_MGR_DEFAULT_SNAPSHOTS_TO_KEEP;
    cfg->wal_segment_size = STORAGE_MGR_DEFAULT_WAL_SEGMENT_SIZE;
    cfg->wal_sync_on_append = 1;
}

// ============================================================================
// Helper Functions
// ============================================================================

static char *build_path(const char *base, const char *suffix)
{
    size_t len = strlen(base) + strlen(PATH_SEP) + strlen(suffix) + 1;
    char *path = malloc(len);
    if (path) {
        snprintf(path, len, "%s%s%s", base, PATH_SEP, suffix);
    }
    return path;
}

static int ensure_directory(const char *path)
{
    int ret = mkdir_p(path);
    if (ret != 0 && errno != EEXIST) {
        return LYGUS_ERR_IO;
    }
    return LYGUS_OK;
}

// ============================================================================
// Lifecycle
// ============================================================================

int storage_mgr_open(const storage_mgr_config_t *cfg, storage_mgr_t **out)
{
    if (!cfg || !cfg->data_dir || !out) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret;
    storage_mgr_t *mgr = calloc(1, sizeof(storage_mgr_t));
    if (!mgr) {
        return LYGUS_ERR_NOMEM;
    }

    // Copy configuration
    mgr->snapshot_threshold = cfg->snapshot_threshold;
    mgr->snapshots_to_keep = cfg->snapshots_to_keep;

    // Setup directories
    mgr->data_dir = strdup(cfg->data_dir);
    mgr->wal_dir = cfg->wal_dir ? strdup(cfg->wal_dir)
                                : build_path(cfg->data_dir, "wal");
    mgr->snapshot_dir = cfg->snapshot_dir ? strdup(cfg->snapshot_dir)
                                          : build_path(cfg->data_dir, "snapshots");

    if (!mgr->data_dir || !mgr->wal_dir || !mgr->snapshot_dir) {
        ret = LYGUS_ERR_NOMEM;
        goto fail;
    }

    // Ensure directories exist
    if ((ret = ensure_directory(mgr->data_dir)) != LYGUS_OK ||
        (ret = ensure_directory(mgr->wal_dir)) != LYGUS_OK ||
        (ret = ensure_directory(mgr->snapshot_dir)) != LYGUS_OK) {
        goto fail;
    }

    // Create KV store
    mgr->kv = lygus_kv_create();
    if (!mgr->kv) {
        ret = LYGUS_ERR_NOMEM;
        goto fail;
    }

    // =========================================================================
    //  Load latest snapshot into KV
    // =========================================================================

    uint64_t snapshot_index = 0;
    uint64_t snapshot_term = 0;
    char snapshot_path[512];

    ret = snapshot_find_latest(mgr->snapshot_dir, snapshot_path,
                               &snapshot_index, &snapshot_term);

    if (ret == LYGUS_OK) {
        LOG_INFO(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
                 snapshot_index, snapshot_term, NULL, 0);

        ret = snapshot_load(snapshot_path, mgr->kv, &snapshot_index, &snapshot_term);
        if (ret != LYGUS_OK) {
            LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
                      snapshot_index, snapshot_term, NULL, 0);
            goto fail;
        }

        // KV now contains snapshot state
        mgr->applied_index = snapshot_index;
        mgr->applied_term = snapshot_term;
        mgr->snapshot_index = snapshot_index;

        LOG_INFO(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD,
                 snapshot_index, snapshot_term, NULL, 0);

    } else if (ret == LYGUS_ERR_KEY_NOT_FOUND) {
        // No snapshot, starting fresh
        mgr->applied_index = 0;
        mgr->applied_term = 0;
        mgr->snapshot_index = 0;
        LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_INIT, 0, 0);
    } else {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD, 0, 0, NULL, 0);
        goto fail;
    }

    // =========================================================================
    //  Open WAL, index entries (but DON'T apply)
    // =========================================================================

    recovery_ctx_t recovery_ctx = {
        .mgr = mgr,
        .snapshot_index = snapshot_index,
        .highest_index = snapshot_index,  // Start from snapshot
        .highest_term = snapshot_term,
        .error = LYGUS_OK
    };

    wal_opts_t wal_opts = {
        .data_dir = mgr->wal_dir,
        .zstd_level = 0,
        .fsync_bytes = 0,
        .fsync_interval_us = 0,
        .on_recover = recovery_callback,
        .user_data = &recovery_ctx
    };

    mgr->wal = wal_open(&wal_opts);
    if (!mgr->wal) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_RECOVERY, 0, 0, NULL, 0);
        ret = LYGUS_ERR_IO;
        goto fail;
    }

    if (recovery_ctx.error != LYGUS_OK) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_RECOVERY,
                  recovery_ctx.highest_index, recovery_ctx.highest_term, NULL, 0);
        ret = recovery_ctx.error;
        goto fail;
    }

    // logged_index/term from WAL (or snapshot if WAL empty)
    mgr->logged_index = recovery_ctx.highest_index;
    mgr->logged_term = recovery_ctx.highest_term;

    // Get WAL stats for size tracking
    wal_stats_t wal_stats;
    if (wal_get_stats(mgr->wal, &wal_stats) == LYGUS_OK) {
        mgr->wal_bytes_written = wal_stats.bytes_written;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_INIT,
             mgr->applied_index, mgr->logged_index);

    *out = mgr;
    return LYGUS_OK;

fail:
    storage_mgr_close(mgr);
    return ret;
}

void storage_mgr_close(storage_mgr_t *mgr)
{
    if (!mgr) return;

#ifndef _WIN32
    if (mgr->snapshot_in_progress && mgr->snapshot_pid > 0) {
        int status;
        waitpid(mgr->snapshot_pid, &status, 0);
        mgr->snapshot_in_progress = 0;
    }
#endif

    if (mgr->wal) {
        wal_close(mgr->wal);
    }

    if (mgr->kv) {
        lygus_kv_destroy(mgr->kv);
    }

    free(mgr->data_dir);
    free(mgr->wal_dir);
    free(mgr->snapshot_dir);
    free(mgr);
}

// ============================================================================
// Moment 1: Log Operations
// ============================================================================

int storage_mgr_log_put(storage_mgr_t *mgr,
                        uint64_t index, uint64_t term,
                        const void *key, size_t key_len,
                        const void *val, size_t val_len)
{
    if (!mgr || !key) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_put(mgr->wal, index, term, key, key_len, val, val_len);
    if (ret != LYGUS_OK) {
        return ret;
    }

    mgr->logged_index = index;
    mgr->logged_term = term;
    mgr->wal_bytes_written += 32 + key_len + val_len;

    return LYGUS_OK;
}

int storage_mgr_log_del(storage_mgr_t *mgr,
                        uint64_t index, uint64_t term,
                        const void *key, size_t key_len)
{
    if (!mgr || !key) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_del(mgr->wal, index, term, key, key_len);
    if (ret != LYGUS_OK) {
        return ret;
    }

    mgr->logged_index = index;
    mgr->logged_term = term;
    mgr->wal_bytes_written += 24 + key_len;

    return LYGUS_OK;
}

int storage_mgr_log_noop(storage_mgr_t *mgr,
                         uint64_t index, uint64_t term)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_noop_sync(mgr->wal, index, term);
    if (ret != LYGUS_OK) {
        return ret;
    }

    mgr->logged_index = index;
    mgr->logged_term = term;
    mgr->wal_bytes_written += 20;

    return LYGUS_OK;
}

int storage_mgr_log_raw(storage_mgr_t *mgr,
                        uint64_t index, uint64_t term,
                        const void *data, size_t len) {
    if (!mgr || !data || len == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (index != mgr->logged_index + 1) {
        return LYGUS_ERR_OUT_OF_ORDER;
    }

    kv_op_type_t op_type;
    const void *key, *val;
    uint32_t klen, vlen;

    int ret = kv_op_deserialize(data, len, &op_type, &key, &klen, &val, &vlen);
    if (ret != 0) {
        return LYGUS_ERR_MALFORMED;
    }

    switch (op_type) {
        case KV_OP_PUT:
            ret = wal_put(mgr->wal, index, term, key, klen, val, vlen);
            if (ret != LYGUS_OK) return ret;
            mgr->wal_bytes_written += 32 + klen + vlen;
            break;

        case KV_OP_DEL:
            ret = wal_del(mgr->wal, index, term, key, klen);
            if (ret != LYGUS_OK) return ret;
            mgr->wal_bytes_written += 24 + klen;
            break;

        case KV_OP_NOOP:
            ret = wal_noop_sync(mgr->wal, index, term);
            if (ret != LYGUS_OK) return ret;
            mgr->wal_bytes_written += 20;
            break;

        default:
            return LYGUS_ERR_MALFORMED;
    }

    mgr->logged_index = index;
    mgr->logged_term = term;

    return LYGUS_OK;
}


int storage_mgr_log_fsync(storage_mgr_t *mgr) {
    if (!mgr || !mgr->wal) {
        return LYGUS_ERR_INVALID_ARG;
    }
    return wal_fsync(mgr->wal);
}

// ============================================================================
// Moment 2: Apply Operations
// ============================================================================

int storage_mgr_apply_put(storage_mgr_t *mgr,
                          uint64_t index, uint64_t term,
                          const void *key, size_t key_len,
                          const void *val, size_t val_len)
{
    if (!mgr || !key) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (index != mgr->applied_index + 1) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_APPLY_STALLED,
                  index, mgr->applied_index, NULL, 0);
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = lygus_kv_put(mgr->kv, key, key_len, val, val_len);
    if (ret != LYGUS_OK) {
        return ret;
    }

    mgr->applied_index = index;
    mgr->applied_term = term;

    return LYGUS_OK;
}

int storage_mgr_apply_del(storage_mgr_t *mgr,
                          uint64_t index, uint64_t term,
                          const void *key, size_t key_len)
{
    if (!mgr || !key) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (index != mgr->applied_index + 1) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_APPLY_STALLED,
                  index, mgr->applied_index, NULL, 0);
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = lygus_kv_del(mgr->kv, key, key_len);
    if (ret != LYGUS_OK && ret != LYGUS_ERR_KEY_NOT_FOUND) {
        return ret;
    }

    mgr->applied_index = index;
    mgr->applied_term = term;

    return LYGUS_OK;
}

int storage_mgr_apply_noop(storage_mgr_t *mgr,
                           uint64_t index, uint64_t term)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (index != mgr->applied_index + 1) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_APPLY_STALLED,
                  index, mgr->applied_index, NULL, 0);
        return LYGUS_ERR_INVALID_ARG;
    }

    mgr->applied_index = index;
    mgr->applied_term = term;

    return LYGUS_OK;
}

// ============================================================================
// Read Operations
// ============================================================================

ssize_t storage_mgr_get(storage_mgr_t *mgr,
                        const void *key, size_t key_len,
                        void *val_out, size_t val_cap)
{
    if (!mgr || !key) {
        return LYGUS_ERR_INVALID_ARG;
    }

    return lygus_kv_get(mgr->kv, key, key_len, val_out, val_cap);
}

/**
 * Replay WAL entries from applied_index+1 to target_index
 *
 * Called by Raft after recovery to apply committed entries.
 */
int storage_mgr_replay_to(storage_mgr_t *mgr, uint64_t target_index)
{
    if (!mgr) return LYGUS_ERR_INVALID_ARG;


    if (mgr->applied_index >= target_index) return LYGUS_OK;
    if (target_index > mgr->logged_index) return LYGUS_ERR_INVALID_ARG;

    for (uint64_t idx = mgr->applied_index + 1; idx <= target_index; idx++) {
        uint8_t buf[WAL_BLOCK_SIZE];
        wal_entry_t entry;

        int ret = wal_read_entry(mgr->wal, idx, &entry, buf, sizeof(buf));
        if (ret != LYGUS_OK) {
            return ret;
        }

        switch (entry.type) {
            case WAL_ENTRY_PUT:
                ret = lygus_kv_put(mgr->kv, entry.key, entry.klen,
                                   entry.val, entry.vlen);
                break;

            case WAL_ENTRY_DEL:
                ret = lygus_kv_del(mgr->kv, entry.key, entry.klen);
                if (ret == LYGUS_ERR_KEY_NOT_FOUND) {
                    ret = LYGUS_OK;
                }
                break;

            case WAL_ENTRY_NOOP_SYNC:
            case WAL_ENTRY_SNAP_MARK:
                ret = LYGUS_OK;
                break;

            default:
                LOG_WARN(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_RECOVERY,
                         idx, entry.type, NULL, 0);
                ret = LYGUS_OK;
                break;
        }

        if (ret != LYGUS_OK) {
            LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_RECOVERY,
                      idx, mgr->applied_index, NULL, 0);
            return ret;
        }

        mgr->applied_index = idx;
        mgr->applied_term = entry.term;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_RECOVERY,
             mgr->applied_index, mgr->applied_term);

    return LYGUS_OK;
}

// ============================================================================
// Snapshot Operations - Platform Specific
// ============================================================================

static int snapshot_bookkeeping(storage_mgr_t *mgr,
                                uint64_t snap_index,
                                uint64_t snap_term)
{
    int ret;

    ret = wal_snap_mark(mgr->wal, snap_index, snap_term);
    if (ret != LYGUS_OK) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                  snap_index, snap_term, NULL, 0);
        return ret;
    }

    ret = wal_fsync(mgr->wal);
    if (ret != LYGUS_OK) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                  snap_index, snap_term, NULL, 0);
        return ret;
    }

    ret = wal_purge_before(mgr->wal, snap_index);
    if (ret != LYGUS_OK) {
        LOG_WARN(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                 snap_index, snap_term, NULL, 0);
    }

    mgr->wal_bytes_written = 0;

    mgr->snapshot_index = snap_index;

    ret = snapshot_purge_old(mgr->snapshot_dir, mgr->snapshots_to_keep);
    if (ret != LYGUS_OK) {
        LOG_WARN(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                 snap_index, snap_term, NULL, 0);
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
             snap_index, snap_term);

    return LYGUS_OK;
}

#ifdef _WIN32
// =============================================================================
// Windows Implementation: Synchronous Snapshots
// =============================================================================

int storage_mgr_maybe_snapshot(storage_mgr_t *mgr)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (mgr->wal_bytes_written < mgr->snapshot_threshold) {
        return LYGUS_OK;
    }

    if (mgr->applied_index == 0) {
        return LYGUS_OK;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_START,
             mgr->applied_index, mgr->applied_term);

    char snap_path[512];
    int ret = snapshot_path_from_index(mgr->snapshot_dir,
                                       mgr->applied_index,
                                       mgr->applied_term,
                                       snap_path, sizeof(snap_path));
    if (ret != LYGUS_OK) {
        return ret;
    }

    ret = snapshot_write(mgr->kv, mgr->applied_index, mgr->applied_term, snap_path);
    if (ret != LYGUS_OK) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_START,
                  mgr->applied_index, mgr->applied_term, NULL, 0);
        return ret;
    }

    return snapshot_bookkeeping(mgr, mgr->applied_index, mgr->applied_term);
}

int storage_mgr_poll_snapshot(storage_mgr_t *mgr)
{
    (void)mgr;
    return LYGUS_OK;
}

int storage_mgr_force_snapshot(storage_mgr_t *mgr)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (mgr->applied_index == 0) {
        return LYGUS_OK;
    }

    char snap_path[512];
    int ret = snapshot_path_from_index(mgr->snapshot_dir,
                                       mgr->applied_index,
                                       mgr->applied_term,
                                       snap_path, sizeof(snap_path));
    if (ret != LYGUS_OK) {
        return ret;
    }

    ret = snapshot_write(mgr->kv, mgr->applied_index, mgr->applied_term, snap_path);
    if (ret != LYGUS_OK) {
        return ret;
    }

    return snapshot_bookkeeping(mgr, mgr->applied_index, mgr->applied_term);
}

int storage_mgr_snapshot_in_progress(const storage_mgr_t *mgr)
{
    (void)mgr;
    return 0;
}

#else
// =============================================================================
// POSIX Implementation: Async Fork-Based Snapshots
// =============================================================================

int storage_mgr_maybe_snapshot(storage_mgr_t *mgr)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (mgr->snapshot_in_progress) {
        return LYGUS_OK;
    }

    if (mgr->wal_bytes_written < mgr->snapshot_threshold) {
        return LYGUS_OK;
    }

    if (mgr->applied_index == 0) {
        return LYGUS_OK;
    }

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_START,
             mgr->applied_index, mgr->applied_term);

    mgr->pending_snapshot_index = mgr->applied_index;
    mgr->pending_snapshot_term = mgr->applied_term;

    int ret = snapshot_path_from_index(mgr->snapshot_dir,
                                       mgr->pending_snapshot_index,
                                       mgr->pending_snapshot_term,
                                       mgr->snapshot_path,
                                       sizeof(mgr->snapshot_path));
    if (ret != LYGUS_OK) {
        return ret;
    }

    pid_t pid = fork();

    if (pid < 0) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_START,
                  mgr->pending_snapshot_index, mgr->pending_snapshot_term, NULL, 0);
        return LYGUS_ERR_IO;
    }

    if (pid == 0) {
        int child_ret = snapshot_write(mgr->kv,
                                       mgr->pending_snapshot_index,
                                       mgr->pending_snapshot_term,
                                       mgr->snapshot_path);
        _exit(child_ret == LYGUS_OK ? 0 : 1);
    }

    mgr->snapshot_in_progress = 1;
    mgr->snapshot_pid = pid;

    LOG_DEBUG(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_START,
              mgr->pending_snapshot_index, mgr->pending_snapshot_term, NULL, 0);

    return LYGUS_OK;
}

int storage_mgr_poll_snapshot(storage_mgr_t *mgr)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (!mgr->snapshot_in_progress) {
        return LYGUS_OK;
    }

    int status;
    pid_t result = waitpid(mgr->snapshot_pid, &status, WNOHANG);

    if (result == 0) {
        return LYGUS_OK;
    }

    if (result < 0) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                  mgr->pending_snapshot_index, mgr->pending_snapshot_term, NULL, 0);
        mgr->snapshot_in_progress = 0;
        return LYGUS_ERR_IO;
    }

    mgr->snapshot_in_progress = 0;

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                 mgr->pending_snapshot_index, mgr->pending_snapshot_term);

        return snapshot_bookkeeping(mgr, mgr->pending_snapshot_index,
                                    mgr->pending_snapshot_term);
    } else {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_DONE,
                  mgr->pending_snapshot_index, mgr->pending_snapshot_term, NULL, 0);
        unlink(mgr->snapshot_path);
        return LYGUS_ERR_IO;
    }
}

int storage_mgr_force_snapshot(storage_mgr_t *mgr)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (mgr->snapshot_in_progress) {
        return LYGUS_ERR_BUSY;
    }

    if (mgr->applied_index == 0) {
        return LYGUS_OK;
    }

    mgr->pending_snapshot_index = mgr->applied_index;
    mgr->pending_snapshot_term = mgr->applied_term;

    int ret = snapshot_path_from_index(mgr->snapshot_dir,
                                       mgr->pending_snapshot_index,
                                       mgr->pending_snapshot_term,
                                       mgr->snapshot_path,
                                       sizeof(mgr->snapshot_path));
    if (ret != LYGUS_OK) {
        return ret;
    }

    pid_t pid = fork();

    if (pid < 0) {
        return LYGUS_ERR_IO;
    }

    if (pid == 0) {
        int child_ret = snapshot_write(mgr->kv,
                                       mgr->pending_snapshot_index,
                                       mgr->pending_snapshot_term,
                                       mgr->snapshot_path);
        _exit(child_ret == LYGUS_OK ? 0 : 1);
    }

    mgr->snapshot_in_progress = 1;
    mgr->snapshot_pid = pid;

    return LYGUS_OK;
}

int storage_mgr_snapshot_in_progress(const storage_mgr_t *mgr)
{
    return mgr ? mgr->snapshot_in_progress : 0;
}

#endif // _WIN32 vs POSIX

// ============================================================================
// Install Snapshot (from leader)
// ============================================================================

int storage_mgr_install_snapshot(storage_mgr_t *mgr,
                                 const void *data, size_t data_len,
                                 uint64_t index, uint64_t term)
{
    if (!mgr || !data || data_len == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

#ifndef _WIN32
    if (mgr->snapshot_in_progress) {
        int status;
        waitpid(mgr->snapshot_pid, &status, 0);
        mgr->snapshot_in_progress = 0;
    }
#endif

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD, index, term);

    char snap_path[512];
    int ret = snapshot_path_from_index(mgr->snapshot_dir, index, term,
                                       snap_path, sizeof(snap_path));
    if (ret != LYGUS_OK) {
        return ret;
    }

    char tmp_path[520];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", snap_path);

    FILE *f = fopen(tmp_path, "wb");
    if (!f) {
        return LYGUS_ERR_IO;
    }

    if (fwrite(data, 1, data_len, f) != data_len) {
        fclose(f);
        unlink(tmp_path);
        return LYGUS_ERR_IO;
    }

    fclose(f);

    if (rename(tmp_path, snap_path) != 0) {
        unlink(tmp_path);
        return LYGUS_ERR_IO;
    }

    uint64_t loaded_index, loaded_term;
    ret = snapshot_load(snap_path, mgr->kv, &loaded_index, &loaded_term);
    if (ret != LYGUS_OK) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD, index, term, NULL, 0);
        return ret;
    }

    mgr->applied_index = loaded_index;
    mgr->applied_term = loaded_term;
    mgr->logged_index = loaded_index;
    mgr->logged_term = loaded_term;
    mgr->snapshot_index = loaded_index;

    ret = wal_clear(mgr->wal);
    if (ret != LYGUS_OK) {
        LOG_WARN(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD, index, term, NULL, 0);
    }

    snapshot_purge_old(mgr->snapshot_dir, mgr->snapshots_to_keep);

    LOG_INFO_SIMPLE(LYGUS_MODULE_STORAGE, LYGUS_EVENT_SNAPSHOT_LOAD, index, term);

    return LYGUS_OK;
}

// ============================================================================
// Get Snapshot (for sending to follower)
// ============================================================================

ssize_t storage_mgr_get_snapshot(storage_mgr_t *mgr,
                                 void *data_out, size_t data_cap,
                                 uint64_t *index_out, uint64_t *term_out)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    char snap_path[512];
    uint64_t snap_index, snap_term;

    int ret = snapshot_find_latest(mgr->snapshot_dir, snap_path,
                                   &snap_index, &snap_term);
    if (ret != LYGUS_OK) {
        return ret;
    }

    FILE *f = fopen(snap_path, "rb");
    if (!f) {
        return LYGUS_ERR_IO;
    }

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (index_out) *index_out = snap_index;
    if (term_out) *term_out = snap_term;

    if (!data_out || data_cap == 0) {
        fclose(f);
        return size;
    }

    size_t to_read = (size_t)size < data_cap ? (size_t)size : data_cap;
    if (fread(data_out, 1, to_read, f) != to_read) {
        fclose(f);
        return LYGUS_ERR_IO;
    }

    fclose(f);
    return size;
}

// ============================================================================
// State Queries
// ============================================================================

uint64_t storage_mgr_applied_index(const storage_mgr_t *mgr)
{
    return mgr ? mgr->applied_index : 0;
}

uint64_t storage_mgr_applied_term(const storage_mgr_t *mgr)
{
    return mgr ? mgr->applied_term : 0;
}

uint64_t storage_mgr_logged_index(const storage_mgr_t *mgr)
{
    return mgr ? mgr->logged_index : 0;
}

uint64_t storage_mgr_logged_term(const storage_mgr_t *mgr)
{
    return mgr ? mgr->logged_term : 0;
}

size_t storage_mgr_wal_size(const storage_mgr_t *mgr)
{
    return mgr ? mgr->wal_bytes_written : 0;
}

lygus_kv_t *storage_mgr_get_kv(storage_mgr_t *mgr) {
    return mgr ? mgr->kv : 0; // maybe null is better
}

// ============================================================================
// Log Access (for Raft replication)
// ============================================================================

// Entry type mapping from WAL to glue format
#define GLUE_ENTRY_PUT   1
#define GLUE_ENTRY_DEL   2
#define GLUE_ENTRY_NOOP  3

uint64_t storage_mgr_first_index(const storage_mgr_t *mgr)
{
    if (!mgr) {
        return 0;
    }

    // First readable index is one after snapshot
    // (entries at or before snapshot_index are compacted away)
    if (mgr->snapshot_index > 0) {
        return mgr->snapshot_index + 1;
    }

    // No snapshot - first index is 1 (if any entries exist)
    return (mgr->logged_index > 0) ? 1 : 0;
}

ssize_t storage_mgr_log_get(storage_mgr_t *mgr, uint64_t index,
                            uint64_t *term_out, void *buf, size_t buf_cap)
{
    if (!mgr || !mgr->wal) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Check index bounds
    uint64_t first = storage_mgr_first_index(mgr);
    if (index < first || index > mgr->logged_index) {
        return LYGUS_ERR_KEY_NOT_FOUND;
    }

    // Read entry from WAL
    uint8_t wal_buf[WAL_BLOCK_SIZE];
    wal_entry_t entry;

    int ret = wal_read_entry(mgr->wal, index, &entry, wal_buf, sizeof(wal_buf));
    if (ret != LYGUS_OK) {
        return ret;
    }

    // Return term
    if (term_out) {
        *term_out = entry.term;
    }

    // Calculate serialized size: [type:1][klen:4][vlen:4][key][value]
    size_t klen = entry.klen;
    size_t vlen = entry.vlen;
    size_t needed = 1 + 4 + 4 + klen + vlen;  // 9 byte header + data

    // If no buffer or too small, return needed size
    if (!buf || buf_cap < needed) {
        return -(ssize_t)needed;
    }

    // Serialize entry
    uint8_t *p = (uint8_t *)buf;

    // Type mapping
    uint8_t glue_type;
    switch (entry.type) {
        case WAL_ENTRY_PUT:
            glue_type = GLUE_ENTRY_PUT;
            break;
        case WAL_ENTRY_DEL:
            glue_type = GLUE_ENTRY_DEL;
            break;
        case WAL_ENTRY_NOOP_SYNC:
        case WAL_ENTRY_SNAP_MARK:
            glue_type = GLUE_ENTRY_NOOP;
            klen = 0;
            vlen = 0;
            break;
        default:
            glue_type = GLUE_ENTRY_NOOP;
            klen = 0;
            vlen = 0;
            break;
    }

    // Write header
    p[0] = glue_type;

    uint32_t klen32 = (uint32_t)klen;
    uint32_t vlen32 = (uint32_t)vlen;
    memcpy(p + 1, &klen32, 4);
    memcpy(p + 5, &vlen32, 4);

    // Write key
    if (klen > 0 && entry.key) {
        memcpy(p + 9, entry.key, klen);
    }

    // Write value
    if (vlen > 0 && entry.val) {
        memcpy(p + 9 + klen, entry.val, vlen);
    }

    return (ssize_t)(9 + klen + vlen);
}

// ============================================================================
// Log Truncation
// ============================================================================

/**
 * Truncate WAL after given index.
 */
int storage_mgr_truncate_after(storage_mgr_t *mgr, uint64_t after_index)
{
    if (!mgr) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Can't truncate below snapshot - that data is gone from WAL
    if (after_index < mgr->snapshot_index) {
        LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_CORRUPTION,
                  after_index, mgr->snapshot_index, NULL, 0);
        return LYGUS_ERR_INVALID_ARG;
    }

    int ret = wal_truncate_after(mgr->wal, after_index);
    if (ret != LYGUS_OK) {
        return ret;
    }

    // Update logged state
    mgr->logged_index = after_index;

    // If we truncated below applied_index, roll back applied state too
    // This requires reloading from snapshot
    if (after_index < mgr->applied_index) {
        // Reload snapshot to reset KV state
        char snap_path[512];
        uint64_t snap_idx, snap_term;

        ret = snapshot_find_latest(mgr->snapshot_dir, snap_path, &snap_idx, &snap_term);
        if (ret == LYGUS_OK && snap_idx <= after_index) {
            // Clear and reload KV from snapshot
            lygus_kv_destroy(mgr->kv);
            mgr->kv = lygus_kv_create();
            if (!mgr->kv) {
                return LYGUS_ERR_NOMEM;
            }

            ret = snapshot_load(snap_path, mgr->kv, &snap_idx, &snap_term);
            if (ret != LYGUS_OK) {
                return ret;
            }

            mgr->applied_index = snap_idx;
            mgr->applied_term = snap_term;

            // Replay up to after_index
            if (after_index > snap_idx) {
                ret = storage_mgr_replay_to(mgr, after_index);
                if (ret != LYGUS_OK) {
                    return ret;
                }
            }
        } else if (ret == LYGUS_ERR_KEY_NOT_FOUND && after_index == 0) {
            // No snapshot, truncating to 0 - clear everything
            lygus_kv_destroy(mgr->kv);
            mgr->kv = lygus_kv_create();
            if (!mgr->kv) {
                return LYGUS_ERR_NOMEM;
            }
            mgr->applied_index = 0;
            mgr->applied_term = 0;
        } else {
            // Snapshot is ahead of truncate point - invalid
            LOG_ERROR(LYGUS_MODULE_STORAGE, LYGUS_EVENT_WAL_CORRUPTION,
                      after_index, snap_idx, NULL, 0);
            return LYGUS_ERR_INVALID_ARG;
        }
    }

    // Update logged_term
    if (after_index == mgr->applied_index) {
        mgr->logged_term = mgr->applied_term;
    } else if (after_index == 0) {
        mgr->logged_term = 0;
    }
    // Otherwise term should come from WAL lookup if needed

    return LYGUS_OK;
}