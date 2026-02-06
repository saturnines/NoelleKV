#ifndef LYGUS_H
#define LYGUS_H


#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#include "lygus_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Handle
// ============================================================================

typedef struct lygus lygus_t;

// ============================================================================
// Options
// ============================================================================

typedef struct {
    const char *data_dir;        // path to data directory
    const char *peers;           // "host:port,host:port,..."
    uint16_t    node_id;         // 1-based replica ID

    size_t      wal_block_bytes; // 0 = default (65536)
    int         zstd_level;      // 0 = default (3)
    int         alr_max_batch;   // 0 = default (128)

    uint64_t    reserved[8];     // future-proofing
} lygus_opts_t;

// ============================================================================
// Stats
// ============================================================================

typedef struct {
    uint64_t applied_index;
    uint64_t commit_index;
    uint64_t term;
    uint64_t wal_bytes;
    uint64_t snapshot_bytes;
    uint64_t reads_total;
    uint64_t writes_total;

    uint32_t read_p50_us;
    uint32_t read_p99_us;
    uint32_t write_p50_us;
    uint32_t write_p99_us;

    uint16_t leader_id;          // 0 = unknown
    uint8_t  is_leader;
    uint8_t  _pad;
} lygus_stats_t;

// ============================================================================
// API
// ============================================================================

// Lifecycle
lygus_t *lygus_open(const lygus_opts_t *opts, lygus_err_t *err);
void     lygus_close(lygus_t *l);

// Writes
int lygus_put(lygus_t *l, const void *key, size_t klen, const void *val, size_t vlen);
int lygus_del(lygus_t *l, const void *key, size_t klen);
int lygus_flush(lygus_t *l);

// Reads
ssize_t lygus_get(lygus_t *l, const void *key, size_t klen, void *buf, size_t blen);
int     lygus_sync(lygus_t *l);

// Maintenance
int lygus_snapshot(lygus_t *l);

// Observability
int      lygus_get_stats(lygus_t *l, lygus_stats_t *out);
uint32_t lygus_version(void);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_H