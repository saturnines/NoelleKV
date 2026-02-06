// kv_store.h - In-memory key-value store for Lygus state machine

#ifndef LYGUS_KV_STORE_H
#define LYGUS_KV_STORE_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

#include "../public/lygus_errors.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Opaque Handle
// ============================================================================

typedef struct lygus_kv lygus_kv_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a new KV store
 * @return KV store handle, or NULL on OOM
 */
lygus_kv_t* lygus_kv_create(void);

/**
 * Destroy KV store and free all entries
 */
void lygus_kv_destroy(lygus_kv_t *kv);

// ============================================================================
// Operations
// ============================================================================

/**
 * Put key-value pair (creates or updates)
 * 
 * @param kv    KV store
 * @param key   Key (arbitrary bytes, copied internally)
 * @param klen  Key length
 * @param val   Value (arbitrary bytes, copied internally)
 * @param vlen  Value length
 * @return      LYGUS_OK on success, negative error code otherwise
 */
int lygus_kv_put(lygus_kv_t *kv, const void *key, size_t klen, 
                 const void *val, size_t vlen);

/**
 * Get value for key
 * 
 * @param kv    KV store
 * @param key   Key to lookup
 * @param klen  Key length
 * @param buf   Output buffer (can be NULL to query size)
 * @param blen  Buffer length
 * @return      Number of bytes in value (>= 0), or negative error code
 * 
 * If buf is NULL, returns value size without copying.
 * If buf is too small, copies what fits and returns actual size.
 * Returns LYGUS_ERR_KEY_NOT_FOUND if key doesn't exist.
 */
ssize_t lygus_kv_get(lygus_kv_t *kv, const void *key, size_t klen, 
                     void *buf, size_t blen);

/**
 * Delete key
 * 
 * @param kv    KV store
 * @param key   Key to delete
 * @param klen  Key length
 * @return      LYGUS_OK on success, LYGUS_ERR_KEY_NOT_FOUND if not found
 */
int lygus_kv_del(lygus_kv_t *kv, const void *key, size_t klen);

/**
 * Clear all entries (keeps capacity)
 * 
 * @param kv  KV store
 * @return    LYGUS_OK on success
 */
int lygus_kv_clear(lygus_kv_t *kv);

// ============================================================================
// Stats
// ============================================================================

/**
 * Get number of entries
 */
size_t lygus_kv_count(lygus_kv_t *kv);

/**
 * Get approximate memory usage in bytes
 */
size_t lygus_kv_memory_usage(lygus_kv_t *kv);

// ============================================================================
// Iterator (for snapshots)
// ============================================================================

typedef struct {
    lygus_kv_t *kv;
    size_t bucket_idx;
    void *curr; // Opaque entry pointer
} lygus_kv_iter_t;

/**
 * Initialize iterator
 */
void lygus_kv_iter_init(lygus_kv_iter_t *it, lygus_kv_t *kv);

/**
 * Get next entry
 * 
 * @param it    Iterator
 * @param key   Output: key pointer (valid until next put/del/destroy)
 * @param klen  Output: key length
 * @param val   Output: value pointer (valid until next put/del/destroy)
 * @param vlen  Output: value length
 * @return      1 if entry returned, 0 if done
 */
int lygus_kv_iter_next(lygus_kv_iter_t *it, 
                       const void **key, size_t *klen,
                       const void **val, size_t *vlen);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_KV_STORE_H