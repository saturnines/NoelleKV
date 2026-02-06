// Simple chaining hashtable

#include "kv_store.h"
#include "../util/logging.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

// ============================================================================
// Hash Function (FNV for binary keys)
// ============================================================================

static inline uint64_t fnv1a_hash(const void *data, size_t len) {
    const uint8_t *bytes = (const uint8_t *)data;
    uint64_t hash = 0xcbf29ce484222325ULL; // FNV offset basis

    for (size_t i = 0; i < len; i++) {
        hash ^= bytes[i];
        hash *= 0x100000001b3ULL; // FNV prime
    }

    return hash;
}

// ============================================================================
// Internal Types
// ============================================================================

typedef struct kv_entry {
    void   *key;
    size_t  klen;
    void   *val;
    size_t  vlen;
    struct kv_entry *next;
} kv_entry_t;

struct lygus_kv {
    kv_entry_t **buckets;
    size_t       capacity;
    size_t       count;
    size_t       total_mem;
};


// ============================================================================
// Config
// ============================================================================

#define INITIAL_CAPACITY  16
#define MAX_LOAD_FACTOR   0.75

// ============================================================================
// Internal Helpers
// ============================================================================

static kv_entry_t* entry_create(const void *key, size_t klen,
                                 const void *val, size_t vlen) {
    kv_entry_t *e = malloc(sizeof(kv_entry_t));
    if (!e) return NULL;

    e->key = malloc(klen);
    e->val = malloc(vlen ? vlen : 1); // force atleast 1 byte

    if (!e->key || !e->val) {
        free(e->key);
        free(e->val);
        free(e);
        return NULL;
    }

    memcpy(e->key, key, klen);
    memcpy(e->val, val, vlen);
    e->klen = klen;
    e->vlen = vlen;
    e->next = NULL;

    return e;
}

static void entry_destroy(kv_entry_t *e) {
    if (!e) return;
    free(e->key);
    free(e->val);
    free(e);
}

static int keys_equal(const void *k1, size_t len1, const void *k2, size_t len2) {
    return len1 == len2 && memcmp(k1, k2, len1) == 0;
}

static int kv_resize(lygus_kv_t *kv, size_t new_capacity);

// ============================================================================
// Public API
// ============================================================================

lygus_kv_t* lygus_kv_create(void) {
    lygus_kv_t *kv = malloc(sizeof(lygus_kv_t));
    if (!kv) return NULL;

    kv->buckets = calloc(INITIAL_CAPACITY, sizeof(kv_entry_t*));
    if (!kv->buckets) {
        free(kv);
        return NULL;
    }

    kv->capacity = INITIAL_CAPACITY;
    kv->count = 0;
    kv->total_mem = sizeof(lygus_kv_t) + INITIAL_CAPACITY * sizeof(kv_entry_t*);

    LOG_INFO_SIMPLE(LYGUS_MODULE_KV, LYGUS_EVENT_INIT, 0, 0);

    return kv;
}

void lygus_kv_destroy(lygus_kv_t *kv) {
    if (!kv) return;

    // Free the chains!
    for (size_t i = 0; i < kv->capacity; i++) {
        kv_entry_t *curr = kv->buckets[i];
        while (curr) {
            kv_entry_t *next = curr->next;
            entry_destroy(curr);
            curr = next;
        }
    }

    free(kv->buckets);
    free(kv);

    LOG_INFO_SIMPLE(LYGUS_MODULE_KV, LYGUS_EVENT_SHUTDOWN, 0, 0);
}

int lygus_kv_put(lygus_kv_t *kv, const void *key, size_t klen,
                 const void *val, size_t vlen) {
    if (!kv || !key || klen == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    // Check if resize needed (before insert to avoid load > MAX_LOAD_FACTOR)
    if ((double)kv->count / kv->capacity > MAX_LOAD_FACTOR) { // Every put does a double divide, this could be a cpu tax on actual loads but it should be fine now
        int ret = kv_resize(kv, kv->capacity * 2);
        if (ret < 0) return ret;
    }

    uint64_t hash = fnv1a_hash(key, klen);
    size_t idx = hash & (kv->capacity - 1); // Fast modulo for power of 2

    // Check if key exists (update case)
    kv_entry_t *curr = kv->buckets[idx];
    while (curr) {
        if (keys_equal(curr->key, curr->klen, key, klen)) {
            // Update existing entry
            void *new_val = malloc(vlen);
            if (!new_val) return LYGUS_ERR_NOMEM;

            memcpy(new_val, val, vlen);

            // Update memory
            kv->total_mem -= curr->vlen;
            kv->total_mem += vlen;

            free(curr->val);
            curr->val = new_val;
            curr->vlen = vlen;

            return LYGUS_OK;
        }
        curr = curr->next;
    }

    // Insert new entry (prepend to chain)
    kv_entry_t *new_entry = entry_create(key, klen, val, vlen);
    if (!new_entry) return LYGUS_ERR_NOMEM;

    new_entry->next = kv->buckets[idx];
    kv->buckets[idx] = new_entry;
    kv->count++;
    kv->total_mem += sizeof(kv_entry_t) + klen + vlen;

    return LYGUS_OK;
}

ssize_t lygus_kv_get(lygus_kv_t *kv, const void *key, size_t klen,
                     void *buf, size_t blen) {
    if (!kv || !key || klen == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    uint64_t hash = fnv1a_hash(key, klen);
    size_t idx = hash & (kv->capacity - 1);

    kv_entry_t *curr = kv->buckets[idx];
    while (curr) {
        if (keys_equal(curr->key, curr->klen, key, klen)) {
            // Found it
            if (buf && blen > 0) {
                size_t copy_len = curr->vlen < blen ? curr->vlen : blen;
                memcpy(buf, curr->val, copy_len);
            }
            return (ssize_t)curr->vlen; // Return actual value len
        }
        curr = curr->next;
    }

    return LYGUS_ERR_KEY_NOT_FOUND;
}

int lygus_kv_del(lygus_kv_t *kv, const void *key, size_t klen) {
    if (!kv || !key || klen == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    uint64_t hash = fnv1a_hash(key, klen);
    size_t idx = hash & (kv->capacity - 1);

    kv_entry_t *curr = kv->buckets[idx];
    kv_entry_t *prev = NULL;

    while (curr) {
        if (keys_equal(curr->key, curr->klen, key, klen)) {
            // Found it - remove from chain
            if (prev) {
                prev->next = curr->next;
            } else {
                kv->buckets[idx] = curr->next;
            }

            kv->total_mem -= sizeof(kv_entry_t) + curr->klen + curr->vlen;
            kv->count--;

            entry_destroy(curr);
            return LYGUS_OK;
        }
        prev = curr;
        curr = curr->next;
    }

    return LYGUS_ERR_KEY_NOT_FOUND;
}

size_t lygus_kv_count(lygus_kv_t *kv) {
    return kv ? kv->count : 0;
}

size_t lygus_kv_memory_usage(lygus_kv_t *kv) {
    return kv ? kv->total_mem : 0;
}

int lygus_kv_clear(lygus_kv_t *kv) {
    if (!kv) return LYGUS_ERR_INVALID_ARG;

    for (size_t i = 0; i < kv->capacity; i++) {
        kv_entry_t *curr = kv->buckets[i];
        while (curr) {
            kv_entry_t *next = curr->next;
            entry_destroy(curr);
            curr = next;
        }
        kv->buckets[i] = NULL;
    }

    kv->count = 0;
    kv->total_mem = sizeof(lygus_kv_t) + kv->capacity * sizeof(kv_entry_t*);

    return LYGUS_OK;
}

// ============================================================================
// Resize (internal)
// ============================================================================

static int kv_resize(lygus_kv_t *kv, size_t new_capacity) {
    assert((new_capacity & (new_capacity - 1)) == 0); // Must be power of 2

    kv_entry_t **new_buckets = calloc(new_capacity, sizeof(kv_entry_t*));
    if (!new_buckets) return LYGUS_ERR_NOMEM;

    // Rehash all entries
    for (size_t i = 0; i < kv->capacity; i++) {
        kv_entry_t *curr = kv->buckets[i];

        while (curr) {
            kv_entry_t *next = curr->next;

            // Recompute bucket index
            uint64_t hash = fnv1a_hash(curr->key, curr->klen);
            size_t new_idx = hash & (new_capacity - 1);

            // Prepend to new bucket
            curr->next = new_buckets[new_idx];
            new_buckets[new_idx] = curr;

            curr = next;
        }
    }

    // Update memory accounting (bucket array size changed)
    kv->total_mem -= kv->capacity * sizeof(kv_entry_t*);
    kv->total_mem += new_capacity * sizeof(kv_entry_t*);

    free(kv->buckets);
    kv->buckets = new_buckets;
    kv->capacity = new_capacity;

    return LYGUS_OK;
}

// ============================================================================
// Iterator (snapshots useage)
// ============================================================================


void lygus_kv_iter_init(lygus_kv_iter_t *it, lygus_kv_t *kv) {
    it->kv = kv;
    it->bucket_idx = 0;
    it->curr = NULL;

    // Find first non empty bucket
    while (it->bucket_idx < kv->capacity && !kv->buckets[it->bucket_idx]) {
        it->bucket_idx++;
    }

    if (it->bucket_idx < kv->capacity) {
        it->curr = kv->buckets[it->bucket_idx];
    }
}

int lygus_kv_iter_next(lygus_kv_iter_t *it,
                       const void **key, size_t *klen,
                       const void **val, size_t *vlen) {
    kv_entry_t *entry = (kv_entry_t *)it->curr;
    if (!entry) return 0; // Done

    // Return current entry
    *key = entry->key;
    *klen = entry->klen;
    *val = entry->val;
    *vlen = entry->vlen;

    // Advance to next entry
    entry = entry->next;

    // If chain exhausted, find next non empty bucket
    while (!entry && ++it->bucket_idx < it->kv->capacity) {
        entry = it->kv->buckets[it->bucket_idx];
    }

    it->curr = entry;

    return 1;
}