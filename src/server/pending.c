/**
 * pending.c - Pending request tracking
 */

#include "pending.h"
#include <stdlib.h>
#include <string.h>
#include "public/lygus_errors.h"

// ============================================================================
// Internal Structure
// ============================================================================

struct pending_table {
    pending_entry_t    *entries;
    size_t              capacity;
    size_t              count;
    pending_complete_fn on_complete;
    void               *ctx;
};

// ============================================================================
// Lifecycle
// ============================================================================

pending_table_t *pending_create(size_t max_pending,
                                pending_complete_fn on_complete,
                                void *ctx) {
    if (max_pending == 0) max_pending = 1024;

    pending_table_t *t = calloc(1, sizeof(*t));
    if (!t) return NULL;

    t->entries = calloc(max_pending, sizeof(pending_entry_t));
    if (!t->entries) {
        free(t);
        return NULL;
    }

    t->capacity = max_pending;
    t->on_complete = on_complete;
    t->ctx = ctx;

    return t;
}

void pending_destroy(pending_table_t *t) {
    if (!t) return;
    free(t->entries);
    free(t);
}

// ============================================================================
// Internal Helpers
// ============================================================================

static pending_entry_t *find_entry(pending_table_t *t, uint64_t index) {
    for (size_t i = 0; i < t->capacity; i++) {
        if (t->entries[i].conn && t->entries[i].index == index) {
            return &t->entries[i];
        }
    }
    return NULL;
}

static pending_entry_t *find_free_slot(pending_table_t *t) {
    for (size_t i = 0; i < t->capacity; i++) {
        if (!t->entries[i].conn) {
            return &t->entries[i];
        }
    }
    return NULL;
}

static void clear_entry(pending_entry_t *e) {
    memset(e, 0, sizeof(*e));
}

// ============================================================================
// Operations
// ============================================================================

int pending_add(pending_table_t *t,
                uint64_t index,
                uint64_t term,
                uint64_t deadline_ms,
                void *conn,
                uint64_t request_id) {
    if (!t || !conn) return -1;

    pending_entry_t *slot = find_free_slot(t);
    if (!slot) return -1;

    slot->index = index;
    slot->term = term;
    slot->deadline_ms = deadline_ms;
    slot->conn = conn;
    slot->request_id = request_id;

    t->count++;
    return 0;
}

bool pending_remove(pending_table_t *t, uint64_t index, pending_entry_t *out) {
    if (!t) return false;

    pending_entry_t *e = find_entry(t, index);
    if (!e) return false;

    if (out) *out = *e;
    clear_entry(e);
    t->count--;

    return true;
}

bool pending_exists(const pending_table_t *t, uint64_t index) {
    if (!t) return false;
    return find_entry((pending_table_t *)t, index) != NULL;
}

// ============================================================================
// Completion
// ============================================================================

bool pending_complete(pending_table_t *t, uint64_t index) {
    if (!t) return false;

    pending_entry_t *e = find_entry(t, index);
    if (!e) return false;

    pending_entry_t copy = *e;
    clear_entry(e);
    t->count--;

    if (t->on_complete) {
        t->on_complete(&copy, 0, t->ctx);
    }

    return true;
}

bool pending_fail(pending_table_t *t, uint64_t index, int err) {
    if (!t) return false;

    pending_entry_t *e = find_entry(t, index);
    if (!e) return false;

    pending_entry_t copy = *e;
    clear_entry(e);
    t->count--;

    if (t->on_complete) {
        t->on_complete(&copy, err, t->ctx);
    }

    return true;
}

int pending_timeout_sweep(pending_table_t *t, uint64_t now_ms) {
    if (!t) return 0;

    int expired = 0;

    for (size_t i = 0; i < t->capacity; i++) {
        pending_entry_t *e = &t->entries[i];
        if (e->conn && e->deadline_ms <= now_ms) {
            pending_entry_t copy = *e;
            clear_entry(e);
            t->count--;
            expired++;

            if (t->on_complete) {
                t->on_complete(&copy, LYGUS_ERR_TIMEOUT, t->ctx);
            }
        }
    }

    return expired;
}

int pending_fail_conn(pending_table_t *t, void *conn, int err) {
    if (!t || !conn) return 0;

    int failed = 0;

    for (size_t i = 0; i < t->capacity; i++) {
        pending_entry_t *e = &t->entries[i];
        if (e->conn == conn) {
            pending_entry_t copy = *e;
            clear_entry(e);
            t->count--;
            failed++;

            if (t->on_complete) {
                t->on_complete(&copy, err, t->ctx);
            }
        }
    }

    return failed;
}

int pending_fail_all(pending_table_t *t, int err) {
    if (!t) return 0;

    int failed = 0;

    for (size_t i = 0; i < t->capacity; i++) {
        pending_entry_t *e = &t->entries[i];
        if (e->conn) {
            pending_entry_t copy = *e;
            clear_entry(e);
            t->count--;
            failed++;

            if (t->on_complete) {
                t->on_complete(&copy, err, t->ctx);
            }
        }
    }

    return failed;
}

int pending_fail_from(pending_table_t *t, uint64_t from_index, int err) {
    if (!t) return 0;

    int failed = 0;

    for (size_t i = 0; i < t->capacity; i++) {
        pending_entry_t *e = &t->entries[i];
        if (e->conn && e->index >= from_index) {
            pending_entry_t copy = *e;
            clear_entry(e);
            t->count--;
            failed++;

            if (t->on_complete) {
                t->on_complete(&copy, err, t->ctx);
            }
        }
    }

    return failed;
}

// ============================================================================
// Stats
// ============================================================================

size_t pending_count(const pending_table_t *t) {
    return t ? t->count : 0;
}

size_t pending_capacity(const pending_table_t *t) {
    return t ? t->capacity : 0;
}