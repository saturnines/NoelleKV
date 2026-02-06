/**
 * handler.c - Request handling and Raft integration
 */

#include "handler.h"
#include "pending.h"
#include "protocol.h"

// External dependencies
#include "raft.h"
#include "raft/raft_glue.h"
#include "storage/storage_mgr.h"
#include "event/event_loop.h"
#include "state/kv_store.h"
#include "ALRs/alr.h"
#include "public/lygus_errors.h"

#include <stdlib.h>
#include <string.h>

// ============================================================================
// Defaults
// ============================================================================

#define DEFAULT_MAX_PENDING      1024
#define DEFAULT_TIMEOUT_MS       5000
#define DEFAULT_ALR_CAPACITY     4096
#define DEFAULT_ALR_SLAB         (16 * 1024 * 1024)
#define DEFAULT_MAX_KEY          1024
#define DEFAULT_MAX_VALUE        (1024 * 1024)

#define RESPONSE_BUF_SIZE        (2 * 1024 * 1024)
#define ENTRY_BUF_SIZE           (1024 * 1024 + 1024)

// ============================================================================
// Internal Structure
// ============================================================================

struct handler {
    // Dependencies (borrowed)
    event_loop_t    *loop;
    raft_t          *raft;
    raft_glue_ctx_t *glue_ctx;
    storage_mgr_t   *storage;
    lygus_kv_t      *kv;

    // Owned components
    protocol_ctx_t  *proto;
    pending_table_t *pending;
    alr_t           *alr;

    // Config
    uint32_t         timeout_ms;
    const char      *version;
    bool             leader_only;  // Benchmark mode: reject reads on followers

    // Scratch buffers
    char            *resp_buf;
    uint8_t         *entry_buf;

    // Stats
    handler_stats_t  stats;
};

// ============================================================================
// Forward Declarations
// ============================================================================

static void on_pending_complete(const pending_entry_t *entry, int err, void *ctx);
static void on_alr_respond(void *conn, const void *key, size_t klen,
                           const void *val, size_t vlen,
                           lygus_err_t err, void *ctx);

// ============================================================================
// Lifecycle
// ============================================================================

handler_t *handler_create(const handler_config_t *cfg) {
    if (!cfg || !cfg->loop || !cfg->raft || !cfg->glue_ctx || !cfg->storage || !cfg->kv) {
        return NULL;
    }

    handler_t *h = calloc(1, sizeof(*h));
    if (!h) return NULL;

    h->loop = cfg->loop;
    h->raft = cfg->raft;
    h->glue_ctx = cfg->glue_ctx;
    h->storage = cfg->storage;
    h->kv = cfg->kv;
    h->version = cfg->version ? cfg->version : "unknown";
    h->timeout_ms = cfg->request_timeout_ms > 0 ? cfg->request_timeout_ms : DEFAULT_TIMEOUT_MS;
    h->leader_only = cfg->leader_only_reads;

    // Create protocol context
    size_t max_key = cfg->max_key_size > 0 ? cfg->max_key_size : DEFAULT_MAX_KEY;
    size_t max_val = cfg->max_value_size > 0 ? cfg->max_value_size : DEFAULT_MAX_VALUE;
    h->proto = protocol_ctx_create(max_key, max_val);
    if (!h->proto) goto fail;

    // Create pending table
    size_t max_pending = cfg->max_pending > 0 ? cfg->max_pending : DEFAULT_MAX_PENDING;
    h->pending = pending_create(max_pending, on_pending_complete, h);
    if (!h->pending) goto fail;

    // Create ALR (even in leader_only mode, we need it for leader reads)
    uint16_t alr_cap = cfg->alr_capacity > 0 ? cfg->alr_capacity : DEFAULT_ALR_CAPACITY;
    size_t alr_slab = cfg->alr_slab_size > 0 ? cfg->alr_slab_size : DEFAULT_ALR_SLAB;
    uint32_t alr_timeout = cfg->alr_timeout_ms > 0 ? cfg->alr_timeout_ms : DEFAULT_TIMEOUT_MS;

    alr_config_t alr_cfg = {
        .raft = h->raft,
        .kv = h->kv,
        .respond = on_alr_respond,
        .respond_ctx = h,
        .capacity = alr_cap,
        .slab_size = alr_slab,
        .timeout_ms = alr_timeout,
    };
    h->alr = alr_create(&alr_cfg);
    if (!h->alr) goto fail;

    // Allocate scratch buffers
    h->resp_buf = malloc(RESPONSE_BUF_SIZE);
    h->entry_buf = malloc(ENTRY_BUF_SIZE);
    if (!h->resp_buf || !h->entry_buf) goto fail;

    return h;

fail:
    handler_destroy(h);
    return NULL;
}

void handler_destroy(handler_t *h) {
    if (!h) return;

    // Fail all pending requests
    if (h->pending) {
        pending_fail_all(h->pending, LYGUS_ERR_INTERNAL);
        pending_destroy(h->pending);
    }

    if (h->alr) alr_destroy(h->alr);
    if (h->proto) protocol_ctx_destroy(h->proto);

    free(h->resp_buf);
    free(h->entry_buf);
    free(h);
}

// ============================================================================
// Callbacks
// ============================================================================

static void on_pending_complete(const pending_entry_t *entry, int err, void *ctx) {
    handler_t *h = (handler_t *)ctx;
    conn_t *conn = (conn_t *)entry->conn;

    if (!conn) return;
    if (conn_get_state(conn) == CONN_STATE_CLOSED) return;

    int n;
    if (err == 0) {
        n = protocol_fmt_ok(h->resp_buf, RESPONSE_BUF_SIZE);
        h->stats.requests_ok++;
    } else if (err == LYGUS_ERR_TIMEOUT) {
        n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "timeout");
        h->stats.requests_timeout++;
    } else {
        n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, lygus_strerror(err));
        h->stats.requests_error++;
    }

    if (n > 0) {
        conn_send(conn, h->resp_buf, (size_t)n);
    }
}

static void on_alr_respond(void *conn_ptr, const void *key, size_t klen,
                           const void *val, size_t vlen,
                           lygus_err_t err, void *ctx) {
    (void)key;
    (void)klen;

    handler_t *h = (handler_t *)ctx;
    conn_t *conn = (conn_t *)conn_ptr;

    if (!conn) return;

    int n;
    if (err == LYGUS_OK) {
        n = protocol_fmt_value(h->resp_buf, RESPONSE_BUF_SIZE, val, vlen);
        h->stats.requests_ok++;
    } else if (err == LYGUS_ERR_KEY_NOT_FOUND) {
        n = protocol_fmt_not_found(h->resp_buf, RESPONSE_BUF_SIZE);
        h->stats.requests_ok++;
    } else if (err == LYGUS_ERR_TIMEOUT) {
        n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "timeout");
        h->stats.requests_timeout++;
    } else {
        n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, lygus_strerror(err));
        h->stats.requests_error++;
    }

    if (n > 0) {
        conn_send(conn, h->resp_buf, (size_t)n);
    }
}

// ============================================================================
// Request Handling
// ============================================================================

static void handle_status(handler_t *h, conn_t *conn) {
    uint64_t term = raft_get_term(h->raft);
    uint64_t index = storage_mgr_applied_index(h->storage);

    int n;
    if (raft_is_leader(h->raft)) {
        n = protocol_fmt_leader(h->resp_buf, RESPONSE_BUF_SIZE, term, index);
    } else {
        int leader = raft_get_leader_id(h->raft);
        n = protocol_fmt_follower(h->resp_buf, RESPONSE_BUF_SIZE, leader, term);
    }

    if (n > 0) {
        conn_send(conn, h->resp_buf, (size_t)n);
    }
}

static void handle_get(handler_t *h, conn_t *conn, const request_t *req) {
    h->stats.reads_total++;

    // Leader-only mode
    if (h->leader_only && !raft_is_leader(h->raft)) {
        int leader = raft_get_leader_id(h->raft);
        int n;
        if (leader >= 0) {
            n = protocol_fmt_errorf(h->resp_buf, RESPONSE_BUF_SIZE,
                                    "not leader, try node %d", leader);
        } else {
            n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                   "not leader, leader unknown");
        }
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    // Get current time for deadline calculation
    uint64_t now_ms = event_loop_now_ms(h->loop);

    // Use ALR for linearizable reads
    lygus_err_t err = alr_read(h->alr, req->key, req->klen, conn, now_ms);

    if (err == LYGUS_OK) {
        // Response will come via alr callback
        return;
    }

    // ALR failed! respond with error
    int n;
    if (err == LYGUS_ERR_TRY_LEADER) {
        int leader = raft_get_leader_id(h->raft);
        if (leader >= 0) {
            n = protocol_fmt_errorf(h->resp_buf, RESPONSE_BUF_SIZE,
                                    "not leader, try node %d", leader);
        } else {
            n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                   "not leader, leader unknown");
        }
    } else if (err == LYGUS_ERR_BATCH_FULL) {
        n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "read queue full");
    } else {
        n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, lygus_strerror(err));
    }

    if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
    h->stats.requests_error++;
}

static void handle_put(handler_t *h, conn_t *conn, const request_t *req) {
    h->stats.writes_total++;

    // Check leadership
    if (!raft_is_leader(h->raft)) {
        int leader = raft_get_leader_id(h->raft);
        int n;
        if (leader >= 0) {
            n = protocol_fmt_errorf(h->resp_buf, RESPONSE_BUF_SIZE,
                                    "not leader, try node %d", leader);
        } else {
            n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                   "not leader, leader unknown");
        }
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    // Serialize entry
    ssize_t elen = glue_serialize_put(h->entry_buf, ENTRY_BUF_SIZE,
                                      req->key, req->klen,
                                      req->value, req->vlen);
    if (elen < 0) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "serialize failed");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    // Propose to Raft
    // Note: raft_propose returns 0 on success. We need to get the index
    // from the log after propose.
    int ret = raft_propose(h->raft, h->entry_buf, (size_t)elen);
    if (ret != 0) {
        int n = protocol_fmt_errorf(h->resp_buf, RESPONSE_BUF_SIZE,
                                    "propose failed: %d", ret);
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    // Get the index that was just appended
    uint64_t index = glue_log_last_index(h->glue_ctx);

    // Track pending request
    uint64_t now = event_loop_now_ms(h->loop);
    uint64_t deadline = now + h->timeout_ms;
    uint64_t term = raft_get_term(h->raft);

    if (pending_add(h->pending, index, term, deadline, conn, 0) < 0) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "too many pending");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    // Response will come via pending callback when Raft commits
}

static void handle_del(handler_t *h, conn_t *conn, const request_t *req) {
    h->stats.writes_total++;

    // Check leadership
    if (!raft_is_leader(h->raft)) {
        int leader = raft_get_leader_id(h->raft);
        int n;
        if (leader >= 0) {
            n = protocol_fmt_errorf(h->resp_buf, RESPONSE_BUF_SIZE,
                                    "not leader, try node %d", leader);
        } else {
            n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                   "not leader, leader unknown");
        }
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    // Serialize entry
    ssize_t elen = glue_serialize_del(h->entry_buf, ENTRY_BUF_SIZE,
                                      req->key, req->klen);
    if (elen < 0) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "serialize failed");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    // Propose to Raft
    int ret = raft_propose(h->raft, h->entry_buf, (size_t)elen);
    if (ret != 0) {
        int n = protocol_fmt_errorf(h->resp_buf, RESPONSE_BUF_SIZE,
                                    "propose failed: %d", ret);
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    // Get the index that was just appended
    uint64_t index = glue_log_last_index(h->glue_ctx);

    // Track pending
    uint64_t now = event_loop_now_ms(h->loop);
    uint64_t deadline = now + h->timeout_ms;
    uint64_t term = raft_get_term(h->raft);

    if (pending_add(h->pending, index, term, deadline, conn, 0) < 0) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "too many pending");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }
}

void handler_process(handler_t *h, conn_t *conn, const char *line, size_t len) {
    if (!h || !conn || !line) return;

    h->stats.requests_total++;

    request_t req;
    int err = protocol_parse(h->proto, line, len, &req);

    if (err != 0) {
        int n = protocol_fmt_errorf(h->resp_buf, RESPONSE_BUF_SIZE,
                                    "%s", protocol_parse_strerror(err));
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    switch (req.type) {
        case REQ_STATUS:
            handle_status(h, conn);
            h->stats.requests_ok++;
            break;

        case REQ_PING: {
            int n = protocol_fmt_pong(h->resp_buf, RESPONSE_BUF_SIZE);
            if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
            h->stats.requests_ok++;
            break;
        }

        case REQ_VERSION: {
            int n = protocol_fmt_version(h->resp_buf, RESPONSE_BUF_SIZE, h->version);
            if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
            h->stats.requests_ok++;
            break;
        }

        case REQ_GET:
            handle_get(h, conn, &req);
            break;

        case REQ_PUT:
            handle_put(h, conn, &req);
            break;

        case REQ_DEL:
            handle_del(h, conn, &req);
            break;

        default: {
            int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "unknown command");
            if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
            h->stats.requests_error++;
            break;
        }
    }
}

// ============================================================================
// Event Hooks
// ============================================================================

void handler_on_commit(handler_t *h, uint64_t index, uint64_t term) {
    (void)term;
    if (!h) return;
    pending_complete(h->pending, index);
}

void handler_on_apply(handler_t *h, uint64_t last_applied) {
    if (!h) return;
    alr_notify(h->alr, last_applied);
}

void handler_on_leadership_change(handler_t *h, bool is_leader) {
    if (!h) return;

    if (!is_leader) {
        // FIX: Use correct error code so clients know to redirect
        pending_fail_all(h->pending, LYGUS_ERR_NOT_LEADER);

        // FIX: Also invalidate ALR state - leadership change means term changed
        alr_on_term_change(h->alr, raft_get_term(h->raft));
    }
}

void handler_on_term_change(handler_t *h, uint64_t new_term) {
    if (!h) return;
    alr_on_term_change(h->alr, new_term);
}

void handler_on_log_truncate(handler_t *h, uint64_t from_index) {
    if (!h) return;

    pending_fail_from(h->pending, from_index, LYGUS_ERR_LOG_MISMATCH);
}

void handler_on_conn_close(handler_t *h, conn_t *conn) {
    if (!h || !conn) return;

    pending_fail_conn(h->pending, conn, LYGUS_ERR_NET);
    alr_cancel_conn(h->alr, conn);
}

void handler_tick(handler_t *h, uint64_t now_ms) {
    if (!h) return;

    pending_timeout_sweep(h->pending, now_ms);

    // FIX: Also sweep timed-out reads
    alr_timeout_sweep(h->alr, now_ms);
}

void handler_on_readindex_complete(handler_t *h, uint64_t req_id,
                                    uint64_t read_index, int err) {
    if (!h || !h->alr) return;
    alr_on_read_index(h->alr, req_id, read_index, err);
}

// ============================================================================
// Stats
// ============================================================================

void handler_get_stats(const handler_t *h, handler_stats_t *out) {
    if (!h || !out) return;

    *out = h->stats;
    out->writes_pending = pending_count(h->pending);

    alr_stats_t alr_stats;
    alr_get_stats(h->alr, &alr_stats);
    out->reads_pending = alr_stats.pending_count;
}

size_t handler_pending_count(const handler_t *h) {
    if (!h) return 0;
    return pending_count(h->pending);
}