/**
 * handler.c - Request handling with PRDT Merkle DAG + Raft integration
 *
 * Write path (leader):
 *   PUT/DEL → dag_add → fire-and-forget push to all peers
 *           → wait for one peer MSG_DAG_PUSH_FF_ACK
 *           → OK to client (bilateral: leader + confirming peer)
 *
 * Write path (follower):
 *   PUT/DEL → dag_add → confirmed push to leader + ff push to others
 *           → wait for leader MSG_DAG_PUSH_ACK
 *           → OK to client (bilateral: follower + leader)
 *
 * Read path (leader — FRONTIER):
 *   GET → send MSG_QUORUM_PING to all peers
 *       → wait for majority MSG_QUORUM_PING_ACK
 *       → dag_get_latest(key) → serve from DAG
 *       → zero Raft, zero drain, zero disk I/O
 *
 * Apply path (background GC only):
 *   Periodic drain → raft_propose batch → commit → apply to KV
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
#include "public/lygus_errors.h"
#include "network/network.h"
#include "network/wire_format.h"

// DAG modules
#include "merkle_dag.h"
#include "dag_key_index.h"
#include "dag_serial.h"
#include "gossip.h"
#include "dag_entry.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// ============================================================================
// Defaults
// ============================================================================

#define DEFAULT_MAX_PENDING      1024
#define DEFAULT_TIMEOUT_MS       5000
#define DEFAULT_MAX_KEY          1024
#define DEFAULT_MAX_VALUE        (1024 * 1024)

#define RESPONSE_BUF_SIZE        (2 * 1024 * 1024)
#define ENTRY_BUF_SIZE           (1024 * 1024 + 1024)

#define DEFAULT_DAG_MAX_NODES    65536
#define DEFAULT_DAG_ARENA_SIZE   (16 * 1024 * 1024)
#define DEFAULT_BATCH_BUF_SIZE   (8 * 1024 * 1024)

#define MAX_TIP_PARENTS          8
#define MAX_PENDING_PUSHES       4096
#define DRAIN_INTERVAL_TICKS     10

// Frontier read path
#define MAX_PENDING_FRONTIER_READS  2048
#define FRONTIER_MAX_KEY_SIZE       1024

// DAG sync (leader election)
#define MAX_SYNC_PEERS              16
#define SYNC_PEER_HASH_INIT_CAP     4096

// ============================================================================
// Confirmed Push - pending write awaiting acknowledgement
// ============================================================================

typedef struct {
    uint64_t    seq;
    uint8_t     hash[DAG_HASH_SIZE];
    conn_t     *conn;
    uint64_t    deadline_ms;
    bool        active;
    bool        is_leader_write;
} pending_push_t;

// ============================================================================
// Frontier Read - pending read awaiting quorum ping
// ============================================================================

typedef struct {
    conn_t     *conn;
    uint8_t     key[FRONTIER_MAX_KEY_SIZE];
    size_t      key_len;
    uint64_t    deadline_ms;
    bool        active;
} pending_frontier_read_t;

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
    network_t       *net;

    // Owned components
    protocol_ctx_t  *proto;
    pending_table_t *pending;

    // DAG (owned)
    merkle_dag_t    *dag;
    gossip_t        *gossip;
    merkle_dag_t    *apply_dag;

    // Config
    int              node_id;
    int              num_peers;
    uint32_t         timeout_ms;
    const char      *version;
    bool             leader_only;

    // Scratch buffers
    char            *resp_buf;
    uint8_t         *entry_buf;
    uint8_t         *batch_buf;
    size_t           batch_buf_cap;
    uint8_t         *node_push_buf;
    size_t           node_push_cap;

    // DAG commit state
    bool             dag_msync_enabled;

    // Confirmed push state
    pending_push_t   pushes[MAX_PENDING_PUSHES];
    uint64_t         next_push_seq;

    // Background drain tick counter
    uint64_t         drain_tick_count;
    uint64_t         leader_seq_counter; // Per-term monotonic counter (§4.2)

    // ---- Frontier read state ----
    pending_frontier_read_t frontier_reads[MAX_PENDING_FRONTIER_READS];
    bool             qping_active;      // Is a quorum ping in flight?
    uint64_t         qping_term;        // Term of current ping
    int              qping_acks;        // ACKs received (including self)
    uint64_t         qping_deadline_ms; // Timeout for current ping

    // ---- DAG sync on election (§6.2) ----
    bool             leader_ready;      // false during sync, gates reads
    bool             sync_active;       // sync in progress
    uint64_t         sync_term;         // term of current sync
    uint64_t         sync_deadline_ms;  // timeout for sync
    int              sync_responses;    // peers that responded
    bool             sync_peer_responded[MAX_SYNC_PEERS];
    uint8_t         *sync_peer_hashes;  // flat array of hashes from peers (Bug 3)
    size_t           sync_peer_hash_count;
    size_t           sync_peer_hash_cap;

    // Stats
    handler_stats_t  stats;
};

// ============================================================================
// Forward Declarations
// ============================================================================

static void on_pending_complete(const pending_entry_t *entry, int err, void *ctx);
static void gossip_send_cb(void *ctx, int to_peer, uint8_t msg_type,
                           const uint8_t *data, size_t len);
static int gossip_pick_peer_cb(void *ctx);
static void apply_node_to_kv(dag_node_t *node, void *ctx);
static uint64_t propose_dag_batch(handler_t *h);
static void push_node_to_peers(handler_t *h, dag_node_t *node);
static void push_node_confirmed(handler_t *h, conn_t *conn, dag_node_t *node);

// Frontier read helpers
static void frontier_start_qping(handler_t *h);
static void frontier_serve_all(handler_t *h);
static void frontier_fail_all(handler_t *h, const char *reason);
static void frontier_serve_one(handler_t *h, pending_frontier_read_t *r);

// DAG sync helpers (leader election)
static void sync_start(handler_t *h);
static void sync_abort(handler_t *h);
static void sync_handle_response(handler_t *h, int from_peer,
                                  const uint8_t *data, size_t len);
static void sync_complete(handler_t *h);
static bool sync_peer_has_hash(handler_t *h, const uint8_t *hash);
static void sync_add_peer_hash(handler_t *h, const uint8_t *hash);

// ============================================================================
// Confirmed Push Helpers
// ============================================================================

static pending_push_t *push_alloc(handler_t *h) {
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (!h->pushes[i].active) return &h->pushes[i];
    }
    return NULL;
}

static pending_push_t *push_find(handler_t *h, uint64_t seq) {
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active && !h->pushes[i].is_leader_write
            && h->pushes[i].seq == seq)
            return &h->pushes[i];
    }
    return NULL;
}

static pending_push_t *push_find_by_hash(handler_t *h, const uint8_t *hash) {
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active && h->pushes[i].is_leader_write
            && memcmp(h->pushes[i].hash, hash, DAG_HASH_SIZE) == 0)
            return &h->pushes[i];
    }
    return NULL;
}

static void push_remove(pending_push_t *pp) {
    pp->active = false;
    pp->conn = NULL;
    pp->seq = 0;
    pp->is_leader_write = false;
    memset(pp->hash, 0, DAG_HASH_SIZE);
}

// ============================================================================
// Frontier Read Helpers
// ============================================================================

/**
 * Check if a DAG node is an unconfirmed leader write.
 *
 * These are writes the leader inserted locally but no peer has ACK'd
 * yet.  The TLA+ spec excludes leaderPendingAck from safeWrites —
 * a read must not observe a write that might vanish if the leader
 * crashes before any peer receives it.
 */
static bool is_pending_leader_write(handler_t *h, const uint8_t *hash) {
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active && h->pushes[i].is_leader_write
            && memcmp(h->pushes[i].hash, hash, DAG_HASH_SIZE) == 0)
            return true;
    }
    return false;
}

/**
 * Find the latest confirmed DAG node for a key, skipping any
 * unconfirmed leader writes.
 *
 * Fast path: dag_get_latest is O(1).  This O(n) scan only runs
 * when the winner is pending — rare, and DAG size between drains
 * is bounded by the 200-node batch cap.
 */
static dag_node_t *find_latest_safe(handler_t *h,
                                     const uint8_t *key, size_t key_len) {
    dag_node_t *best = NULL;
    dag_node_t *node, *tmp;
    HASH_ITER(hh, h->dag->nodes, node, tmp) {
        if (node->key_len != key_len) continue;
        if (memcmp(node->key, key, key_len) != 0) continue;
        if (is_pending_leader_write(h, node->hash)) continue;
        if (!best || dag_node_wins(node, best)) {
            best = node;
        }
    }
    return best;
}

static pending_frontier_read_t *frontier_alloc(handler_t *h) {
    for (int i = 0; i < MAX_PENDING_FRONTIER_READS; i++) {
        if (!h->frontier_reads[i].active) return &h->frontier_reads[i];
    }
    return NULL;
}

/**
 * Start a quorum ping: send MSG_QUORUM_PING [term:8] to all peers.
 * Self counts as 1 ACK.  Majority triggers frontier_serve_all.
 */
static void frontier_start_qping(handler_t *h) {
    uint64_t term = raft_get_term(h->raft);
    uint64_t now = event_loop_now_ms(h->loop);

    h->qping_active = true;
    h->qping_term = term;
    h->qping_acks = 1;  // Self counts
    h->qping_deadline_ms = now + h->timeout_ms;

    // Send to all peers
    for (int i = 0; i < h->num_peers; i++) {
        if (i == h->node_id) continue;
        network_send_raft(h->net, i, MSG_QUORUM_PING,
                          (const uint8_t *)&term, 8);
    }
}

/**
 * Serve a single pending read from the leader's DAG.
 *
 * Lookup order:
 *   1. O(1) key index (dag_get_latest)
 *   2. If winner is an unconfirmed leader write → O(n) safe scan
 *   3. If nothing in DAG → fall back to KV store (drained values)
 */
static void frontier_serve_one(handler_t *h, pending_frontier_read_t *r) {
    if (!r->active) return;

    dag_node_t *node = dag_get_latest(h->dag, r->key, r->key_len);

    // Exclude unconfirmed leader writes (TLA+ safeWrites filter)
    if (node && is_pending_leader_write(h, node->hash)) {
        node = find_latest_safe(h, r->key, r->key_len);
    }

    int n;
    if (node) {
        // Serve from DAG — value[0] is op tag
        if (node->value[0] == DAG_OP_DEL) {
            n = protocol_fmt_not_found(h->resp_buf, RESPONSE_BUF_SIZE);
        } else if (node->value[0] == DAG_OP_PUT && node->value_len > 1) {
            n = protocol_fmt_value(h->resp_buf, RESPONSE_BUF_SIZE,
                                   node->value + 1, node->value_len - 1);
        } else {
            n = protocol_fmt_not_found(h->resp_buf, RESPONSE_BUF_SIZE);
        }
    } else {
        // Nothing in DAG (or only pending writes).
        // Fall back to KV store which has all background-drained values.
        uint8_t val_buf[DEFAULT_MAX_VALUE];
        size_t vlen = lygus_kv_get(h->kv, r->key, r->key_len,
                                    val_buf, sizeof(val_buf));
        if (vlen > 0 && vlen < sizeof(val_buf)) {
            n = protocol_fmt_value(h->resp_buf, RESPONSE_BUF_SIZE, val_buf, vlen);
        } else {
            n = protocol_fmt_not_found(h->resp_buf, RESPONSE_BUF_SIZE);
        }
    }

    if (n > 0 && r->conn) {
        conn_send(r->conn, h->resp_buf, (size_t)n);
    }
    h->stats.requests_ok++;

    r->active = false;
    r->conn = NULL;
}

/**
 * Quorum ping succeeded — serve all queued reads and reset.
 */
static void frontier_serve_all(handler_t *h) {
    for (int i = 0; i < MAX_PENDING_FRONTIER_READS; i++) {
        if (h->frontier_reads[i].active) {
            frontier_serve_one(h, &h->frontier_reads[i]);
        }
    }
    h->qping_active = false;
    h->qping_acks = 0;
}

/**
 * Fail all pending frontier reads with an error message.
 */
static void frontier_fail_all(handler_t *h, const char *reason) {
    for (int i = 0; i < MAX_PENDING_FRONTIER_READS; i++) {
        pending_frontier_read_t *r = &h->frontier_reads[i];
        if (!r->active) continue;
        if (r->conn) {
            int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, reason);
            if (n > 0) conn_send(r->conn, h->resp_buf, (size_t)n);
            h->stats.requests_error++;
        }
        r->active = false;
        r->conn = NULL;
    }
    h->qping_active = false;
    h->qping_acks = 0;
}

// ============================================================================
// DAG Sync on Election (§6.2)
// ============================================================================

/**
 * Track a hash as peer-confirmed (for Bug 3 guard).
 */
static void sync_add_peer_hash(handler_t *h, const uint8_t *hash) {
    if (h->sync_peer_hash_count >= h->sync_peer_hash_cap) {
        size_t new_cap = h->sync_peer_hash_cap * 2;
        if (new_cap == 0) new_cap = SYNC_PEER_HASH_INIT_CAP;
        uint8_t *nb = realloc(h->sync_peer_hashes, new_cap * DAG_HASH_SIZE);
        if (!nb) return;
        h->sync_peer_hashes = nb;
        h->sync_peer_hash_cap = new_cap;
    }
    memcpy(h->sync_peer_hashes + h->sync_peer_hash_count * DAG_HASH_SIZE,
           hash, DAG_HASH_SIZE);
    h->sync_peer_hash_count++;
}

/**
 * Check if a hash was seen in any peer's sync response.
 */
static bool sync_peer_has_hash(handler_t *h, const uint8_t *hash) {
    for (size_t i = 0; i < h->sync_peer_hash_count; i++) {
        if (memcmp(h->sync_peer_hashes + i * DAG_HASH_SIZE,
                   hash, DAG_HASH_SIZE) == 0)
            return true;
    }
    return false;
}

/**
 * Start DAG sync: send MSG_DAG_SYNC_REQ to all peers.
 *
 * Called on leadership change. Sets leader_ready = false,
 * blocking reads until sync completes.
 */
static void sync_start(handler_t *h) {
    h->leader_ready = false;
    h->sync_active = true;
    h->sync_term = raft_get_term(h->raft);
    h->sync_deadline_ms = event_loop_now_ms(h->loop) + h->timeout_ms;
    h->sync_responses = 0;
    h->leader_seq_counter = 0;

    memset(h->sync_peer_responded, 0, sizeof(h->sync_peer_responded));

    // Reset peer hash tracking
    h->sync_peer_hash_count = 0;
    // (keep allocated buffer for reuse)

    // Single-node cluster: self is majority, complete immediately
    int majority = (h->num_peers / 2) + 1;
    if (majority <= 1) {
        sync_complete(h);
        return;
    }

    // Send sync request to all peers
    uint64_t term = h->sync_term;
    for (int i = 0; i < h->num_peers; i++) {
        if (i == h->node_id) continue;
        network_send_raft(h->net, i, MSG_DAG_SYNC_REQ,
                          (const uint8_t *)&term, 8);
    }
}

/**
 * Abort in-progress sync (term change or leadership loss).
 */
static void sync_abort(handler_t *h) {
    h->sync_active = false;
    h->leader_ready = false;
    h->sync_responses = 0;
}

/**
 * Handle a peer's sync response: merge their DAG into ours,
 * track peer-confirmed hashes.
 */
static void sync_handle_response(handler_t *h, int from_peer,
                                  const uint8_t *data, size_t len) {
    if (!h->sync_active) return;
    if (len < 8) return;

    // Validate term
    uint64_t resp_term;
    memcpy(&resp_term, data, 8);
    if (resp_term != h->sync_term) return;

    // Don't double-count
    if (from_peer >= 0 && from_peer < MAX_SYNC_PEERS
        && h->sync_peer_responded[from_peer])
        return;

    const uint8_t *batch_data = data + 8;
    size_t batch_len = len - 8;

    // Deserialize peer's DAG nodes one at a time so we can track hashes.
    // Batch format: [count:4][node1][node2]...
    if (batch_len < 4) goto count_response;

    uint32_t count;
    memcpy(&count, batch_data, 4);
    size_t offset = 4;

    for (uint32_t i = 0; i < count && offset < batch_len; i++) {
        size_t consumed = 0;
        dag_node_t *node = dag_node_deserialize(h->dag,
                                                 batch_data + offset,
                                                 batch_len - offset,
                                                 &consumed);
        if (!node) break;

        // Track this hash as peer-confirmed
        sync_add_peer_hash(h, node->hash);
        offset += consumed;
    }

count_response:
    if (from_peer >= 0 && from_peer < MAX_SYNC_PEERS) {
        h->sync_peer_responded[from_peer] = true;
    }
    h->sync_responses++;

    // Check majority (self + responding peers)
    int majority = (h->num_peers / 2) + 1;
    int total = 1 + h->sync_responses;  // +1 for self
    if (total >= majority) {
        sync_complete(h);
    }
}


static void sync_complete(handler_t *h) {
    uint64_t term = raft_get_term(h->raft);

    // Reconstruct leader_seq_counter from max existing leader_seq.
    uint64_t max_counter = h->leader_seq_counter;
    dag_node_t *node, *tmp;
    HASH_ITER(hh, h->dag->nodes, node, tmp) {
        if (node->leader_seq > 0) {
            uint64_t counter = node->leader_seq & 0xFFFFFFFF;
            if (counter > max_counter) max_counter = counter;
        }
    }
    h->leader_seq_counter = max_counter;

    // Assign leader_seq to peer-confirmed unordered writes (§6.2 step 6).
    // Iterate all nodes in DAG. Nodes with leader_seq == 0 that appear
    // in peer responses are peer-confirmed → assign sequence.
    // Nodes with leader_seq == 0 that are local-only → skip (Bug 3).
    HASH_ITER(hh, h->dag->nodes, node, tmp) {
        if (node->leader_seq == 0 && sync_peer_has_hash(h, node->hash)) {
            node->leader_seq = (term << 32) | (++h->leader_seq_counter);
        }
    }

    // Rebuild key index from scratch (sequences changed)
    key_index_clear(h->dag);
    HASH_ITER(hh, h->dag->nodes, node, tmp) {
        if (node->leader_seq > 0) {
            key_index_update(h->dag, node);
        }
    }

    // Msync the arena if durable mode is on (sequences are in DAG node metadata)
    if (h->dag_msync_enabled && h->dag->arena) {
        dag_msync(h->dag, 0, arena_used(h->dag->arena));
    }

    h->sync_active = false;
    h->leader_ready = true;
}

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
    h->net = cfg->net;
    h->node_id = cfg->node_id;
    h->num_peers = cfg->num_peers;
    h->version = cfg->version ? cfg->version : "unknown";
    h->timeout_ms = cfg->request_timeout_ms > 0 ? cfg->request_timeout_ms : DEFAULT_TIMEOUT_MS;
    h->leader_only = cfg->leader_only_reads;
    h->dag_msync_enabled = cfg->dag_msync_enabled;

    size_t max_key = cfg->max_key_size > 0 ? cfg->max_key_size : DEFAULT_MAX_KEY;
    size_t max_val = cfg->max_value_size > 0 ? cfg->max_value_size : DEFAULT_MAX_VALUE;
    h->proto = protocol_ctx_create(max_key, max_val);
    if (!h->proto) goto fail;

    size_t max_pending = cfg->max_pending > 0 ? cfg->max_pending : DEFAULT_MAX_PENDING;
    h->pending = pending_create(max_pending, on_pending_complete, h);
    if (!h->pending) goto fail;

    // DAG setup
    size_t dag_max = cfg->dag_max_nodes > 0 ? cfg->dag_max_nodes : DEFAULT_DAG_MAX_NODES;
    size_t dag_arena = cfg->dag_arena_size > 0 ? cfg->dag_arena_size : DEFAULT_DAG_ARENA_SIZE;

    h->dag = dag_create(dag_max, dag_arena);
    if (!h->dag) goto fail;

    h->apply_dag = dag_create(dag_max, dag_arena);
    if (!h->apply_dag) goto fail;

    gossip_config_t gossip_cfg = {
        .node_id = cfg->node_id,
        .dag = h->dag,
        .pick_peer = gossip_pick_peer_cb,
        .pick_peer_ctx = h,
    };
    h->gossip = gossip_create(&gossip_cfg);
    if (!h->gossip) goto fail;

    h->resp_buf = malloc(RESPONSE_BUF_SIZE);
    h->entry_buf = malloc(ENTRY_BUF_SIZE);
    h->batch_buf_cap = cfg->batch_buf_size > 0 ? cfg->batch_buf_size : DEFAULT_BATCH_BUF_SIZE;
    h->batch_buf = malloc(h->batch_buf_cap);
    h->node_push_cap = 4096;
    h->node_push_buf = malloc(h->node_push_cap);

    if (!h->resp_buf || !h->entry_buf || !h->batch_buf || !h->node_push_buf) goto fail;

    // DAG sync state
    h->leader_ready = false;
    h->sync_active = false;
    h->sync_peer_hashes = NULL;
    h->sync_peer_hash_count = 0;
    h->sync_peer_hash_cap = 0;

    return h;

fail:
    handler_destroy(h);
    return NULL;
}

void handler_destroy(handler_t *h) {
    if (!h) return;

    if (h->pending) {
        pending_fail_all(h->pending, LYGUS_ERR_INTERNAL);
        pending_destroy(h->pending);
    }

    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active) push_remove(&h->pushes[i]);
    }

    // Clean up frontier reads (no response — we're shutting down)
    for (int i = 0; i < MAX_PENDING_FRONTIER_READS; i++) {
        h->frontier_reads[i].active = false;
    }

    if (h->proto) protocol_ctx_destroy(h->proto);
    if (h->gossip) gossip_destroy(h->gossip);
    if (h->dag) dag_destroy(h->dag);
    if (h->apply_dag) dag_destroy(h->apply_dag);

    free(h->resp_buf);
    free(h->entry_buf);
    free(h->batch_buf);
    free(h->node_push_buf);
    free(h->sync_peer_hashes);
    free(h);
}

// ============================================================================
// Gossip Callbacks
// ============================================================================

static void gossip_send_cb(void *ctx, int to_peer, uint8_t msg_type,
                           const uint8_t *data, size_t len) {
    handler_t *h = (handler_t *)ctx;
    if (!h->net) return;
    network_send_raft(h->net, to_peer, msg_type, data, len);
}

static int gossip_pick_peer_cb(void *ctx) {
    handler_t *h = (handler_t *)ctx;
    if (h->num_peers <= 1) return -1;
    int peer;
    do { peer = rand() % h->num_peers; } while (peer == h->node_id);
    return peer;
}

// ============================================================================
// Callbacks
// ============================================================================

static void on_pending_complete(const pending_entry_t *entry, int err, void *ctx) {
    // Pending tracks background drain proposals only — no client response.
    (void)entry; (void)err; (void)ctx;
}

// ============================================================================
// DAG Apply Path
// ============================================================================

static void apply_node_to_kv(dag_node_t *node, void *ctx) {
    handler_t *h = (handler_t *)ctx;
    if (!node || node->value_len == 0) return;

    uint8_t op = node->value[0];
    if (op == DAG_OP_PUT && node->value_len > 1) {
        lygus_kv_put(h->kv, node->key, node->key_len,
                     node->value + 1, node->value_len - 1);
    } else if (op == DAG_OP_DEL) {
        lygus_kv_del(h->kv, node->key, node->key_len);
    }
}

int handler_apply_dag_batch(handler_t *h, const uint8_t *entry, size_t len) {
    if (!h || !entry || !dag_entry_is_batch(entry, len)) return -1;

    const uint8_t *payload = dag_entry_batch_payload(entry);
    size_t payload_len = dag_entry_batch_len(len);
    if (payload_len < 4) return -1;

    dag_reset(h->apply_dag);
    int count = dag_deserialize_batch(h->apply_dag, payload, payload_len);
    if (count < 0) return -1;

    size_t cc = dag_count(h->apply_dag);
    uint8_t *ch = NULL;
    if (cc > 0) {
        ch = malloc(cc * DAG_HASH_SIZE);
        if (ch) cc = dag_collect_hashes(h->apply_dag, ch, cc);
        else cc = 0;
    }

    dag_iter_topo(h->apply_dag, apply_node_to_kv, h);
    dag_reset(h->apply_dag);

    if (cc > 0 && ch) {
        dag_remove_by_hashes(h->dag, ch, cc);
    }
    free(ch);

    h->stats.dag_batches_applied++;
    return count;
}

// ============================================================================
// DAG Batch Proposal (background drain only — NEVER on read path)
// ============================================================================

static uint64_t propose_dag_batch(handler_t *h) {
    size_t count = dag_count(h->dag);
    if (count == 0) return 0;

    uint8_t *excl = NULL;
    size_t ec = 0;

    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active && h->pushes[i].is_leader_write) ec++;
    }

    if (ec > 0) {
        excl = malloc(ec * DAG_HASH_SIZE);
        if (excl) {
            size_t idx = 0;
            for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
                if (h->pushes[i].active && h->pushes[i].is_leader_write) {
                    memcpy(excl + (idx * DAG_HASH_SIZE),
                           h->pushes[i].hash, DAG_HASH_SIZE);
                    idx++;
                }
            }
        } else {
            return 0;
        }
    }

    h->batch_buf[0] = DAG_ENTRY_MARKER;
    ssize_t bl = dag_serialize_batch_excluding(
        h->dag, h->batch_buf + 1, h->batch_buf_cap - 1, excl, ec, 200);

    if (bl < 0) { free(excl); return 0; }

    uint32_t bc;
    memcpy(&bc, h->batch_buf + 1, 4);
    if (bc == 0) { free(excl); return 0; }

    size_t el = 1 + (size_t)bl;
    int ret = raft_propose(h->raft, h->batch_buf, el);
    if (ret != 0) { free(excl); return 0; }

    // Remove proposed nodes from live DAG
    dag_reset(h->apply_dag);
    int proposed = dag_deserialize_batch(h->apply_dag,
                                          h->batch_buf + 1, (size_t)bl);
    if (proposed > 0) {
        size_t pc = dag_count(h->apply_dag);
        uint8_t *ph = malloc(pc * DAG_HASH_SIZE);
        if (ph) {
            pc = dag_collect_hashes(h->apply_dag, ph, pc);
            dag_remove_by_hashes(h->dag, ph, pc);
            free(ph);
        }
    }
    dag_reset(h->apply_dag);

    free(excl);
    h->stats.dag_batches_proposed++;
    return raft_get_pending_index(h->raft);
}

// ============================================================================
// Push-on-write Gossip
// ============================================================================

static void push_node_to_peers(handler_t *h, dag_node_t *node) {
    if (!h->net || !node) return;

    ssize_t needed = dag_node_serialized_size(node);
    if (needed <= 0) return;

    if ((size_t)needed > h->node_push_cap) {
        size_t nc = (size_t)needed * 2;
        uint8_t *nb = realloc(h->node_push_buf, nc);
        if (!nb) return;
        h->node_push_buf = nb;
        h->node_push_cap = nc;
    }

    ssize_t wrote = dag_node_serialize(node, h->node_push_buf, h->node_push_cap);
    if (wrote <= 0) return;

    for (int i = 0; i < h->num_peers; i++) {
        if (i == h->node_id) continue;
        network_send_raft(h->net, i, MSG_DAG_PUSH,
                          h->node_push_buf, (size_t)wrote);
    }
    h->stats.dag_nodes_gossiped++;
}

static void push_node_to_peers_confirmed(handler_t *h, conn_t *conn, dag_node_t *node) {
    if (!h->net || !node) goto fail;
    if (h->num_peers <= 1) {
        int n = protocol_fmt_ok(h->resp_buf, RESPONSE_BUF_SIZE);
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_ok++;
        return;
    }

    pending_push_t *pp = push_alloc(h);
    if (!pp) goto fail;

    push_node_to_peers(h, node);

    uint64_t now = event_loop_now_ms(h->loop);
    memcpy(pp->hash, node->hash, DAG_HASH_SIZE);
    pp->seq = 0;
    pp->conn = conn;
    pp->deadline_ms = now + h->timeout_ms;
    pp->active = true;
    pp->is_leader_write = true;
    return;

fail:
    {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                   "confirmed push failed");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
    }
}

static void push_node_confirmed(handler_t *h, conn_t *conn, dag_node_t *node) {
    if (!h->net || !node) goto fail;

    int leader = raft_get_leader_id(h->raft);
    if (leader < 0) goto fail;

    pending_push_t *pp = push_alloc(h);
    if (!pp) goto fail;

    ssize_t needed = dag_node_serialized_size(node);
    if (needed <= 0) goto fail;

    size_t total = 8 + (size_t)needed;
    if (total > h->node_push_cap) {
        size_t nc = total * 2;
        uint8_t *nb = realloc(h->node_push_buf, nc);
        if (!nb) goto fail;
        h->node_push_buf = nb;
        h->node_push_cap = nc;
    }

    uint64_t seq = h->next_push_seq++;
    memcpy(h->node_push_buf, &seq, 8);

    ssize_t wrote = dag_node_serialize(node, h->node_push_buf + 8,
                                        h->node_push_cap - 8);
    if (wrote <= 0) goto fail;

    network_send_raft(h->net, leader, MSG_DAG_PUSH_CONFIRMED,
                      h->node_push_buf, 8 + (size_t)wrote);

    for (int i = 0; i < h->num_peers; i++) {
        if (i == h->node_id || i == leader) continue;
        network_send_raft(h->net, i, MSG_DAG_PUSH,
                          h->node_push_buf + 8, (size_t)wrote);
    }

    uint64_t now = event_loop_now_ms(h->loop);
    pp->seq = seq;
    pp->conn = conn;
    pp->deadline_ms = now + h->timeout_ms;
    pp->active = true;

    h->stats.dag_nodes_gossiped++;
    return;

fail:
    {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                   "confirmed push failed");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
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

    if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
}

/**
 * GET handler — FRONTIER read path.
 *
 * 1. Verify leadership (redirect if not leader)
 * 2. Stash conn + key in pending frontier reads
 * 3. If no quorum ping in flight → start one
 * 4. On majority ACK → frontier_serve_all (see handler_on_gossip)
 */
static void handle_get(handler_t *h, conn_t *conn, const request_t *req) {
    h->stats.reads_total++;

    // Frontier: all reads go through leader
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

    // Block reads until DAG sync completes (§6.2 leaderReady gate)
    if (!h->leader_ready) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                   "leader syncing, retry");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    // Bounds check
    if (req->klen > FRONTIER_MAX_KEY_SIZE) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                   "key too large");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    // Stash the read
    pending_frontier_read_t *r = frontier_alloc(h);
    if (!r) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                   "read queue full");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    r->conn = conn;
    memcpy(r->key, req->key, req->klen);
    r->key_len = req->klen;
    r->deadline_ms = event_loop_now_ms(h->loop) + h->timeout_ms;
    r->active = true;

    // Start quorum ping if not already in flight
    if (!h->qping_active) {
        frontier_start_qping(h);
    }

    // Single-node cluster: self is majority, serve immediately
    int majority = (h->num_peers / 2) + 1;
    if (h->qping_acks >= majority) {
        frontier_serve_all(h);
    }
}

static void handle_put(handler_t *h, conn_t *conn, const request_t *req) {
    h->stats.writes_total++;

    size_t tc = dag_tip_count(h->dag);
    uint8_t tb[MAX_TIP_PARENTS * DAG_HASH_SIZE];
    uint32_t pc = 0;

    if (tc > 0) {
        if (tc > MAX_TIP_PARENTS) tc = MAX_TIP_PARENTS;
        dag_get_tips(h->dag, tb, &tc);
        pc = (uint32_t)tc;
    }

    size_t tvl = 1 + req->vlen;
    uint8_t *tv = malloc(tvl);
    if (!tv) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "alloc failed");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }
    tv[0] = DAG_OP_PUT;
    memcpy(tv + 1, req->value, req->vlen);

    size_t ab = arena_used(h->dag->arena);
    dag_node_t *node = dag_add(h->dag, req->key, req->klen,
                                tv, tvl, pc > 0 ? tb : NULL, pc);
    free(tv);

    if (!node) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "dag full");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    size_t aa = arena_used(h->dag->arena);
    if (h->dag_msync_enabled && aa > ab) {
        dag_msync(h->dag, ab, aa - ab);
    }

    h->stats.dag_inserts++;
    if (raft_is_leader(h->raft)) {
        uint64_t term = raft_get_term(h->raft);
        node->leader_seq = (term << 32) | (++h->leader_seq_counter);
        key_index_update(h->dag, node);
        push_node_to_peers_confirmed(h, conn, node);
    } else {
        push_node_confirmed(h, conn, node);
    }
}

static void handle_del(handler_t *h, conn_t *conn, const request_t *req) {
    h->stats.writes_total++;

    size_t tc = dag_tip_count(h->dag);
    uint8_t tb[MAX_TIP_PARENTS * DAG_HASH_SIZE];
    uint32_t pc = 0;

    if (tc > 0) {
        if (tc > MAX_TIP_PARENTS) tc = MAX_TIP_PARENTS;
        dag_get_tips(h->dag, tb, &tc);
        pc = (uint32_t)tc;
    }

    uint8_t dm = DAG_OP_DEL;
    size_t ab = arena_used(h->dag->arena);
    dag_node_t *node = dag_add(h->dag, req->key, req->klen,
                                &dm, 1, pc > 0 ? tb : NULL, pc);

    if (!node) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "dag full");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    size_t aa = arena_used(h->dag->arena);
    if (h->dag_msync_enabled && aa > ab) {
        dag_msync(h->dag, ab, aa - ab);
    }

    h->stats.dag_inserts++;
    if (raft_is_leader(h->raft)) {
        uint64_t term = raft_get_term(h->raft);
        node->leader_seq = (term << 32) | (++h->leader_seq_counter);
        key_index_update(h->dag, node);
        push_node_to_peers_confirmed(h, conn, node);
    } else {
        push_node_confirmed(h, conn, node);
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
        case REQ_GET:    handle_get(h, conn, &req); break;
        case REQ_PUT:    handle_put(h, conn, &req); break;
        case REQ_DEL:    handle_del(h, conn, &req); break;
        default: {
            int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "unknown command");
            if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
            h->stats.requests_error++;
            break;
        }
    }
}

// ============================================================================
// Gossip + Quorum Ping Handling
// ============================================================================

void handler_on_gossip(handler_t *h, int from_peer, uint8_t msg_type,
                       const uint8_t *data, size_t len) {
    if (!h) return;

    // ---- Quorum Ping (frontier read path) ----

    if (msg_type == MSG_QUORUM_PING) {
        // Peer received leadership check from leader.
        // Respond only if term matches (haven't seen a higher term).
        if (!data || len < 8) return;

        uint64_t ping_term;
        memcpy(&ping_term, data, 8);

        uint64_t my_term = raft_get_term(h->raft);
        if (ping_term == my_term) {
            network_send_raft(h->net, from_peer, MSG_QUORUM_PING_ACK,
                              (const uint8_t *)&ping_term, 8);
        }
        // If ping_term < my_term: don't respond.  Leader's ping
        // will timeout and it will step down.  This is the quorum
        // intersection argument from the TLA+ spec.
        return;
    }

    if (msg_type == MSG_QUORUM_PING_ACK) {
        // Leader received confirmation from a peer.
        if (!data || len < 8) return;
        if (!h->qping_active) return;

        uint64_t ack_term;
        memcpy(&ack_term, data, 8);

        // Only count ACKs for our current ping
        if (ack_term != h->qping_term) return;

        // Verify we're still leader at this term
        if (!raft_is_leader(h->raft)) {
            frontier_fail_all(h, "lost leadership during read");
            return;
        }

        h->qping_acks++;

        int majority = (h->num_peers / 2) + 1;
        if (h->qping_acks >= majority) {
            frontier_serve_all(h);
        }
        return;
    }

    // ---- DAG sync (leader election) ----

    if (msg_type == MSG_DAG_SYNC_REQ) {
        // New leader wants our DAG. Respond with full batch.
        if (!data || len < 8) return;

        uint64_t req_term;
        memcpy(&req_term, data, 8);

        // Only respond if term is current (don't help stale leaders)
        uint64_t my_term = raft_get_term(h->raft);
        if (req_term < my_term) return;

        // Serialize our entire DAG
        ssize_t batch_size = dag_batch_serialized_size(h->dag);
        if (batch_size <= 0) {
            // Empty DAG — send just the term + empty batch [count=0]
            uint8_t resp[12];
            memcpy(resp, &req_term, 8);
            uint32_t zero = 0;
            memcpy(resp + 8, &zero, 4);
            network_send_raft(h->net, from_peer, MSG_DAG_SYNC_RESP,
                              resp, 12);
            return;
        }

        size_t resp_len = 8 + (size_t)batch_size;
        uint8_t *resp = malloc(resp_len);
        if (!resp) return;

        memcpy(resp, &req_term, 8);
        ssize_t wrote = dag_serialize_batch(h->dag, resp + 8, (size_t)batch_size);
        if (wrote > 0) {
            network_send_raft(h->net, from_peer, MSG_DAG_SYNC_RESP,
                              resp, 8 + (size_t)wrote);
        }
        free(resp);
        return;
    }

    if (msg_type == MSG_DAG_SYNC_RESP) {
        sync_handle_response(h, from_peer, data, len);
        return;
    }

    // ---- Write path messages ----

    if (msg_type == MSG_DAG_PUSH) {
        if (data && len > 0) {
            size_t consumed = 0;
            dag_node_t *node = dag_node_deserialize(h->dag, data, len, &consumed);
            if (node) {
                network_send_raft(h->net, from_peer, MSG_DAG_PUSH_FF_ACK,
                                  node->hash, DAG_HASH_SIZE);
            }
        }
        return;
    }

    if (msg_type == MSG_DAG_PUSH_FF_ACK) {
        if (!data || len < DAG_HASH_SIZE) return;
        pending_push_t *pp = push_find_by_hash(h, data);
        if (pp && pp->conn) {
            int n = protocol_fmt_ok(h->resp_buf, RESPONSE_BUF_SIZE);
            if (n > 0) conn_send(pp->conn, h->resp_buf, (size_t)n);
            h->stats.requests_ok++;
            push_remove(pp);
        } else if (pp) {
            push_remove(pp);
        }
        return;
    }

    if (msg_type == MSG_DAG_PUSH_CONFIRMED) {
        if (!data || len <= 8) return;
        if (!raft_is_leader(h->raft)) return;

        uint64_t seq;
        memcpy(&seq, data, 8);

        size_t consumed = 0;
        dag_node_t *node = dag_node_deserialize(h->dag, data + 8, len - 8, &consumed);
        if (node && node->leader_seq == 0) {
            uint64_t term = raft_get_term(h->raft);
            node->leader_seq = (term << 32) | (++h->leader_seq_counter);
            key_index_update(h->dag, node);
        }

        network_send_raft(h->net, from_peer, MSG_DAG_PUSH_ACK,
                          (const uint8_t *)&seq, 8);
        return;
    }

    if (msg_type == MSG_DAG_PUSH_ACK) {
        if (!data || len < 8) return;

        uint64_t seq;
        memcpy(&seq, data, 8);

        pending_push_t *pp = push_find(h, seq);
        if (pp && pp->conn) {
            int n = protocol_fmt_ok(h->resp_buf, RESPONSE_BUF_SIZE);
            if (n > 0) conn_send(pp->conn, h->resp_buf, (size_t)n);
            h->stats.requests_ok++;
            push_remove(pp);
        } else if (pp) {
            push_remove(pp);
        }
        return;
    }

    // Anti-entropy (30-34)
    gossip_recv(h->gossip, from_peer, msg_type, data, len,
                gossip_send_cb, h);
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
    (void)last_applied;
}

void handler_on_leadership_change(handler_t *h, bool is_leader) {
    if (!h) return;

    if (!is_leader) {
        pending_fail_all(h->pending, LYGUS_ERR_NOT_LEADER);

        for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
            pending_push_t *pp = &h->pushes[i];
            if (!pp->active) continue;
            if (pp->conn) {
                int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                           "leader changed");
                if (n > 0) conn_send(pp->conn, h->resp_buf, (size_t)n);
                h->stats.requests_error++;
            }
            push_remove(pp);
        }

        frontier_fail_all(h, "leader changed");

        // Abort any in-progress sync
        if (h->sync_active) {
            sync_abort(h);
        }
        h->leader_ready = false;
    }

    if (is_leader) {
        // Don't set leader_seq_counter here — sync_complete will
        // reconstruct it from max existing leader_seq.
        // Don't set leader_ready here — sync_complete will.
        sync_start(h);
    }
}

void handler_on_term_change(handler_t *h, uint64_t new_term) {
    if (!h) return;
    // Term changed — any in-flight quorum ping is stale
    if (h->qping_active && new_term != h->qping_term) {
        frontier_fail_all(h, "term changed during read");
    }
    // Term changed — any in-flight sync is stale
    if (h->sync_active && new_term != h->sync_term) {
        sync_abort(h);
    }
}

void handler_on_log_truncate(handler_t *h, uint64_t from_index) {
    if (!h) return;
    pending_fail_from(h->pending, from_index, LYGUS_ERR_LOG_MISMATCH);
}

void handler_on_conn_close(handler_t *h, conn_t *conn) {
    if (!h || !conn) return;
    pending_fail_conn(h->pending, conn, LYGUS_ERR_NET);

    // Null out connection in pending pushes
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active && h->pushes[i].conn == conn) {
            h->pushes[i].conn = NULL;
        }
    }

    // Null out connection in pending frontier reads
    for (int i = 0; i < MAX_PENDING_FRONTIER_READS; i++) {
        if (h->frontier_reads[i].active && h->frontier_reads[i].conn == conn) {
            h->frontier_reads[i].conn = NULL;
        }
    }
}

void handler_tick(handler_t *h, uint64_t now_ms) {
    if (!h) return;

    pending_timeout_sweep(h->pending, now_ms);

    // Sweep pending pushes for timeouts
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        pending_push_t *pp = &h->pushes[i];
        if (!pp->active || now_ms < pp->deadline_ms) continue;
        if (pp->conn) {
            int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                       "write timeout (leader ack)");
            if (n > 0) conn_send(pp->conn, h->resp_buf, (size_t)n);
            h->stats.requests_error++;
        }
        push_remove(pp);
    }

    // Sweep pending frontier reads for timeouts
    for (int i = 0; i < MAX_PENDING_FRONTIER_READS; i++) {
        pending_frontier_read_t *r = &h->frontier_reads[i];
        if (!r->active || now_ms < r->deadline_ms) continue;
        if (r->conn) {
            int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                       "read timeout (quorum ping)");
            if (n > 0) conn_send(r->conn, h->resp_buf, (size_t)n);
            h->stats.requests_error++;
        }
        r->active = false;
        r->conn = NULL;
    }

    // Reset stale quorum ping if it timed out
    if (h->qping_active && now_ms >= h->qping_deadline_ms) {
        frontier_fail_all(h, "quorum ping timeout");
    }

    // Reset stale DAG sync if it timed out
    if (h->sync_active && now_ms >= h->sync_deadline_ms) {
        sync_abort(h);
        // leader_ready stays false — reads blocked until next election
    }

    gossip_tick(h->gossip, gossip_send_cb, h);

    // Background drain (GC) — one batch per interval, not on any hot path
    h->drain_tick_count++;
    if (h->drain_tick_count >= DRAIN_INTERVAL_TICKS) {
        h->drain_tick_count = 0;
        if (raft_is_leader(h->raft) && dag_count(h->dag) > 0
            && !h->sync_active) {
            propose_dag_batch(h);
        }
    }
}

void handler_on_readindex_complete(handler_t *h, uint64_t req_id,
                                    uint64_t read_index, int err) {
    (void)h; (void)req_id; (void)read_index; (void)err;
}

void handler_reset_dag(handler_t *h) {
    if (!h) return;
    dag_reset(h->dag);
}

// ============================================================================
// Accessors
// ============================================================================

merkle_dag_t *handler_get_dag(const handler_t *h) {
    return h ? h->dag : NULL;
}

void handler_flush_dag(handler_t *h) {
    (void)h;
}

// ============================================================================
// Stats
// ============================================================================

void handler_get_stats(const handler_t *h, handler_stats_t *out) {
    if (!h || !out) return;
    *out = h->stats;
    out->writes_pending = pending_count(h->pending);
    out->dag_node_count = dag_count(h->dag);

    // Count pending frontier reads
    uint64_t rp = 0;
    for (int i = 0; i < MAX_PENDING_FRONTIER_READS; i++) {
        if (h->frontier_reads[i].active) rp++;
    }
    out->reads_pending = rp;
}

size_t handler_pending_count(const handler_t *h) {
    if (!h) return 0;
    return pending_count(h->pending);
}