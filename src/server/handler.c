/**
 * handler.c - Request handling with CRDT Merkle DAG + Raft integration
 *
 * Write path (leader):
 *   PUT/DEL → dag_add(key, tagged_value, tips_as_parents)
 *           → fire-and-forget push to all peers
 *           → wait for one peer MSG_DAG_PUSH_FF_ACK
 *           → OK to client (write on 2 nodes: leader + confirming peer)
 *
 * Write path (follower):
 *   PUT/DEL → dag_add(key, tagged_value, tips_as_parents)
 *           → confirmed push to leader (MSG_DAG_PUSH_CONFIRMED)
 *           → fire-and-forget push to other peers
 *           → wait for leader ACK (MSG_DAG_PUSH_ACK)
 *           → OK to client (causal lease: leader has the write)
 *
 * Read path (leader):
 *   GET → if DAG non-empty: serialize batch, raft_propose
 *       → alr_read (ReadIndex → wait for apply → serve from KV)
 *
 * Apply path (all replicas):
 *   Raft commits batch → dag_deserialize_batch into temp DAG
 *                       → dag_iter_topo for deterministic order
 *                       → apply each op to KV state machine
 *                       → selectively prune committed nodes from local DAG
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
#include "network/network.h"
#include "network/wire_format.h"

// DAG modules
#include "merkle_dag.h"
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
#define DEFAULT_ALR_CAPACITY     4096
#define DEFAULT_ALR_SLAB         (16 * 1024 * 1024)
#define DEFAULT_MAX_KEY          1024
#define DEFAULT_MAX_VALUE        (1024 * 1024)

#define RESPONSE_BUF_SIZE        (2 * 1024 * 1024)
#define ENTRY_BUF_SIZE           (1024 * 1024 + 1024)

#define DEFAULT_DAG_MAX_NODES    65536
#define DEFAULT_DAG_ARENA_SIZE   (16 * 1024 * 1024)
#define DEFAULT_BATCH_BUF_SIZE   (8 * 1024 * 1024)

// Max tips to use as parents for a new node
#define MAX_TIP_PARENTS          8

// Confirmed push: max outstanding writes awaiting leader ACK
#define MAX_PENDING_PUSHES       4096

// Fire-and-forget push ACK: peer confirms receipt of a pushed node.
// Payload: [hash:32].  Leader uses this to ACK client writes.
// NOTE: Move this to wire_format.h alongside MSG_DAG_PUSH etc.
#ifndef MSG_DAG_PUSH_FF_ACK
#define MSG_DAG_PUSH_FF_ACK  36
#endif

// ============================================================================
// Confirmed Push - pending write awaiting acknowledgement
//
// Used for BOTH paths:
//   - Follower writes: keyed by seq, awaiting leader MSG_DAG_PUSH_ACK
//   - Leader writes:   keyed by hash, awaiting peer MSG_DAG_PUSH_FF_ACK
// ============================================================================

typedef struct {
    uint64_t    seq;            // Sequence number (follower path only)
    uint8_t     hash[DAG_HASH_SIZE]; // Node hash (leader path only)
    conn_t     *conn;           // Client connection to ACK when confirmed
    uint64_t    deadline_ms;    // Timeout deadline
    bool        active;         // Slot in use
    bool        is_leader_write; // true = leader awaiting ff-ack, false = follower awaiting push-ack
} pending_push_t;

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
    alr_t           *alr;

    // DAG (owned)
    merkle_dag_t    *dag;
    gossip_t        *gossip;
    merkle_dag_t    *apply_dag;        // Temp DAG for deserializing committed batches

    // Config
    int              node_id;
    int              num_peers;
    uint32_t         timeout_ms;
    const char      *version;
    bool             leader_only;

    // Scratch buffers
    char            *resp_buf;
    uint8_t         *entry_buf;
    uint8_t         *batch_buf;        // For DAG batch serialization
    size_t           batch_buf_cap;
    uint8_t         *node_push_buf;    // For push-on-write serialization
    size_t           node_push_cap;

    // DAG commit state
    bool             dag_propose_pending;  // true while a batch is in Raft pipeline

    // Confirmed push state
    pending_push_t   pushes[MAX_PENDING_PUSHES];
    uint64_t         next_push_seq;

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
static void gossip_send_cb(void *ctx, int to_peer, uint8_t msg_type,
                           const uint8_t *data, size_t len);
static int gossip_pick_peer_cb(void *ctx);

// Apply a single DAG node's operation to the KV state machine
static void apply_node_to_kv(dag_node_t *node, void *ctx);

// Propose current DAG as a Raft batch entry (leader only)
static uint64_t propose_dag_batch(handler_t *h);

// Push a single node to all peers
static void push_node_to_peers(handler_t *h, dag_node_t *node);

// Send confirmed push to leader, stash connection for ACK
static void push_node_confirmed(handler_t *h, conn_t *conn, dag_node_t *node);

// ============================================================================
// Confirmed Push Helpers
// ============================================================================

static pending_push_t *push_alloc(handler_t *h) {
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (!h->pushes[i].active) {
            return &h->pushes[i];
        }
    }
    return NULL;  // Table full
}

static pending_push_t *push_find(handler_t *h, uint64_t seq) {
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active && !h->pushes[i].is_leader_write
            && h->pushes[i].seq == seq) {
            return &h->pushes[i];
        }
    }
    return NULL;
}

static pending_push_t *push_find_by_hash(handler_t *h, const uint8_t *hash) {
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active && h->pushes[i].is_leader_write
            && memcmp(h->pushes[i].hash, hash, DAG_HASH_SIZE) == 0) {
            return &h->pushes[i];
        }
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

    // Create protocol context
    size_t max_key = cfg->max_key_size > 0 ? cfg->max_key_size : DEFAULT_MAX_KEY;
    size_t max_val = cfg->max_value_size > 0 ? cfg->max_value_size : DEFAULT_MAX_VALUE;
    h->proto = protocol_ctx_create(max_key, max_val);
    if (!h->proto) goto fail;

    // Create pending table (used for batch commit tracking)
    size_t max_pending = cfg->max_pending > 0 ? cfg->max_pending : DEFAULT_MAX_PENDING;
    h->pending = pending_create(max_pending, on_pending_complete, h);
    if (!h->pending) goto fail;

    // Create ALR
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

    // ---- DAG setup ----
    size_t dag_max = cfg->dag_max_nodes > 0 ? cfg->dag_max_nodes : DEFAULT_DAG_MAX_NODES;
    size_t dag_arena = cfg->dag_arena_size > 0 ? cfg->dag_arena_size : DEFAULT_DAG_ARENA_SIZE;

    h->dag = dag_create(dag_max, dag_arena);
    if (!h->dag) goto fail;

    // Temp DAG for applying committed batches (separate from write DAG)
    h->apply_dag = dag_create(dag_max, dag_arena);
    if (!h->apply_dag) goto fail;

    // Gossip
    gossip_config_t gossip_cfg = {
        .node_id = cfg->node_id,
        .dag = h->dag,
        .pick_peer = gossip_pick_peer_cb,
        .pick_peer_ctx = h,
    };
    h->gossip = gossip_create(&gossip_cfg);
    if (!h->gossip) goto fail;

    // Scratch buffers
    h->resp_buf = malloc(RESPONSE_BUF_SIZE);
    h->entry_buf = malloc(ENTRY_BUF_SIZE);

    h->batch_buf_cap = cfg->batch_buf_size > 0 ? cfg->batch_buf_size : DEFAULT_BATCH_BUF_SIZE;
    h->batch_buf = malloc(h->batch_buf_cap);

    h->node_push_cap = 4096;  // Enough for a single node
    h->node_push_buf = malloc(h->node_push_cap);

    if (!h->resp_buf || !h->entry_buf || !h->batch_buf || !h->node_push_buf) goto fail;

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

    // Fail any outstanding confirmed pushes
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active) {
            push_remove(&h->pushes[i]);
        }
    }

    if (h->alr) alr_destroy(h->alr);
    if (h->proto) protocol_ctx_destroy(h->proto);
    if (h->gossip) gossip_destroy(h->gossip);
    if (h->dag) dag_destroy(h->dag);
    if (h->apply_dag) dag_destroy(h->apply_dag);

    free(h->resp_buf);
    free(h->entry_buf);
    free(h->batch_buf);
    free(h->node_push_buf);
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

    // Simple random peer selection (skip self)
    int peer;
    do {
        peer = rand() % h->num_peers;  // Assumes peer IDs are 0..num_peers-1
    } while (peer == h->node_id);
    return peer;
}

// ============================================================================
// Callbacks
// ============================================================================

static void on_pending_complete(const pending_entry_t *entry, int err, void *ctx) {
    handler_t *h = (handler_t *)ctx;
    conn_t *conn = (conn_t *)entry->conn;

    // Pending is now only used for tracking batch proposals (optional).
    // No client response needed here — writes are ack'd immediately.
    (void)h;
    (void)conn;
    (void)err;
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
// DAG Apply Path
// ============================================================================

/**
 * Apply a single committed DAG node to the KV state machine.
 *
 * Value encoding:
 *   value[0] == DAG_OP_PUT  → kv_put(key, value[1:])
 *   value[0] == DAG_OP_DEL  → kv_del(key)
 */
static void apply_node_to_kv(dag_node_t *node, void *ctx) {
    handler_t *h = (handler_t *)ctx;

    if (!node || node->value_len == 0) return;

    uint8_t op = node->value[0];

    if (op == DAG_OP_PUT && node->value_len > 1) {
        lygus_kv_put(h->kv,
                     node->key, node->key_len,
                     node->value + 1, node->value_len - 1);
    } else if (op == DAG_OP_DEL) {
        lygus_kv_del(h->kv, node->key, node->key_len);
    }
}

/**
 * Apply a committed DAG batch entry from the Raft log.
 *
 * Called from the Raft glue layer when it sees entry[0] == 0xDA.
 *
 * BUG FIX: Selective removal instead of dag_reset.  Only remove
 * committed nodes from the local DAG.  Nodes that arrived via gossip
 * or client writes during the commit window survive for the next cycle.
 */
int handler_apply_dag_batch(handler_t *h, const uint8_t *entry, size_t len) {
    if (!h || !entry || !dag_entry_is_batch(entry, len)) return -1;

    const uint8_t *payload = dag_entry_batch_payload(entry);
    size_t payload_len = dag_entry_batch_len(len);

    if (payload_len < 4) return -1;

    // Deserialize into temporary DAG
    dag_reset(h->apply_dag);

    int count = dag_deserialize_batch(h->apply_dag, payload, payload_len);
    if (count < 0) return -1;

    // Collect hashes of committed nodes BEFORE applying
    size_t committed_count = dag_count(h->apply_dag);
    uint8_t *committed_hashes = NULL;

    if (committed_count > 0) {
        committed_hashes = malloc(committed_count * DAG_HASH_SIZE);
        if (committed_hashes) {
            committed_count = dag_collect_hashes(h->apply_dag,
                                                  committed_hashes,
                                                  committed_count);
        } else {
            committed_count = 0;
        }
    }

    // Apply in deterministic topo order to KV state machine
    dag_iter_topo(h->apply_dag, apply_node_to_kv, h);

    // Clean up temp DAG
    dag_reset(h->apply_dag);

    if (committed_count > 0 && committed_hashes) {
        dag_remove_by_hashes(h->dag, committed_hashes, committed_count);
    }
    free(committed_hashes);

    h->stats.dag_batches_applied++;

    // Clear the in-flight flag so the next tick/read can propose again.
    h->dag_propose_pending = false;

    return count;
}

// ============================================================================
// DAG Batch Proposal
// ============================================================================

static uint64_t propose_dag_batch(handler_t *h) {
    if (h->dag_propose_pending) {
        return 0;
    }

    size_t count = dag_count(h->dag);
    if (count == 0) return 0;

    // Item 3: collect hashes of unconfirmed leader writes

    uint8_t *exclude_hashes = NULL;
    size_t excl_count = 0;

    // Count pending leader writes first
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active && h->pushes[i].is_leader_write) {
            excl_count++;
        }
    }

    if (excl_count > 0) {
        exclude_hashes = malloc(excl_count * DAG_HASH_SIZE);
        if (exclude_hashes) {
            size_t idx = 0;
            for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
                if (h->pushes[i].active && h->pushes[i].is_leader_write) {
                    memcpy(exclude_hashes + (idx * DAG_HASH_SIZE),
                           h->pushes[i].hash, DAG_HASH_SIZE);
                    idx++;
                }
            }
        } else {
            excl_count = 0;  // OOM fallback: propose nothing
            free(exclude_hashes);
            return 0;
        }
    }

    // Serialize only confirmed nodes
    h->batch_buf[0] = DAG_ENTRY_MARKER;
    ssize_t batch_len = dag_serialize_batch_excluding(
        h->dag, h->batch_buf + 1, h->batch_buf_cap - 1,
        exclude_hashes, excl_count);

    if (batch_len < 0) {
        free(exclude_hashes);
        return 0;
    }

    // Check if batch is empty (all nodes were excluded)
    uint32_t batch_count;
    memcpy(&batch_count, h->batch_buf + 1, 4);
    if (batch_count == 0) {
        free(exclude_hashes);
        return 0;
    }

    size_t entry_len = 1 + (size_t)batch_len;

    int ret = raft_propose(h->raft, h->batch_buf, entry_len);

    if (ret != 0) {
        free(exclude_hashes);
        return 0;
    }

    // ---- Selective removal instead of dag_reset ----
    // Remove only the nodes we just proposed. Unconfirmed writes survive
    // in the DAG for the next batch after their ff-push confirms.
    if (excl_count == 0) {
        // No exclusions — all nodes were proposed, safe to full reset
        dag_reset(h->dag);
    } else {
        // Collect hashes of nodes we DID propose (everything minus excluded)
        size_t total = dag_count(h->dag);
        size_t proposed_max = total;  // upper bound
        uint8_t *proposed_hashes = malloc(proposed_max * DAG_HASH_SIZE);
        if (proposed_hashes) {
            size_t proposed_count = dag_collect_hashes(h->dag, proposed_hashes, proposed_max);

            // Filter out the excluded ones
            size_t write_idx = 0;
            for (size_t i = 0; i < proposed_count; i++) {
                const uint8_t *h_ptr = proposed_hashes + (i * DAG_HASH_SIZE);
                int excluded = 0;
                for (size_t j = 0; j < excl_count; j++) {
                    if (memcmp(h_ptr, exclude_hashes + (j * DAG_HASH_SIZE),
                               DAG_HASH_SIZE) == 0) {
                        excluded = 1;
                        break;
                    }
                }
                if (!excluded) {
                    if (write_idx != i) {
                        memcpy(proposed_hashes + (write_idx * DAG_HASH_SIZE),
                               h_ptr, DAG_HASH_SIZE);
                    }
                    write_idx++;
                }
            }

            if (write_idx > 0) {
                dag_remove_by_hashes(h->dag, proposed_hashes, write_idx);
            }
            free(proposed_hashes);
        }
    }

    free(exclude_hashes);
    h->dag_propose_pending = true;
    h->stats.dag_batches_proposed++;

    return raft_get_pending_index(h->raft);
}
// ============================================================================
// Push-on-write Gossip
// ============================================================================

/**
 * Broadcast a newly inserted node to all peers via PUB/SUB.
 * Fire-and-forget. Anti-entropy catches anything missed.
 */
static void push_node_to_peers(handler_t *h, dag_node_t *node) {
    if (!h->net || !node) return;

    ssize_t needed = dag_node_serialized_size(node);
    if (needed <= 0) return;

    // Grow push buffer if needed
    if ((size_t)needed > h->node_push_cap) {
        size_t new_cap = (size_t)needed * 2;
        uint8_t *new_buf = realloc(h->node_push_buf, new_cap);
        if (!new_buf) return;
        h->node_push_buf = new_buf;
        h->node_push_cap = new_cap;
    }

    ssize_t wrote = dag_node_serialize(node, h->node_push_buf, h->node_push_cap);
    if (wrote <= 0) return;

    // Broadcast to all peers via network layer
    // Using DEALER/ROUTER (reliable) rather than PUB/SUB
    for (int i = 0; i < h->num_peers; i++) {
        if (i == h->node_id) continue;
        network_send_raft(h->net, i, MSG_DAG_PUSH,
                          h->node_push_buf, (size_t)wrote);
    }

    h->stats.dag_nodes_gossiped++;
}

/**
 * Leader write: push node to all peers and stash connection.
 *
 * Client gets ACKed only when a peer sends MSG_DAG_PUSH_FF_ACK back,
 * confirming the write exists on at least two nodes (leader + peer).
 * This is Item 1: "Leader writes need confirmed push before ACK."
 */
static void push_node_to_peers_confirmed(handler_t *h, conn_t *conn, dag_node_t *node) {
    if (!h->net || !node) goto fail;
    if (h->num_peers <= 1) {
        // Single-node cluster: no peers to confirm. ACK immediately.
        // (Durability is msync-only in this case, which is Item 2.)
        int n = protocol_fmt_ok(h->resp_buf, RESPONSE_BUF_SIZE);
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_ok++;
        return;
    }

    pending_push_t *pp = push_alloc(h);
    if (!pp) goto fail;

    // Push to all peers (reuse existing fire-and-forget path)
    push_node_to_peers(h, node);

    // Stash connection — client gets ACKed when any peer confirms
    uint64_t now_ms = event_loop_now_ms(h->loop);
    memcpy(pp->hash, node->hash, DAG_HASH_SIZE);
    pp->seq = 0;
    pp->conn = conn;
    pp->deadline_ms = now_ms + h->timeout_ms;
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

    // Allocate a slot in the pending push table
    pending_push_t *pp = push_alloc(h);
    if (!pp) goto fail;

    ssize_t needed = dag_node_serialized_size(node);
    if (needed <= 0) goto fail;

    // Build confirmed push payload: [seq:8][serialized_node]
    size_t total = 8 + (size_t)needed;

    // Grow push buffer if needed
    if (total > h->node_push_cap) {
        size_t new_cap = total * 2;
        uint8_t *new_buf = realloc(h->node_push_buf, new_cap);
        if (!new_buf) goto fail;
        h->node_push_buf = new_buf;
        h->node_push_cap = new_cap;
    }

    uint64_t seq = h->next_push_seq++;

    // Write sequence number (little-endian)
    memcpy(h->node_push_buf, &seq, 8);

    // Write serialized node after seq
    ssize_t wrote = dag_node_serialize(node, h->node_push_buf + 8, h->node_push_cap - 8);
    if (wrote <= 0) goto fail;

    // Send confirmed push to leader
    network_send_raft(h->net, leader, MSG_DAG_PUSH_CONFIRMED,
                      h->node_push_buf, 8 + (size_t)wrote);

    // Fire-and-forget push to other peers (not leader, not self)
    for (int i = 0; i < h->num_peers; i++) {
        if (i == h->node_id || i == leader) continue;
        network_send_raft(h->net, i, MSG_DAG_PUSH,
                          h->node_push_buf + 8, (size_t)wrote);
    }

    // Stash connection — client gets ACKed when leader responds
    uint64_t now_ms = event_loop_now_ms(h->loop);
    pp->seq = seq;
    pp->conn = conn;
    pp->deadline_ms = now_ms + h->timeout_ms;
    pp->active = true;

    h->stats.dag_nodes_gossiped++;
    return;

fail:
    // Can't confirm — fail the client
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

    if (n > 0) {
        conn_send(conn, h->resp_buf, (size_t)n);
    }
}

/**
 * GET handler - linearizable read via ALR
 *
 * If leader and DAG has pending writes, propose batch first so the
 * ALR ReadIndex will include them in the commit.
 */
static void handle_get(handler_t *h, conn_t *conn, const request_t *req) {
    h->stats.reads_total++;

    // Leader-only mode (benchmark)
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

    if (raft_is_leader(h->raft) && dag_count(h->dag) > 0) {
        propose_dag_batch(h);
    }

    uint64_t now_ms = event_loop_now_ms(h->loop);
    lygus_err_t err = alr_read(h->alr, req->key, req->klen, conn, now_ms);

    if (err == LYGUS_OK) {
        return;
    }

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

    // Get current tips as parents (captures causal order)
    size_t tip_count = dag_tip_count(h->dag);
    uint8_t tip_buf[MAX_TIP_PARENTS * DAG_HASH_SIZE];
    uint32_t parent_count = 0;

    if (tip_count > 0) {
        if (tip_count > MAX_TIP_PARENTS) tip_count = MAX_TIP_PARENTS;
        dag_get_tips(h->dag, tip_buf, &tip_count);
        parent_count = (uint32_t)tip_count;
    }

    // Tag value with operation type: [DAG_OP_PUT][actual_value]
    size_t tagged_vlen = 1 + req->vlen;
    uint8_t *tagged_val = malloc(tagged_vlen);
    if (!tagged_val) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "alloc failed");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }
    tagged_val[0] = DAG_OP_PUT;
    memcpy(tagged_val + 1, req->value, req->vlen);

    // Insert into DAG
    dag_node_t *node = dag_add(h->dag, req->key, req->klen,
                                tagged_val, tagged_vlen,
                                parent_count > 0 ? tip_buf : NULL,
                                parent_count);
    free(tagged_val);

    if (!node) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "dag full");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    if (raft_is_leader(h->raft)) {
        // Leader: hold connection, push to peers, ACK on first confirmation
        h->stats.dag_inserts++;
        push_node_to_peers_confirmed(h, conn, node);
    } else {
        // Follower: confirmed push — hold connection until leader ACKs
        h->stats.dag_inserts++;
        push_node_confirmed(h, conn, node);
    }
}

/**
 * DEL handler - same as PUT but with DAG_OP_DEL marker
 */
static void handle_del(handler_t *h, conn_t *conn, const request_t *req) {
    h->stats.writes_total++;

    // Get current tips as parents
    size_t tip_count = dag_tip_count(h->dag);
    uint8_t tip_buf[MAX_TIP_PARENTS * DAG_HASH_SIZE];
    uint32_t parent_count = 0;

    if (tip_count > 0) {
        if (tip_count > MAX_TIP_PARENTS) tip_count = MAX_TIP_PARENTS;
        dag_get_tips(h->dag, tip_buf, &tip_count);
        parent_count = (uint32_t)tip_count;
    }

    // Tag value with operation type: [DAG_OP_DEL]
    uint8_t del_marker = DAG_OP_DEL;

    dag_node_t *node = dag_add(h->dag, req->key, req->klen,
                                &del_marker, 1,
                                parent_count > 0 ? tip_buf : NULL,
                                parent_count);

    if (!node) {
        int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE, "dag full");
        if (n > 0) conn_send(conn, h->resp_buf, (size_t)n);
        h->stats.requests_error++;
        return;
    }

    if (raft_is_leader(h->raft)) {
        // Leader: hold connection, push to peers, ACK on first confirmation
        h->stats.dag_inserts++;
        push_node_to_peers_confirmed(h, conn, node);
    } else {
        // Follower: confirmed push
        h->stats.dag_inserts++;
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
// Gossip Handling
// ============================================================================

void handler_on_gossip(handler_t *h, int from_peer, uint8_t msg_type,
                       const uint8_t *data, size_t len) {
    if (!h) return;

    if (msg_type == MSG_DAG_PUSH) {
        // Push-on-write: single serialized node
        if (data && len > 0) {
            size_t consumed = 0;
            dag_node_t *node = dag_node_deserialize(h->dag, data, len, &consumed);
            // Idempotent — if we already have this node, it's a no-op

            // ACK back to sender so leader writes get confirmed.
            // Payload: the 32-byte hash of the node we just absorbed.
            if (node) {
                network_send_raft(h->net, from_peer, MSG_DAG_PUSH_FF_ACK,
                                  node->hash, DAG_HASH_SIZE);
            }
        }
        return;
    }

    if (msg_type == MSG_DAG_PUSH_FF_ACK) {
        // Leader receives confirmation that a peer has a pushed node.
        // Payload: [hash:32]
        if (!data || len < DAG_HASH_SIZE) return;

        pending_push_t *pp = push_find_by_hash(h, data);
        if (pp && pp->conn) {
            // First confirmation — ACK the client
            int n = protocol_fmt_ok(h->resp_buf, RESPONSE_BUF_SIZE);
            if (n > 0) conn_send(pp->conn, h->resp_buf, (size_t)n);
            h->stats.requests_ok++;
            push_remove(pp);
        } else if (pp) {
            // Connection gone (client disconnected), just clean up
            push_remove(pp);
        }
        // If pp is NULL, this is a duplicate ACK (already confirmed). Ignore.
        return;
    }

    if (msg_type == MSG_DAG_PUSH_CONFIRMED) {
        // Leader receives confirmed push from follower: [seq:8][serialized_node]
        if (!data || len <= 8) return;

        uint64_t seq;
        memcpy(&seq, data, 8);

        // Deserialize node into our DAG
        size_t consumed = 0;
        dag_node_deserialize(h->dag, data + 8, len - 8, &consumed);

        // Send ACK back to the follower
        network_send_raft(h->net, from_peer, MSG_DAG_PUSH_ACK,
                          (const uint8_t *)&seq, 8);
        return;
    }

    if (msg_type == MSG_DAG_PUSH_ACK) {
        // Follower receives ACK from leader: [seq:8]
        if (!data || len < 8) return;

        uint64_t seq;
        memcpy(&seq, data, 8);

        pending_push_t *pp = push_find(h, seq);
        if (pp && pp->conn) {
            // NOW we can ACK the client
            int n = protocol_fmt_ok(h->resp_buf, RESPONSE_BUF_SIZE);
            if (n > 0) conn_send(pp->conn, h->resp_buf, (size_t)n);
            h->stats.requests_ok++;
            push_remove(pp);
        } else if (pp) {
            // Connection gone (client disconnected), just clean up
            push_remove(pp);
        }
        return;
    }

    // Anti-entropy protocol messages (30-34)
    gossip_recv(h->gossip, from_peer, msg_type, data, len,
                gossip_send_cb, h);
}

// ============================================================================
// Event Hooks
// ============================================================================

void handler_on_commit(handler_t *h, uint64_t index, uint64_t term) {
    (void)term;
    if (!h) return;

    // Complete any pending entries (batch proposals)
    pending_complete(h->pending, index);
}

void handler_on_apply(handler_t *h, uint64_t last_applied) {
    if (!h) return;
    alr_notify(h->alr, last_applied);
}

void handler_on_leadership_change(handler_t *h, bool is_leader) {
    if (!h) return;

    if (!is_leader) {
        pending_fail_all(h->pending, LYGUS_ERR_NOT_LEADER);
        alr_on_term_change(h->alr, raft_get_term(h->raft));
        // Clear in-flight flag — the new leader may re-commit our

        h->dag_propose_pending = false;

        // Fail all pending pushes — leader changed, ACKs won't come
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
    }
}

void handler_on_term_change(handler_t *h, uint64_t new_term) {
    if (!h) return;
    alr_on_term_change(h->alr, new_term);
}

void handler_on_log_truncate(handler_t *h, uint64_t from_index) {
    if (!h) return;
    pending_fail_from(h->pending, from_index, LYGUS_ERR_LOG_MISMATCH);
    h->dag_propose_pending = false;
}

void handler_on_conn_close(handler_t *h, conn_t *conn) {
    if (!h || !conn) return;
    pending_fail_conn(h->pending, conn, LYGUS_ERR_NET);
    alr_cancel_conn(h->alr, conn);

    // Null out connection in any pending pushes (don't remove — ACK may still come)
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        if (h->pushes[i].active && h->pushes[i].conn == conn) {
            h->pushes[i].conn = NULL;
        }
    }
}

void handler_tick(handler_t *h, uint64_t now_ms) {
    if (!h) return;

    pending_timeout_sweep(h->pending, now_ms);
    alr_timeout_sweep(h->alr, now_ms);

    // Sweep pending pushes for timeouts
    for (int i = 0; i < MAX_PENDING_PUSHES; i++) {
        pending_push_t *pp = &h->pushes[i];
        if (!pp->active) continue;
        if (now_ms < pp->deadline_ms) continue;

        // Timed out — fail the client
        if (pp->conn) {
            int n = protocol_fmt_error(h->resp_buf, RESPONSE_BUF_SIZE,
                                       "write timeout (leader ack)");
            if (n > 0) conn_send(pp->conn, h->resp_buf, (size_t)n);
            h->stats.requests_error++;
        }
        push_remove(pp);
    }

    gossip_tick(h->gossip, gossip_send_cb, h);

}

void handler_on_readindex_complete(handler_t *h, uint64_t req_id,
                                    uint64_t read_index, int err) {
    if (!h || !h->alr) return;
    alr_on_read_index(h->alr, req_id, read_index, err);
}

// ============================================================================
// Accessors
// ============================================================================

merkle_dag_t *handler_get_dag(const handler_t *h) {
    return h ? h->dag : NULL;
}

/**
 * Flush pending DAG nodes to Raft if this node is leader.
 *
 * Called when a ReadIndex arrives from a follower — this counts as
 * an "observation" in the Hollow Purple model.  Without this, follower
 * reads would never trigger the leader to commit pending DAG nodes,
 * since ReadIndex is processed entirely in the Raft layer and never
 * flows through handle_get.
 */
void handler_flush_dag(handler_t *h) {
    if (!h) return;
    if (raft_is_leader(h->raft) && dag_count(h->dag) > 0) {
        propose_dag_batch(h);
    }
}

// ============================================================================
// Stats
// ============================================================================

void handler_get_stats(const handler_t *h, handler_stats_t *out) {
    if (!h || !out) return;

    *out = h->stats;
    out->writes_pending = pending_count(h->pending);
    out->dag_node_count = dag_count(h->dag);

    alr_stats_t alr_stats;
    alr_get_stats(h->alr, &alr_stats);
    out->reads_pending = alr_stats.pending_count;
}

size_t handler_pending_count(const handler_t *h) {
    if (!h) return 0;
    return pending_count(h->pending);
}