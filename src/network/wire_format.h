#ifndef LYGUS_WIRE_FORMAT_H
#define LYGUS_WIRE_FORMAT_H

#include <stdint.h>
#include <stddef.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Message Type
// ============================================================================

    typedef enum {
        // Raft RPCs
        MSG_REQUESTVOTE_REQ     = 1,
        MSG_REQUESTVOTE_RESP    = 2,
        MSG_APPENDENTRIES_REQ   = 3,
        MSG_APPENDENTRIES_RESP  = 4,
        MSG_INSTALLSNAPSHOT_REQ  = 5,
        MSG_INSTALLSNAPSHOT_RESP = 6,
        MSG_READINDEX_REQ        = 7,
        MSG_READINDEX_RESP       = 8,

        MSG_INV                 = 10,   // Invalidation broadcast

        // Control
        MSG_PING                = 20,
        MSG_PONG                = 21,

        // ---- DAG gossip (maps to gossip.h constants) ----
        MSG_GOSSIP_SYNC         = 30,   // GOSSIP_SYNC
        MSG_GOSSIP_NEED_TIPS    = 31,   // GOSSIP_NEED_TIPS
        MSG_GOSSIP_TIPS         = 32,   // GOSSIP_TIPS
        MSG_GOSSIP_NEED_NODES   = 33,   // GOSSIP_NEED_NODES
        MSG_GOSSIP_NODES        = 34,   // GOSSIP_NODES
        MSG_DAG_PUSH            = 35,   // Push-on-write: single serialized node
        MSG_DAG_PUSH_CONFIRMED  = 36,   // Confirmed push: [seq:8][serialized node]
        MSG_DAG_PUSH_ACK        = 37,   // Push ACK: [seq:8]
        MSG_DAG_PUSH_FF_ACK     = 38,   // Fire-and-forget push ACK: [hash:32]

        // ---- Frontier read path ----
        MSG_QUORUM_PING         = 39,   // Leader → peers: "am I still leader?" [term:8]
        MSG_QUORUM_PING_ACK     = 40,   // Peer → leader: "yes" [term:8]

        // ---- DAG sync (leader election) ----
        MSG_DAG_SYNC_REQ        = 41,   // New leader → peers: "send me your DAG" [term:8]
        MSG_DAG_SYNC_RESP       = 42,   // Peer → new leader: [term:8][dag batch...]

        // ---- Follower read path (Lazy-ALR) ----
        MSG_FOLLOWER_SYNC_REQ   = 43,   // Follower → leader: "max_acked_seq?" [term:8]
        MSG_FOLLOWER_SYNC_RESP  = 44,   // Leader → follower: [max_acked_seq:8]

    } msg_type_t;

/**
 * Check if message type is a gossip/DAG protocol message.
 * These route to gossip_inbox in the network layer.
 */
static inline int msg_is_gossip(uint8_t type) {
    return type >= MSG_GOSSIP_SYNC && type <= MSG_FOLLOWER_SYNC_RESP;
}

// ============================================================================
// Wire Header
// ============================================================================

#define WIRE_HEADER_SIZE 8

typedef struct __attribute__((packed)) {
    uint8_t  type;      // msg_type_t
    uint8_t  from_id;   // Sender node ID
    uint8_t  _reserved[2];
    uint32_t len;       // Payload length
} wire_header_t;

// ============================================================================
// Serialization Helpers
// ============================================================================

static inline size_t wire_encode(void *buf, uint8_t type, uint8_t from_id,
                                  const void *payload, uint32_t payload_len)
{
    uint8_t *p = (uint8_t *)buf;

    p[0] = type;
    p[1] = from_id;
    p[2] = 0;
    p[3] = 0;
    memcpy(p + 4, &payload_len, 4);

    if (payload && payload_len > 0) {
        memcpy(p + WIRE_HEADER_SIZE, payload, payload_len);
    }

    return WIRE_HEADER_SIZE + payload_len;
}

static inline const void *wire_decode(const void *buf, size_t len,
                                       wire_header_t *hdr)
{
    if (!buf || len < WIRE_HEADER_SIZE || !hdr) {
        return NULL;
    }

    const uint8_t *p = (const uint8_t *)buf;

    hdr->type = p[0];
    hdr->from_id = p[1];
    hdr->_reserved[0] = 0;
    hdr->_reserved[1] = 0;
    memcpy(&hdr->len, p + 4, 4);

    if (len < WIRE_HEADER_SIZE + hdr->len) {
        return NULL;
    }

    return p + WIRE_HEADER_SIZE;
}

static inline size_t wire_msg_size(const wire_header_t *hdr)
{
    return WIRE_HEADER_SIZE + hdr->len;
}

#ifdef __cplusplus
}
#endif

#endif // LYGUS_WIRE_FORMAT_H