#ifndef LYGUS_WIRE_FORMAT_H
#define LYGUS_WIRE_FORMAT_H

#include <stdint.h>
#include <stddef.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Message Types
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
    } msg_type_t;

// ============================================================================
// Wire Header
// ============================================================================

#define WIRE_HEADER_SIZE 4

typedef struct __attribute__((packed)) {
    uint8_t  type;      // msg_type_t
    uint8_t  from_id;   // Sender node ID
    uint16_t len;       // Payload length (max 64KB)
} wire_header_t;

// ============================================================================
// Serialization Helpers
// ============================================================================

/**
 * Encode a message for sending
 *
 * @param buf       Output buffer (must be WIRE_HEADER_SIZE + payload_len)
 * @param type      Message type
 * @param from_id   Sender node ID
 * @param payload   Message payload
 * @param payload_len Payload length
 * @return Total bytes written
 */
static inline size_t wire_encode(void *buf, uint8_t type, uint8_t from_id,
                                  const void *payload, uint16_t payload_len)
{
    uint8_t *p = (uint8_t *)buf;

    p[0] = type;
    p[1] = from_id;
    memcpy(p + 2, &payload_len, 2);

    if (payload && payload_len > 0) {
        memcpy(p + WIRE_HEADER_SIZE, payload, payload_len);
    }

    return WIRE_HEADER_SIZE + payload_len;
}

/**
 * Decode a message header
 *
 * @param buf       Input buffer
 * @param len       Buffer length
 * @param hdr       Output header
 * @return Pointer to payload, or NULL if invalid
 */
static inline const void *wire_decode(const void *buf, size_t len,
                                       wire_header_t *hdr)
{
    if (!buf || len < WIRE_HEADER_SIZE || !hdr) {
        return NULL;
    }

    const uint8_t *p = (const uint8_t *)buf;

    hdr->type = p[0];
    hdr->from_id = p[1];
    memcpy(&hdr->len, p + 2, 2);

    // Validate
    if (len < WIRE_HEADER_SIZE + hdr->len) {
        return NULL;
    }

    return p + WIRE_HEADER_SIZE;
}

/**
 * Get total message size from header
 */
static inline size_t wire_msg_size(const wire_header_t *hdr)
{
    return WIRE_HEADER_SIZE + hdr->len;
}

#ifdef __cplusplus
}
#endif

#endif // LYGUS_WIRE_FORMAT_H