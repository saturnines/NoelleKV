/**
 * protocol.h - Client wire protocol
 *
 * Internal to server library. Parses requests, formats responses.
 *
 * Wire format (text, newline-terminated):
 *   PUT <key_b64> <value_b64>
 *   GET <key_b64>
 *   DEL <key_b64>
 *   STATUS | PING | VERSION
 *
 * Responses:
 *   OK | VALUE <b64> | NOT_FOUND | ERROR <msg>
 *   LEADER <term> <index> | FOLLOWER <leader_id> <term>
 *   PONG | VERSION <ver>
 */

#ifndef LYGUS_PROTOCOL_H
#define LYGUS_PROTOCOL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Request Types
// ============================================================================

typedef enum {
    REQ_INVALID = 0,
    REQ_PUT,
    REQ_GET,
    REQ_DEL,
    REQ_STATUS,
    REQ_PING,
    REQ_VERSION,
} request_type_t;

static inline bool request_is_write(request_type_t t) {
    return t == REQ_PUT || t == REQ_DEL;
}

// ============================================================================
// Parsed Request
// ============================================================================

/**
 * Parsed request
 *
 * key/value point into parser's internal buffers.
 * Valid until next parse call.
 */
typedef struct {
    request_type_t  type;
    const uint8_t  *key;
    size_t          klen;
    const uint8_t  *value;  // PUT only
    size_t          vlen;
} request_t;

// ============================================================================
// Parser Context
// ============================================================================

typedef struct protocol_ctx protocol_ctx_t;

/**
 * Create parser context
 *
 * @param max_key_size   Max key bytes
 * @param max_value_size Max value bytes
 */
protocol_ctx_t *protocol_ctx_create(size_t max_key_size, size_t max_value_size);
void protocol_ctx_destroy(protocol_ctx_t *ctx);

// ============================================================================
// Parsing
// ============================================================================

/**
 * Parse request line
 *
 * @param ctx   Parser context
 * @param line  Null-terminated line (no \n)
 * @param len   Line length
 * @param out   Output request
 * @return 0 success, <0 error (-1=unknown cmd, -2=missing arg, -3=bad b64, -4=too large)
 */
int protocol_parse(protocol_ctx_t *ctx, const char *line, size_t len, request_t *out);

const char *protocol_parse_strerror(int err);

// ============================================================================
// Response Formatting
// ============================================================================

int protocol_fmt_ok(char *buf, size_t cap);
int protocol_fmt_value(char *buf, size_t cap, const void *val, size_t vlen);
int protocol_fmt_not_found(char *buf, size_t cap);
int protocol_fmt_error(char *buf, size_t cap, const char *msg);
int protocol_fmt_errorf(char *buf, size_t cap, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));

int protocol_fmt_leader(char *buf, size_t cap, uint64_t term, uint64_t index);
int protocol_fmt_follower(char *buf, size_t cap, int leader_id, uint64_t term);
int protocol_fmt_pong(char *buf, size_t cap);
int protocol_fmt_version(char *buf, size_t cap, const char *ver);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_PROTOCOL_H