/**
 * protocol.c - Client wire protocol implementation
 */

#include "protocol.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <ctype.h>

// ============================================================================
// Defaults
// ============================================================================

#define DEFAULT_MAX_KEY   1024
#define DEFAULT_MAX_VALUE (1024 * 1024)

// ============================================================================
// Base64 Codec
// ============================================================================

static const char b64_table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static int b64_decode_char(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

static ssize_t base64_decode(const char *in, size_t in_len, void *out, size_t out_cap) {
    if (in_len == 0) return 0;
    if (in_len % 4 != 0) return -1;

    size_t out_len = (in_len / 4) * 3;
    if (in_len > 0 && in[in_len - 1] == '=') out_len--;
    if (in_len > 1 && in[in_len - 2] == '=') out_len--;
    if (out_cap < out_len) return -1;

    uint8_t *o = (uint8_t *)out;
    size_t j = 0;

    for (size_t i = 0; i < in_len; i += 4) {
        int a = b64_decode_char(in[i]);
        int b = b64_decode_char(in[i + 1]);
        int c = (in[i + 2] == '=') ? 0 : b64_decode_char(in[i + 2]);
        int d = (in[i + 3] == '=') ? 0 : b64_decode_char(in[i + 3]);

        if (a < 0 || b < 0 || c < 0 || d < 0) return -1;

        uint32_t v = ((uint32_t)a << 18) | ((uint32_t)b << 12) |
                     ((uint32_t)c << 6) | d;

        if (j < out_len) o[j++] = (v >> 16) & 0xFF;
        if (j < out_len) o[j++] = (v >> 8) & 0xFF;
        if (j < out_len) o[j++] = v & 0xFF;
    }

    return (ssize_t)out_len;
}

static size_t base64_encode(const void *data, size_t len, char *out, size_t out_cap) {
    const uint8_t *in = (const uint8_t *)data;
    size_t out_len = ((len + 2) / 3) * 4;
    if (out_cap < out_len + 1) return 0;

    size_t i, j;
    for (i = 0, j = 0; i < len; i += 3, j += 4) {
        uint32_t v = ((uint32_t)in[i]) << 16;
        if (i + 1 < len) v |= ((uint32_t)in[i + 1]) << 8;
        if (i + 2 < len) v |= in[i + 2];

        out[j]     = b64_table[(v >> 18) & 0x3F];
        out[j + 1] = b64_table[(v >> 12) & 0x3F];
        out[j + 2] = (i + 1 < len) ? b64_table[(v >> 6) & 0x3F] : '=';
        out[j + 3] = (i + 2 < len) ? b64_table[v & 0x3F] : '=';
    }
    out[j] = '\0';
    return j;
}

// ============================================================================
// Parser Context
// ============================================================================

struct protocol_ctx {
    uint8_t *key_buf;
    uint8_t *val_buf;
    size_t   max_key;
    size_t   max_val;
    char    *b64_buf;      // For encoding responses
    size_t   b64_cap;
};

protocol_ctx_t *protocol_ctx_create(size_t max_key, size_t max_val) {
    if (max_key == 0) max_key = DEFAULT_MAX_KEY;
    if (max_val == 0) max_val = DEFAULT_MAX_VALUE;

    protocol_ctx_t *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;

    ctx->max_key = max_key;
    ctx->max_val = max_val;

    ctx->key_buf = malloc(max_key);
    ctx->val_buf = malloc(max_val);
    ctx->b64_cap = ((max_val + 2) / 3) * 4 + 1;
    ctx->b64_buf = malloc(ctx->b64_cap);

    if (!ctx->key_buf || !ctx->val_buf || !ctx->b64_buf) {
        protocol_ctx_destroy(ctx);
        return NULL;
    }

    return ctx;
}

void protocol_ctx_destroy(protocol_ctx_t *ctx) {
    if (!ctx) return;
    free(ctx->key_buf);
    free(ctx->val_buf);
    free(ctx->b64_buf);
    free(ctx);
}

// ============================================================================
// Parsing
// ============================================================================

// Parse errors
#define PARSE_ERR_UNKNOWN_CMD  -1
#define PARSE_ERR_MISSING_ARG  -2
#define PARSE_ERR_BAD_B64      -3
#define PARSE_ERR_TOO_LARGE    -4

static int strcasecmp_n(const char *a, const char *b, size_t n) {
    for (size_t i = 0; i < n; i++) {
        char ca = (char)tolower((unsigned char)a[i]);
        char cb = (char)tolower((unsigned char)b[i]);
        if (ca != cb) return ca - cb;
        if (ca == '\0') return 0;
    }
    return 0;
}

int protocol_parse(protocol_ctx_t *ctx, const char *line, size_t len, request_t *out) {
    if (!ctx || !line || !out) return PARSE_ERR_UNKNOWN_CMD;

    memset(out, 0, sizeof(*out));

    // Skip leading whitespace
    while (len > 0 && isspace((unsigned char)*line)) {
        line++;
        len--;
    }

    if (len == 0) return PARSE_ERR_UNKNOWN_CMD;

    // Find command end
    const char *cmd_end = line;
    while (cmd_end < line + len && !isspace((unsigned char)*cmd_end)) {
        cmd_end++;
    }
    size_t cmd_len = (size_t)(cmd_end - line);

    // Parse command
    if (cmd_len == 3 && strcasecmp_n(line, "GET", 3) == 0) {
        out->type = REQ_GET;
    } else if (cmd_len == 3 && strcasecmp_n(line, "PUT", 3) == 0) {
        out->type = REQ_PUT;
    } else if (cmd_len == 3 && strcasecmp_n(line, "DEL", 3) == 0) {
        out->type = REQ_DEL;
    } else if (cmd_len == 6 && strcasecmp_n(line, "STATUS", 6) == 0) {
        out->type = REQ_STATUS;
        return 0;
    } else if (cmd_len == 4 && strcasecmp_n(line, "PING", 4) == 0) {
        out->type = REQ_PING;
        return 0;
    } else if (cmd_len == 7 && strcasecmp_n(line, "VERSION", 7) == 0) {
        out->type = REQ_VERSION;
        return 0;
    } else {
        return PARSE_ERR_UNKNOWN_CMD;
    }

    // Skip whitespace after command
    const char *p = cmd_end;
    const char *end = line + len;
    while (p < end && isspace((unsigned char)*p)) p++;

    // Parse key (required for GET/PUT/DEL)
    if (p >= end) return PARSE_ERR_MISSING_ARG;

    const char *key_start = p;
    while (p < end && !isspace((unsigned char)*p)) p++;
    size_t key_b64_len = (size_t)(p - key_start);

    // Decode key
    ssize_t klen = base64_decode(key_start, key_b64_len, ctx->key_buf, ctx->max_key);
    if (klen < 0) return PARSE_ERR_BAD_B64;
    if ((size_t)klen > ctx->max_key) return PARSE_ERR_TOO_LARGE;

    out->key = ctx->key_buf;
    out->klen = (size_t)klen;

    // PUT needs value
    if (out->type == REQ_PUT) {
        while (p < end && isspace((unsigned char)*p)) p++;
        if (p >= end) return PARSE_ERR_MISSING_ARG;

        const char *val_start = p;
        while (p < end && !isspace((unsigned char)*p)) p++;
        size_t val_b64_len = (size_t)(p - val_start);

        ssize_t vlen = base64_decode(val_start, val_b64_len, ctx->val_buf, ctx->max_val);
        if (vlen < 0) return PARSE_ERR_BAD_B64;
        if ((size_t)vlen > ctx->max_val) return PARSE_ERR_TOO_LARGE;

        out->value = ctx->val_buf;
        out->vlen = (size_t)vlen;
    }

    return 0;
}

const char *protocol_parse_strerror(int err) {
    switch (err) {
        case 0:                    return "OK";
        case PARSE_ERR_UNKNOWN_CMD: return "unknown command";
        case PARSE_ERR_MISSING_ARG: return "missing argument";
        case PARSE_ERR_BAD_B64:     return "invalid base64";
        case PARSE_ERR_TOO_LARGE:   return "key or value too large";
        default:                    return "parse error";
    }
}

// ============================================================================
// Response Formatting
// ============================================================================

int protocol_fmt_ok(char *buf, size_t cap) {
    if (cap < 4) return -1;
    memcpy(buf, "OK\n", 4);
    return 3;
}

int protocol_fmt_value(char *buf, size_t cap, const void *val, size_t vlen) {
    // "VALUE " + base64 + "\n"
    size_t b64_len = ((vlen + 2) / 3) * 4;
    size_t needed = 6 + b64_len + 2;
    if (cap < needed) return -1;

    memcpy(buf, "VALUE ", 6);
    base64_encode(val, vlen, buf + 6, cap - 6);
    buf[6 + b64_len] = '\n';
    buf[6 + b64_len + 1] = '\0';

    return (int)(6 + b64_len + 1);
}

int protocol_fmt_not_found(char *buf, size_t cap) {
    if (cap < 11) return -1;
    memcpy(buf, "NOT_FOUND\n", 11);
    return 10;
}

int protocol_fmt_error(char *buf, size_t cap, const char *msg) {
    int n = snprintf(buf, cap, "ERROR %s\n", msg);
    if (n < 0 || (size_t)n >= cap) return -1;
    return n;
}

int protocol_fmt_errorf(char *buf, size_t cap, const char *fmt, ...) {
    char msg[256];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    return protocol_fmt_error(buf, cap, msg);
}

int protocol_fmt_leader(char *buf, size_t cap, uint64_t term, uint64_t index) {
    int n = snprintf(buf, cap, "LEADER %llu %llu\n",
                     (unsigned long long)term,
                     (unsigned long long)index);
    if (n < 0 || (size_t)n >= cap) return -1;
    return n;
}

int protocol_fmt_follower(char *buf, size_t cap, int leader_id, uint64_t term) {
    int n = snprintf(buf, cap, "FOLLOWER %d %llu\n",
                     leader_id,
                     (unsigned long long)term);
    if (n < 0 || (size_t)n >= cap) return -1;
    return n;
}

int protocol_fmt_pong(char *buf, size_t cap) {
    if (cap < 6) return -1;
    memcpy(buf, "PONG\n", 6);
    return 5;
}

int protocol_fmt_version(char *buf, size_t cap, const char *ver) {
    int n = snprintf(buf, cap, "VERSION %s\n", ver);
    if (n < 0 || (size_t)n >= cap) return -1;
    return n;
}