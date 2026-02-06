/**
 * varint.c - Variable-length integer encoding/decoding
 *
 * Uses LEB128 (Little Endian Base 128) encoding, same as Protocol Buffers.
 * Each byte uses 7 bits for data and 1 bit (MSB) as continuation flag.
 *
 * Max encoded size: 10 bytes for 64-bit values
 */

#include "varint.h"
#include <string.h>

// ============================================================================
// Encoding
// ============================================================================

ssize_t varint_encode(uint64_t value, uint8_t *buf, size_t buf_len) {
    if (!buf || buf_len == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    size_t i = 0;

    while (value >= 0x80) {
        if (i >= buf_len) {
            return LYGUS_ERR_BUFFER_TOO_SMALL;
        }
        buf[i++] = (uint8_t)(value & 0x7F) | 0x80;
        value >>= 7;
    }

    // Final byte (no continuation bit)
    if (i >= buf_len) {
        return LYGUS_ERR_BUFFER_TOO_SMALL;
    }
    buf[i++] = (uint8_t)value;

    return (ssize_t)i;
}

// ============================================================================
// Decoding
// ============================================================================

ssize_t varint_decode(const uint8_t *buf, size_t buf_len, uint64_t *out) {
    if (!buf || !out) {
        return LYGUS_ERR_INVALID_ARG;
    }

    if (buf_len == 0) {
        return LYGUS_ERR_BUFFER_TOO_SMALL;
    }

    uint64_t result = 0;
    size_t shift = 0;
    size_t i = 0;

    while (i < buf_len) {
        uint8_t byte = buf[i];
        uint64_t value = byte & 0x7F;

        if (shift >= 64 || (shift == 63 && value > 1)) {
            return LYGUS_ERR_OVERFLOW;
        }

        result |= (value << shift);
        i++;

        // Check continuation bit
        if ((byte & 0x80) == 0) {
            // Done - no continuation
            *out = result;
            return (ssize_t)i;
        }

        shift += 7;
    }

    // Ran out of buffer before finding end of varint
    return LYGUS_ERR_BUFFER_TOO_SMALL;
}

// ============================================================================
// Utilities
// ============================================================================

size_t varint_size(uint64_t value) {
    size_t size = 1;
    while (value >= 0x80) {
        value >>= 7;
        size++;
    }
    return size;
}

ssize_t varint_peek_size(const uint8_t *buf, size_t buf_len) {
    if (!buf || buf_len == 0) {
        return LYGUS_ERR_INVALID_ARG;
    }

    for (size_t i = 0; i < buf_len && i < VARINT_MAX_LEN; i++) {
        if ((buf[i] & 0x80) == 0) {
            return (ssize_t)(i + 1);
        }
    }

    // Either truncated or invalid (>10 bytes)
    if (buf_len < VARINT_MAX_LEN) {
        return LYGUS_ERR_BUFFER_TOO_SMALL;
    }
    return LYGUS_ERR_OVERFLOW;
}