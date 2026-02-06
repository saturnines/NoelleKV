/**
* varint.h - Variable length integer encoding/decoding
 */

#ifndef LYGUS_VARINT_H
#define LYGUS_VARINT_H

#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>
#include "../public/lygus_errors.h"

// Maximum bytes needed to encode a 64-bit value
#define VARINT_MAX_LEN 10

/**
 * Encode a 64-bit unsigned integer as a varint.
 *
 * @param value   Value to encode
 * @param buf     Output buffer
 * @param buf_len Size of output buffer
 * @return        Number of bytes written, or negative error code
 */
ssize_t varint_encode(uint64_t value, uint8_t *buf, size_t buf_len);

/**
 * Decode a varint to a 64-bit unsigned integer.
 *
 * @param buf     Input buffer containing varint
 * @param buf_len Size of input buffer
 * @param out     Output: decoded value
 * @return        Number of bytes consumed, or negative error code
 *
 * Error codes:
 *   LYGUS_ERR_INVALID_ARG      - NULL pointer
 *   LYGUS_ERR_BUFFER_TOO_SMALL - Truncated varint
 *   LYGUS_ERR_OVERFLOW         - Value exceeds 64 bits
 */
ssize_t varint_decode(const uint8_t *buf, size_t buf_len, uint64_t *out);

/**
 * Calculate the encoded size of a value without encoding it.
 *
 * @param value  Value to measure
 * @return       Number of bytes needed (1-10)
 */
size_t varint_size(uint64_t value);

/**
 * Peek at a buffer to determine varint size without fully decoding.
 *
 * @param buf     Input buffer
 * @param buf_len Size of input buffer
 * @return        Size in bytes, or negative error code
 */
ssize_t varint_peek_size(const uint8_t *buf, size_t buf_len);

#endif // LYGUS_VARINT_H