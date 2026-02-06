#ifndef LYGUS_CRC32C_H
#define LYGUS_CRC32C_H

#include <stdint.h>
#include <stddef.h>

/**
 * CRC32C initial value (for standard CRC-32C)
 */
#define CRC32C_INIT 0xFFFFFFFF

/**
 * Compute CRC32C checksum (castagnoli polynomial, one-shot)
 *
 * This is the standard CRC-32C function:
 * - Initializes with 0xFFFFFFFF
 * - Processes data
 * - Final XOR with 0xFFFFFFFF
 *
 * @param buf  Data buffer
 * @param len  Buffer length
 * @return     CRC32C checksum
 */
uint32_t crc32c(const uint8_t *buf, size_t len);

/**
 * Streaming CRC32C update (no init/final XOR)
 *
 * For incremental calculation:
 *   uint32_t crc = CRC32C_INIT;
 *   crc = crc32c_update(crc, chunk1, len1);
 *   crc = crc32c_update(crc, chunk2, len2);
 *   crc ^= 0xFFFFFFFF; // Final XOR
 *
 * @param crc  Current CRC state
 * @param buf  Data buffer
 * @param len  Buffer length
 * @return     Updated CRC state
 */
uint32_t crc32c_update(uint32_t crc, const uint8_t *buf, size_t len);

/**
 * Check if hardware CRC32C is available
 * @return 1 if SSE4.2 available, 0 otherwise
 */
int crc32c_hw_available(void);

#endif // LYGUS_CRC32C_H