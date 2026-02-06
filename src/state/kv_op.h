/**
 * kv_op.h - KV operation serialization for Raft replication
 *
 * This is the wire format for KV operations replicated through Raft.
 * Raft treats these as opaque bytes.
 */

#ifndef LYGUS_KV_OP_H
#define LYGUS_KV_OP_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// KV Operation Types
// ============================================================================

typedef enum {
    KV_OP_PUT  = 1,
    KV_OP_DEL  = 2,
    KV_OP_NOOP = 3,
} kv_op_type_t;

// ============================================================================
// Serialization
// ============================================================================

/**
 * Serialize a KV operation to bytes
 *
 * Format: [type:1][klen:4][vlen:4][key][value]
 *
 * @param buf      Output buffer
 * @param buf_size Buffer capacity
 * @param type     Operation type
 * @param key      Key bytes (can be NULL for NOOP)
 * @param klen     Key length
 * @param val      Value bytes (can be NULL for DEL/NOOP)
 * @param vlen     Value length
 * @return         Number of bytes written, or 0 on error
 */
size_t kv_op_serialize(uint8_t *buf, size_t buf_size,
                       kv_op_type_t type,
                       const void *key, uint32_t klen,
                       const void *val, uint32_t vlen);

/**
 * Deserialize bytes into a KV operation
 *
 * Note: Returned pointers point into 'data' - do not free them separately.
 *
 * @param data  Input bytes
 * @param len   Input length
 * @param type  Output: operation type
 * @param key   Output: key pointer (into data)
 * @param klen  Output: key length
 * @param val   Output: value pointer (into data)
 * @param vlen  Output: value length
 * @return      0 on success, -1 on malformed data
 */
int kv_op_deserialize(const void *data, size_t len,
                      kv_op_type_t *type,
                      const void **key, uint32_t *klen,
                      const void **val, uint32_t *vlen);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_KV_OP_H