/**
* kv_op.c - KV operation serialization implementation
 */

#include "kv_op.h"
#include <string.h>

size_t kv_op_serialize(uint8_t *buf, size_t buf_size,
                       kv_op_type_t type,
                       const void *key, uint32_t klen,
                       const void *val, uint32_t vlen) {
    // Calculate size: type(1) + klen(4) + vlen(4) + key + val
    size_t needed = 1 + 4 + 4 + klen + vlen;

    if (!buf || buf_size < needed) {
        return 0;
    }

    uint8_t *p = buf;
    *p++ = (uint8_t)type;
    memcpy(p, &klen, 4);
    p += 4;
    memcpy(p, &vlen, 4);
    p += 4;

    // Key
    if (key && klen > 0) {
        memcpy(p, key, klen);
        p += klen;
    }

    // Value
    if (val && vlen > 0) {
        memcpy(p, val, vlen);
        p += vlen;
    }

    return needed;
}

int kv_op_deserialize(const void *data, size_t len,
                      kv_op_type_t *type,
                      const void **key, uint32_t *klen,
                      const void **val, uint32_t *vlen) {
    if (!data || !type || !key || !klen || !val || !vlen) {
        return -1;
    }

    // Minimum size: 9 bytes (type + klen + vlen)
    if (len < 9) {
        return -1;
    }

    const uint8_t *p = (const uint8_t *)data;

    *type = (kv_op_type_t)*p++;
    memcpy(klen, p, 4);
    p += 4;

    // Value length
    memcpy(vlen, p, 4);
    p += 4;

    // Validate total length
    if (len != (size_t)(9 + *klen + *vlen)) {
        return -1;  // Malformed
    }

    // Key
    if (*klen > 0) {
        *key = p;
        p += *klen;
    } else {
        *key = NULL;
    }

    // Value
    if (*vlen > 0) {
        *val = p;
    } else {
        *val = NULL;
    }

    return 0;
}