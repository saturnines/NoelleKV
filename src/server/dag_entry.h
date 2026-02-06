#ifndef ANNA_DAG_ENTRY_H
#define ANNA_DAG_ENTRY_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DAG_ENTRY_MARKER  0xDA   /* First byte of DAG batch Raft entries */
#define DAG_OP_PUT        0x01   /* Stored in dag_node value[0] */
#define DAG_OP_DEL        0x02   /* Stored in dag_node value[0] */

    /**
     * Check if a Raft log entry is a DAG batch
     */
    static inline int dag_entry_is_batch(const uint8_t *entry, size_t len) {
        return len > 0 && entry[0] == DAG_ENTRY_MARKER;
    }

    /**
     * Get pointer to the batch payload (skipping the marker byte)
     */
    static inline const uint8_t *dag_entry_batch_payload(const uint8_t *entry) {
        return entry + 1;
    }

    /**
     * Get batch payload length
     */
    static inline size_t dag_entry_batch_len(size_t entry_len) {
        return entry_len > 0 ? entry_len - 1 : 0;
    }

#ifdef __cplusplus
}
#endif

#endif /* ANNA_DAG_ENTRY_H */