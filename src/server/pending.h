/**
 * pending.h - Pending request tracking
 *
 * Tracks write requests waiting for Raft commit.
 *
 * Flow:
 *   1. Client sends PUT/DEL
 *   2. Server proposes to Raft, gets log index
 *   3. pending_add(index, conn, deadline)
 *   4. Raft commits, apply_entry called
 *   5. pending_complete(index) -> responds to client
 *
 * Edge cases:
 *   - Timeout: pending_timeout_sweep() fails expired
 *   - Leadership loss: pending_fail_all(NOT_LEADER)
 *   - Client disconnect: pending_fail_conn(conn)
 *   - Log truncation: pending_fail_from(index)
 */

#ifndef LYGUS_PENDING_H
#define LYGUS_PENDING_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Types
// ============================================================================

typedef struct pending_table pending_table_t;

typedef struct {
    uint64_t  index;        // Raft log index
    uint64_t  term;         // Term when proposed
    uint64_t  deadline_ms;  // Absolute timeout
    void     *conn;         // Connection to respond to
    uint64_t  request_id;   // Optional client request ID
} pending_entry_t;

/**
 * Completion callback
 *
 * @param entry  The pending entry
 * @param err    0 on success, error code on failure
 * @param ctx    User context
 */
typedef void (*pending_complete_fn)(const pending_entry_t *entry, int err, void *ctx);

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create pending table
 *
 * @param max_pending  Max concurrent pending requests
 * @param on_complete  Callback when request completes/fails
 * @param ctx          User context for callback
 */
pending_table_t *pending_create(size_t max_pending,
                                pending_complete_fn on_complete,
                                void *ctx);

/**
 * Destroy table
 *
 * Does NOT call on_complete for remaining entries.
 */
void pending_destroy(pending_table_t *table);

// ============================================================================
// Operations
// ============================================================================

/**
 * Add pending request
 *
 * @return 0 success, -1 table full
 */
int pending_add(pending_table_t *table,
                uint64_t index,
                uint64_t term,
                uint64_t deadline_ms,
                void *conn,
                uint64_t request_id);

/**
 * Remove and return entry by index
 *
 * @return true if found
 */
bool pending_remove(pending_table_t *table, uint64_t index, pending_entry_t *out);

/**
 * Check if index is pending
 */
bool pending_exists(const pending_table_t *table, uint64_t index);

// ============================================================================
// Completion
// ============================================================================

/**
 * Complete request (success) - calls on_complete with err=0
 */
bool pending_complete(pending_table_t *table, uint64_t index);

/**
 * Fail request - calls on_complete with given error
 */
bool pending_fail(pending_table_t *table, uint64_t index, int err);

/**
 * Sweep expired requests
 *
 * @return Number expired
 */
int pending_timeout_sweep(pending_table_t *table, uint64_t now_ms);

/**
 * Fail all requests for a connection
 *
 * @return Number failed
 */
int pending_fail_conn(pending_table_t *table, void *conn, int err);

/**
 * Fail all requests
 *
 * @return Number failed
 */
int pending_fail_all(pending_table_t *table, int err);

/**
 * Fail requests at or after index (log truncation)
 *
 * @return Number failed
 */
int pending_fail_from(pending_table_t *table, uint64_t from_index, int err);

// ============================================================================
// Stats
// ============================================================================

size_t pending_count(const pending_table_t *table);
size_t pending_capacity(const pending_table_t *table);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_PENDING_H