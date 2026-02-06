/**
 * conn.h - Client connection management
 *
 * Internal to server library. Handles:
 *   - Connection lifecycle
 *   - Read/write buffers
 *   - Non-blocking I/O with event loop
 *   - Graceful close with write drain
 */

#ifndef LYGUS_CONN_H
#define LYGUS_CONN_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Forward Declarations
// ============================================================================

typedef struct event_loop event_loop_t;
typedef struct conn conn_t;

// Platform socket type
#ifdef _WIN32
typedef uintptr_t lygus_socket_t;
#else
typedef int lygus_socket_t;
#endif

// ============================================================================
// Types
// ============================================================================

typedef enum {
    CONN_STATE_READING,
    CONN_STATE_WRITING,
    CONN_STATE_CLOSING,
    CONN_STATE_CLOSED,
} conn_state_t;

/**
 * Callback: complete message received
 */
typedef void (*conn_on_message_fn)(conn_t *conn, const char *data, size_t len, void *ctx);

/**
 * Callback: connection closed
 */
typedef void (*conn_on_close_fn)(conn_t *conn, void *ctx);

/**
 * Connection config
 */
typedef struct {
    size_t              read_buf_init;   // Initial read buffer (default: 4KB)
    size_t              write_buf_init;  // Initial write buffer (default: 4KB)
    size_t              read_buf_max;    // Max read buffer (default: 1MB)
    conn_on_message_fn  on_message;      // Line received callback
    conn_on_close_fn    on_close;        // Close callback (optional)
    void               *ctx;             // User context for callbacks
} conn_config_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create connection from accepted socket
 *
 * Socket will be set non-blocking and registered with event loop.
 *
 * @param loop      Event loop
 * @param sock      Accepted socket
 * @param addr_str  Remote address string (copied)
 * @param config    Connection config
 * @return Connection, or NULL on error
 */
conn_t *conn_create(event_loop_t *loop,
                    lygus_socket_t sock,
                    const char *addr_str,
                    const conn_config_t *config);

/**
 * Destroy connection immediately
 *
 * Does NOT call on_close. Use conn_close() for graceful shutdown.
 */
void conn_destroy(conn_t *conn);

// ============================================================================
// I/O
// ============================================================================

/**
 * Queue data for sending
 *
 * @return 0 on success, -1 on error
 */
int conn_send(conn_t *conn, const void *data, size_t len);

/**
 * Queue formatted string
 */
int conn_sendf(conn_t *conn, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

// ============================================================================
// Control
// ============================================================================

/**
 * Close gracefully (drain writes first)
 */
void conn_close(conn_t *conn);

/**
 * Close immediately (discard pending writes)
 */
void conn_close_now(conn_t *conn);

// ============================================================================
// Accessors
// ============================================================================

conn_state_t   conn_get_state(const conn_t *conn);
const char    *conn_get_addr(const conn_t *conn);
int            conn_get_fd(const conn_t *conn);
void          *conn_get_ctx(const conn_t *conn);
void           conn_set_ctx(conn_t *conn, void *ctx);
event_loop_t  *conn_get_loop(const conn_t *conn);
size_t         conn_write_pending(const conn_t *conn);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_CONN_H