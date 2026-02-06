/**
 * event_loop.h - Event loop abstraction
 *
 * Provides a cross-platform event loop using:
 *   - epoll on Linux
 *   - kqueue on macOS/BSD
 *   - select fallback (Windows, others)
 */

#ifndef LYGUS_EVENT_LOOP_H
#define LYGUS_EVENT_LOOP_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Types
// ============================================================================

typedef struct event_loop event_loop_t;
typedef struct ev_timer ev_timer_t;

// ============================================================================
// Event Flags
// ============================================================================

#define EV_READ   0x01
#define EV_WRITE  0x02
#define EV_ERROR  0x04
#define EV_HANGUP 0x08

// ============================================================================
// Callbacks
// ============================================================================

/**
 * Event callback
 *
 * @param loop    Event loop
 * @param fd      File descriptor that triggered
 * @param events  Bitmask of EV_* flags
 * @param data    User data from event_loop_add()
 */
typedef void (*ev_callback_fn)(event_loop_t *loop, int fd, uint32_t events, void *data);

/**
 * Timer callback
 *
 * @param loop  Event loop
 * @param data  User data from event_loop_timer_add()
 */
typedef void (*ev_timer_fn)(event_loop_t *loop, void *data);

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create event loop
 *
 * @return Event loop, or NULL on error
 */
event_loop_t *event_loop_create(void);

/**
 * Destroy event loop
 *
 * All registered fds and timers are automatically removed.
 */
void event_loop_destroy(event_loop_t *loop);

// ============================================================================
// Running
// ============================================================================

/**
 * Run event loop
 *
 * Blocks until event_loop_stop() is called.
 */
void event_loop_run(event_loop_t *loop);

/**
 * Stop event loop
 *
 * Can be called from any callback or signal handler.
 */
void event_loop_stop(event_loop_t *loop);

// ============================================================================
// File Descriptor Events
// ============================================================================

/**
 * Add file descriptor to event loop
 *
 * @param loop      Event loop
 * @param fd        File descriptor
 * @param events    Bitmask of EV_READ, EV_WRITE
 * @param callback  Callback when events occur
 * @param data      User data passed to callback
 * @return 0 on success, -1 on error
 */
int event_loop_add(event_loop_t *loop, int fd, uint32_t events,
                   ev_callback_fn callback, void *data);

/**
 * Remove file descriptor from event loop
 *
 * @param loop  Event loop
 * @param fd    File descriptor
 * @return 0 on success, -1 on error
 */
int event_loop_del(event_loop_t *loop, int fd);

/**
 * Modify events for file descriptor
 *
 * @param loop    Event loop
 * @param fd      File descriptor
 * @param events  New bitmask of EV_READ, EV_WRITE
 * @return 0 on success, -1 on error
 */
int event_loop_mod(event_loop_t *loop, int fd, uint32_t events);

// ============================================================================
// Timers
// ============================================================================

/**
 * Add repeating timer
 *
 * @param loop         Event loop
 * @param interval_ms  Interval in milliseconds
 * @param callback     Callback each interval
 * @param data         User data passed to callback
 * @return Timer handle, or NULL on error
 */
ev_timer_t *event_loop_timer_add(event_loop_t *loop, uint32_t interval_ms,
                                  ev_timer_fn callback, void *data);

/**
 * Remove timer
 *
 * @param loop   Event loop
 * @param timer  Timer handle from event_loop_timer_add()
 */
void event_loop_timer_del(event_loop_t *loop, ev_timer_t *timer);

// ============================================================================
// Time
// ============================================================================

/**
 * Get current monotonic time in milliseconds
 *
 * Cached and updated each loop iteration for efficiency.
 */
uint64_t event_loop_now_ms(event_loop_t *loop);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_EVENT_LOOP_H