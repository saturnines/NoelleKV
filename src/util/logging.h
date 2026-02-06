#ifndef LYGUS_LOGGING_H
#define LYGUS_LOGGING_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Lygus Logging System - Ring Buffer + Background Formatter
 *
 * Design:
 * - Lock-free MPSC ring buffer with per-slot publish flags
 * - Background thread formats and writes to disk
 * - Runtime-switchable NORMAL/DETAILED modes
 * - Fixed-size events (64 bytes, cache-aligned)
 * - Always includes term/index for causality tracking
 */

// ============================================================================
// Log Levels & Modes
// ============================================================================

typedef enum {
    LYGUS_LOG_FATAL = 0,
    LYGUS_LOG_ERROR = 1,
    LYGUS_LOG_WARN  = 2,
    LYGUS_LOG_INFO  = 3,
    LYGUS_LOG_DEBUG = 4,
} lygus_log_level_t;

typedef enum {
    LYGUS_LOG_NORMAL   = LYGUS_LOG_INFO,    // INFO and above (production)
    LYGUS_LOG_DETAILED = LYGUS_LOG_DEBUG,   // Everything (debugging / Jepsen)
} lygus_log_mode_t;

// ============================================================================
// Modules (subsystems)
// ============================================================================

typedef enum {
    LYGUS_MODULE_CORE     = 0,
    LYGUS_MODULE_RAFT     = 1,
    LYGUS_MODULE_WAL      = 2,
    LYGUS_MODULE_ALR      = 3,
    LYGUS_MODULE_NET      = 4,
    LYGUS_MODULE_APPLY    = 5,
    LYGUS_MODULE_SNAPSHOT = 6,
    LYGUS_MODULE_KV       = 7,
    LYGUS_MODULE_STORAGE  = 8,
    LYGUS_MODULE_COUNT    = 9,
} lygus_module_t;

// ============================================================================
// Event Types (module-specific)
// ============================================================================

// Core events
enum {
    LYGUS_EVENT_INIT      = 1,
    LYGUS_EVENT_SHUTDOWN  = 2,
};

// Raft events
enum {
    LYGUS_EVENT_BECAME_LEADER    = 10,
    LYGUS_EVENT_BECAME_FOLLOWER  = 11,
    LYGUS_EVENT_VOTE_GRANTED     = 12,
    LYGUS_EVENT_APPEND_ENTRIES   = 13,
};

// WAL events
enum {
    LYGUS_EVENT_WAL_BLOCK_START  = 20,
    LYGUS_EVENT_WAL_BLOCK_DONE   = 21,
    LYGUS_EVENT_WAL_FSYNC_START  = 22,
    LYGUS_EVENT_WAL_FSYNC_DONE   = 23,
    LYGUS_EVENT_WAL_CORRUPTION   = 24,
    LYGUS_EVENT_WAL_RECOVERY     = 25,
};

// ALR events
enum {
    LYGUS_EVENT_ALR_SYNC_ISSUED    = 30,
    LYGUS_EVENT_ALR_SYNC_COMMITTED = 31,
    LYGUS_EVENT_ALR_READS_RELEASED = 32,
    LYGUS_EVENT_ALR_STALE_READ     = 33,
    LYGUS_EVENT_ALR_PIGGYBACK      = 34,
};

// Network events
enum {
    LYGUS_EVENT_NET_SEND    = 40,
    LYGUS_EVENT_NET_RECV    = 41,
    LYGUS_EVENT_NET_CONNECT = 42,
    LYGUS_EVENT_NET_ERROR   = 43,
};

// Apply events
enum {
    LYGUS_EVENT_APPLY_ADVANCED = 50,
    LYGUS_EVENT_APPLY_STALLED  = 51,
};

// Snapshot events
enum {
    LYGUS_EVENT_SNAPSHOT_START = 60,
    LYGUS_EVENT_SNAPSHOT_DONE  = 61,
    LYGUS_EVENT_SNAPSHOT_LOAD  = 62,
};

// KV events
enum {
    LYGUS_EVENT_KV_PUT = 70,
    LYGUS_EVENT_KV_GET = 71,
    LYGUS_EVENT_KV_DEL = 72,
};

// ============================================================================
// Event Structure (Fixed 64 bytes)
// ============================================================================

typedef struct lygus_event {
    // Header (24 bytes)
    uint64_t timestamp_ns;    // CLOCK_REALTIME (or MONOTONIC, up to you)
    uint32_t thread_id;       // Thread that emitted event
    uint16_t level;           // lygus_log_level_t
    uint16_t module;          // lygus_module_t
    uint16_t event_type;      // Module-specific enum
    uint16_t node_id;         // Replica ID

    // Context (16 bytes) - for causality
    uint64_t term;            // Raft term (0 if N/A)
    uint64_t index;           // Raft index (0 if N/A)

    // Event-specific payload (24 bytes)
    union {
        struct {
            uint64_t seq_no;
            uint32_t raw_len;
            uint32_t comp_len;
            uint32_t latency_us;
        } wal;

        struct {
            uint64_t sync_index;
            uint32_t batch_size;
            uint32_t latency_us;
        } alr;

        struct {
            uint32_t msg_type;
            uint32_t from_node;
            uint64_t payload;
        } net;

        struct {
            uint64_t val1;
            uint64_t val2;
            uint64_t val3;
        } generic;

        uint8_t raw[24];
    } data;
} __attribute__((aligned(64))) lygus_event_t;

// Compile-time size verification
_Static_assert(sizeof(lygus_event_t) == 64, "lygus_event_t must be 64 bytes");

// ============================================================================
// Public API
// ============================================================================

/**
 * Initialize logging system.
 *
 * @param log_path  Path to log file (e.g., "/var/log/ppc-server.log")
 * @param node_id   This replica's ID
 * @return          0 on success, -1 on error
 */
int lygus_log_init(const char *log_path, uint16_t node_id);

/**
 * Shutdown logging system (flushes pending events).
 *
 * Call after all producer threads have stopped emitting logs.
 */
void lygus_log_shutdown(void);

/**
 * Set global logging mode.
 *
 * @param mode  LYGUS_LOG_NORMAL (quiet) or LYGUS_LOG_DETAILED (verbose)
 */
void lygus_log_set_mode(lygus_log_mode_t mode);

/**
 * Set per-module logging mode.
 *
 * @param module  Module to configure
 * @param mode    Mode for this module
 */
void lygus_log_set_module_mode(lygus_module_t module, lygus_log_mode_t mode);

/**
 * Get current global mode.
 */
lygus_log_mode_t lygus_log_get_mode(void);

/**
 * Get number of dropped events (overflow / backpressure).
 */
uint64_t lygus_log_get_dropped(void);

/**
 * Emit event (low-level, use macros instead).
 *
 * @param level       Log level
 * @param module      Module ID
 * @param event_type  Event type (module-specific)
 * @param term        Current Raft term (0 if N/A)
 * @param index       Current Raft index (0 if N/A)
 * @param data        Event-specific data (max 24 bytes)
 * @param data_len    Length of data (0-24)
 */
void lygus_emit_event(uint16_t level, uint16_t module, uint16_t event_type,
                    uint64_t term, uint64_t index,
                    const void *data, size_t data_len);

// ============================================================================
// Convenience Macros
// ============================================================================

#define LOG_FATAL(mod, evt, term, idx, data, len) \
    lygus_emit_event(LYGUS_LOG_FATAL, mod, evt, term, idx, data, len)

#define LOG_ERROR(mod, evt, term, idx, data, len) \
    lygus_emit_event(LYGUS_LOG_ERROR, mod, evt, term, idx, data, len)

#define LOG_WARN(mod, evt, term, idx, data, len) \
    lygus_emit_event(LYGUS_LOG_WARN, mod, evt, term, idx, data, len)

#define LOG_INFO(mod, evt, term, idx, data, len) \
    lygus_emit_event(LYGUS_LOG_INFO, mod, evt, term, idx, data, len)

/* Optional build-time debug erasure */
#ifdef LYGUS_ENABLE_DEBUG
#  define LOG_DEBUG(mod, evt, term, idx, data, len) \
       lygus_emit_event(LYGUS_LOG_DEBUG, mod, evt, term, idx, data, len)
#else
#  define LOG_DEBUG(mod, evt, term, idx, data, len) \
       ((void)0)
#endif

// Shorthand for events with no payload
#define LOG_INFO_SIMPLE(mod, evt, term, idx) \
    LOG_INFO(mod, evt, term, idx, NULL, 0)

#define LOG_DEBUG_SIMPLE(mod, evt, term, idx) \
    LOG_DEBUG(mod, evt, term, idx, NULL, 0)

#ifdef __cplusplus
}
#endif

#endif // LYGUS_LOGGING_H