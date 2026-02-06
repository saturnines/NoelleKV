#include "logging.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>
#include <errno.h>

#ifdef __linux__
#include <sys/syscall.h>
#endif

// Windows compatibility
#ifdef _WIN32
    #include <windows.h>
    #include <io.h>
    #define fsync(fd) _commit(fd)
#endif

// ============================================================================
// Ring Buffer Configuration
// ============================================================================

#define LYGUS_RING_SIZE 65536u        // Must be power of 2
#define LYGUS_RING_MASK (LYGUS_RING_SIZE - 1u)

// ============================================================================
// Slot Structure (Event + Ready Flag)
// ============================================================================

typedef struct {
    atomic_bool   ready;  // false = empty, true = published
    lygus_event_t event;
} lygus_slot_t;

// ============================================================================
// Global State
// ============================================================================

typedef struct {
    // Ring buffer (MPSC queue with per-slot ready flags)
    lygus_slot_t slots[LYGUS_RING_SIZE];

    // Atomics for coordination
    atomic_uint_fast64_t write_idx;  // next ticket to claim
    atomic_uint_fast64_t read_idx;   // next ticket consumer will read
    atomic_uint_fast64_t dropped;    // overflow counter

    // Mode control
    atomic_int global_mode;                      // Global log level threshold
    atomic_int module_modes[LYGUS_MODULE_COUNT]; // Per-module overrides

    // Formatter thread
    pthread_t   formatter_thread;
    atomic_bool running;
    int         log_fd;

    // Node ID
    uint16_t node_id;
} lygus_ring_t;

static lygus_ring_t *g_ring = NULL;

// ============================================================================
// String Tables (for formatting)
// ============================================================================

static const char *level_names[] = {
    "FATAL", "ERROR", "WARN", "INFO", "DEBUG"
};

static const char *module_names[] = {
    "core", "raft", "wal", "alr", "net", "apply", "snapshot", "kv", "storage"
};

// ============================================================================
// Helper: Get thread ID
// ============================================================================

static inline uint32_t get_thread_id(void) {
#ifdef __linux__
    return (uint32_t)syscall(SYS_gettid);
#else
    return (uint32_t)(uintptr_t)pthread_self();
#endif
}

// ============================================================================
// Helper: Get timestamp
// ============================================================================

static inline uint64_t get_timestamp_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

// ============================================================================
// Level Check (Fast Path)
// ============================================================================

static inline bool should_log(uint16_t level, uint16_t module) {
    if (!g_ring) return false;
    if (module >= LYGUS_MODULE_COUNT) return false;

    int module_mode = atomic_load_explicit(&g_ring->module_modes[module],
                                           memory_order_relaxed);
    if (module_mode >= 0) {
        return level <= (uint16_t)module_mode;
    }

    int global_mode = atomic_load_explicit(&g_ring->global_mode,
                                           memory_order_relaxed);
    return level <= (uint16_t)global_mode;
}

// ============================================================================
// Format Event (Background Thread)
// ============================================================================

static int format_event(char *buf, size_t buf_len, const lygus_event_t *e) {
    // Clamp indices defensively
    unsigned lvl = e->level;
    if (lvl > LYGUS_LOG_DEBUG) lvl = LYGUS_LOG_DEBUG;

    unsigned mod = e->module;
    if (mod >= LYGUS_MODULE_COUNT) mod = LYGUS_MODULE_CORE;

    // Base format: timestamp [LEVEL] node=X module.event term=Y index=Z
    int n = snprintf(buf, buf_len,
                     "%llu.%09llu [%s] node=%u %s.%u term=%llu index=%llu",
                     (unsigned long long)(e->timestamp_ns / 1000000000ULL),
                     (unsigned long long)(e->timestamp_ns % 1000000000ULL),
                     level_names[lvl],
                     (unsigned)e->node_id,
                     module_names[mod],
                     (unsigned)e->event_type,
                     (unsigned long long)e->term,
                     (unsigned long long)e->index);
    if (n < 0 || (size_t)n >= buf_len) return -1;

    // Event-specific fields
    switch (mod) {
        case LYGUS_MODULE_WAL:
            if (e->event_type == LYGUS_EVENT_WAL_BLOCK_DONE) {
                n += snprintf(buf + n, buf_len - (size_t)n,
                              " seq=%llu raw=%u comp=%u lat_us=%u",
                              (unsigned long long)e->data.wal.seq_no,
                              e->data.wal.raw_len,
                              e->data.wal.comp_len,
                              e->data.wal.latency_us);
            } else if (e->event_type == LYGUS_EVENT_WAL_FSYNC_DONE) {
                n += snprintf(buf + n, buf_len - (size_t)n,
                              " lat_us=%u",
                              e->data.wal.latency_us);
            }
            break;

        case LYGUS_MODULE_ALR:
            if (e->event_type == LYGUS_EVENT_ALR_SYNC_ISSUED) {
                n += snprintf(buf + n, buf_len - (size_t)n,
                              " sync_idx=%llu batch=%u",
                              (unsigned long long)e->data.alr.sync_index,
                              e->data.alr.batch_size);
            } else if (e->event_type == LYGUS_EVENT_ALR_READS_RELEASED) {
                n += snprintf(buf + n, buf_len - (size_t)n,
                              " sync_idx=%llu lat_us=%u",
                              (unsigned long long)e->data.alr.sync_index,
                              e->data.alr.latency_us);
            }
            break;

        case LYGUS_MODULE_NET:
            n += snprintf(buf + n, buf_len - (size_t)n,
                          " msg=%u from=%u",
                          e->data.net.msg_type,
                          e->data.net.from_node);
            break;

        default:
            break;
    }

    if (n < 0 || (size_t)n >= buf_len - 1) return -1;

    buf[n++] = '\n';
    buf[n] = '\0';
    return n;
}

// ============================================================================
// Formatter Thread (Consumer)
// ============================================================================

static void* formatter_thread_fn(void *arg) {
    lygus_ring_t *ring = (lygus_ring_t*)arg;
    uint64_t local_read_idx = 0;
    char buf[512];

    for (;;) {
        bool running = atomic_load_explicit(&ring->running,
                                            memory_order_acquire);
        uint64_t local_write_idx = atomic_load_explicit(&ring->write_idx,
                                                        memory_order_acquire);

        // Drain all slots that have been claimed
        while (local_read_idx < local_write_idx) {
            uint32_t     slot_idx = (uint32_t)(local_read_idx & LYGUS_RING_MASK);
            lygus_slot_t *slot    = &ring->slots[slot_idx];

            // Wait until producer publishes this slot
            while (!atomic_load_explicit(&slot->ready, memory_order_acquire)) {
#if defined(__x86_64__) || defined(__i386__)
                __builtin_ia32_pause();
#endif
            }

            lygus_event_t *e = &slot->event;
            int len = format_event(buf, sizeof(buf), e);
            if (len > 0) {
                ssize_t written = write(ring->log_fd, buf, (size_t)len);
                (void)written; // ignore errors for now
            }

            // Mark slot empty again
            atomic_store_explicit(&slot->ready, false,
                                  memory_order_release);

            local_read_idx++;
            // Update global read index for producers' capacity checks
            atomic_store_explicit(&ring->read_idx,
                                  local_read_idx,
                                  memory_order_release);
        }

        if (!running) {
            // After running=false, drain once more and exit
            uint64_t final_write_idx =
                atomic_load_explicit(&ring->write_idx,
                                     memory_order_acquire);
            while (local_read_idx < final_write_idx) {
                uint32_t     slot_idx = (uint32_t)(local_read_idx & LYGUS_RING_MASK);
                lygus_slot_t *slot    = &ring->slots[slot_idx];

                while (!atomic_load_explicit(&slot->ready,
                                             memory_order_acquire)) {
#if defined(__x86_64__) || defined(__i386__)
                    __builtin_ia32_pause();
#endif
                }

                lygus_event_t *e = &slot->event;
                int len = format_event(buf, sizeof(buf), e);
                if (len > 0) {
                    write(ring->log_fd, buf, (size_t)len);
                }

                atomic_store_explicit(&slot->ready, false,
                                      memory_order_release);
                local_read_idx++;
                atomic_store_explicit(&ring->read_idx,
                                      local_read_idx,
                                      memory_order_release);
            }

            fsync(ring->log_fd);
            return NULL;
        }

        // Caught up; nap a bit
        usleep(1000); // 1 ms
    }
}

// ============================================================================
// Public API
// ============================================================================

int lygus_log_init(const char *log_path, uint16_t node_id) {
    if (g_ring != NULL) {
        return -1;  // Already initialized
    }

    lygus_ring_t *ring = (lygus_ring_t*)calloc(1, sizeof(lygus_ring_t));
    if (!ring) {
        return -1;
    }

    // Initialize atomics
    atomic_init(&ring->write_idx, 0);
    atomic_init(&ring->read_idx,  0);
    atomic_init(&ring->dropped,   0);
    atomic_init(&ring->global_mode, LYGUS_LOG_NORMAL);
    atomic_init(&ring->running,     true);

    for (int i = 0; i < LYGUS_MODULE_COUNT; i++) {
        atomic_init(&ring->module_modes[i], -1);  // -1 = use global
    }

    for (size_t i = 0; i < LYGUS_RING_SIZE; i++) {
        atomic_init(&ring->slots[i].ready, false);
        // event contents are zeroed by calloc
    }

    ring->node_id = node_id;

    ring->log_fd = open(log_path,
                        O_WRONLY | O_CREAT | O_APPEND,
                        0644);
    if (ring->log_fd < 0) {
        free(ring);
        return -1;
    }

    int rc = pthread_create(&ring->formatter_thread, NULL,
                            formatter_thread_fn, ring);
    if (rc != 0) {
        close(ring->log_fd);
        free(ring);
        return -1;
    }

    g_ring = ring;
    return 0;
}

void lygus_log_shutdown(void) {
    lygus_ring_t *ring = g_ring;
    if (!ring) return;

    // Stop producers in the embedding code before this call.
    atomic_store_explicit(&ring->running, false, memory_order_release);

    pthread_join(ring->formatter_thread, NULL);

    close(ring->log_fd);
    free(ring);
    g_ring = NULL;
}

void lygus_log_set_mode(lygus_log_mode_t mode) {
    if (!g_ring) return;
    atomic_store_explicit(&g_ring->global_mode, mode,
                          memory_order_release);
}

void lygus_log_set_module_mode(lygus_module_t module,
                               lygus_log_mode_t mode) {
    if (!g_ring || module >= LYGUS_MODULE_COUNT) return;
    atomic_store_explicit(&g_ring->module_modes[module], mode,
                          memory_order_release);
}

lygus_log_mode_t lygus_log_get_mode(void) {
    if (!g_ring) return LYGUS_LOG_NORMAL;
    int m = atomic_load_explicit(&g_ring->global_mode,
                                 memory_order_acquire);
    return (lygus_log_mode_t)m;
}

uint64_t lygus_log_get_dropped(void) {
    if (!g_ring) return 0;
    return atomic_load_explicit(&g_ring->dropped,
                                memory_order_relaxed);
}

void lygus_emit_event(uint16_t level, uint16_t module, uint16_t event_type,
                      uint64_t term, uint64_t index,
                      const void *data, size_t data_len) {
    // Early out if logging off or uninitialised
    if (!should_log(level, module)) {
        return;
    }

    lygus_ring_t *ring = g_ring;
    if (!ring) return;

    uint64_t ticket;

    for (;;) {
        // Snapshot of writer/reader positions
        uint64_t w = atomic_load_explicit(&ring->write_idx,
                                          memory_order_relaxed);
        uint64_t r = atomic_load_explicit(&ring->read_idx,
                                          memory_order_acquire);

        // Ring full: too many tickets outstanding
        if (w - r >= LYGUS_RING_SIZE) {
            atomic_fetch_add_explicit(&ring->dropped, 1,
                                      memory_order_relaxed);
            return;
        }

        // Try to claim ticket w
        if (atomic_compare_exchange_weak_explicit(&ring->write_idx,
                                                  &w, w + 1,
                                                  memory_order_acq_rel,
                                                  memory_order_relaxed)) {
            ticket = w;
            break;  // we own this ticket
        }
        // else: some other producer took w, retry
    }

    uint32_t     slot_idx = (uint32_t)(ticket & LYGUS_RING_MASK);
    lygus_slot_t *slot    = &ring->slots[slot_idx];
    lygus_event_t *e      = &slot->event;

    // ready should be false here
    bool was_ready = atomic_load_explicit(&slot->ready,
                                          memory_order_acquire);
    if (was_ready) {
        atomic_fetch_add_explicit(&ring->dropped, 1,
                                  memory_order_relaxed);
        return;
    }

    // Fill event
    e->timestamp_ns = get_timestamp_ns();
    e->thread_id    = get_thread_id();
    e->level        = level;
    e->module       = module;
    e->event_type   = event_type;
    e->node_id      = ring->node_id;
    e->term         = term;
    e->index        = index;

    memset(e->data.raw, 0, sizeof(e->data.raw));
    if (data && data_len > 0) {
        if (data_len > sizeof(e->data.raw)) {
            data_len = sizeof(e->data.raw);
        }
        memcpy(e->data.raw, data, data_len);
    }

    // Publish event: all writes above happen-before this store
    atomic_store_explicit(&slot->ready, true, memory_order_release);
}
