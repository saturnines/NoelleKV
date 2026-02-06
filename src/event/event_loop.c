/**
 * event_loop.c - Event loop implementation
 *
 * Uses epoll on Linux, with platform layer for time.
 */

#include "event_loop.h"
#include "platform/platform.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>

#ifdef __linux__
#include <sys/epoll.h>
#else
#include <sys/select.h>
#endif

// ============================================================================
// Configuration
// ============================================================================

#define MAX_EVENTS      256
#define MAX_FDS         4096
#define MAX_TIMERS      64
#define EPOLL_TIMEOUT   10   // ms, for timer resolution

// ============================================================================
// Internal Types
// ============================================================================

typedef struct {
    int            fd;
    uint32_t       events;
    ev_callback_fn callback;
    void          *data;
    int            active;
} fd_entry_t;

struct ev_timer {
    uint32_t     interval_ms;
    uint64_t     next_fire;
    ev_timer_fn  callback;
    void        *data;
    int          active;
};

struct event_loop {
#ifdef __linux__
    int           epoll_fd;
#endif
    fd_entry_t   *fds;
    size_t        fd_cap;

    ev_timer_t   *timers;
    size_t        timer_cap;
    size_t        timer_count;

    uint64_t      now_ms;
    int           running;
};

// ============================================================================
// Time Helper
// ============================================================================

static uint64_t get_time_ms(void) {
    return lygus_monotonic_ns() / 1000000ULL;
}

// ============================================================================
// Lifecycle
// ============================================================================

event_loop_t *event_loop_create(void) {
    event_loop_t *loop = calloc(1, sizeof(*loop));
    if (!loop) return NULL;

#ifdef __linux__
    loop->epoll_fd = epoll_create1(0);
    if (loop->epoll_fd < 0) {
        free(loop);
        return NULL;
    }
#endif

    loop->fd_cap = MAX_FDS;
    loop->fds = calloc(loop->fd_cap, sizeof(fd_entry_t));
    if (!loop->fds) {
#ifdef __linux__
        close(loop->epoll_fd);
#endif
        free(loop);
        return NULL;
    }

    // Initialize fd entries as inactive
    for (size_t i = 0; i < loop->fd_cap; i++) {
        loop->fds[i].fd = -1;
        loop->fds[i].active = 0;
    }

    loop->timer_cap = MAX_TIMERS;
    loop->timers = calloc(loop->timer_cap, sizeof(ev_timer_t));
    if (!loop->timers) {
        free(loop->fds);
#ifdef __linux__
        close(loop->epoll_fd);
#endif
        free(loop);
        return NULL;
    }

    loop->now_ms = get_time_ms();
    loop->running = 0;

    return loop;
}

void event_loop_destroy(event_loop_t *loop) {
    if (!loop) return;

#ifdef __linux__
    if (loop->epoll_fd >= 0) {
        close(loop->epoll_fd);
    }
#endif

    free(loop->fds);
    free(loop->timers);
    free(loop);
}

// ============================================================================
// Timer Processing
// ============================================================================

static void process_timers(event_loop_t *loop) {
    for (size_t i = 0; i < loop->timer_cap; i++) {
        ev_timer_t *t = &loop->timers[i];
        if (!t->active) continue;

        if (loop->now_ms >= t->next_fire) {
            // Fire callback
            if (t->callback) {
                t->callback(loop, t->data);
            }

            // Schedule next fire (if still active - callback might have removed it)
            if (t->active) {
                t->next_fire = loop->now_ms + t->interval_ms;
            }
        }
    }
}

// ============================================================================
// Running (epoll implementation)
// ============================================================================

#ifdef __linux__

void event_loop_run(event_loop_t *loop) {
    if (!loop) return;

    loop->running = 1;
    struct epoll_event events[MAX_EVENTS];

    while (loop->running) {
        // Update time
        loop->now_ms = get_time_ms();

        // Process timers
        process_timers(loop);

        // Wait for events
        int n = epoll_wait(loop->epoll_fd, events, MAX_EVENTS, EPOLL_TIMEOUT);

        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }

        // Update time again after potentially blocking
        loop->now_ms = get_time_ms();

        // Process events
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;

            if (fd < 0 || (size_t)fd >= loop->fd_cap) continue;

            fd_entry_t *entry = &loop->fds[fd];
            if (!entry->active) continue;

            // Convert epoll events to our flags
            uint32_t ev = 0;
            if (events[i].events & EPOLLIN)  ev |= EV_READ;
            if (events[i].events & EPOLLOUT) ev |= EV_WRITE;
            if (events[i].events & EPOLLERR) ev |= EV_ERROR;
            if (events[i].events & EPOLLHUP) ev |= EV_HANGUP;

            if (entry->callback) {
                entry->callback(loop, fd, ev, entry->data);
            }
        }
    }
}

void event_loop_stop(event_loop_t *loop) {
    if (loop) loop->running = 0;
}

int event_loop_add(event_loop_t *loop, int fd, uint32_t events,
                   ev_callback_fn callback, void *data) {
    if (!loop || fd < 0 || (size_t)fd >= loop->fd_cap) return -1;

    // Convert our flags to epoll
    uint32_t epoll_events = 0;
    if (events & EV_READ)  epoll_events |= EPOLLIN;
    if (events & EV_WRITE) epoll_events |= EPOLLOUT;

    struct epoll_event ev = {
        .events = epoll_events,
        .data.fd = fd
    };

    if (epoll_ctl(loop->epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        return -1;
    }

    loop->fds[fd].fd = fd;
    loop->fds[fd].events = events;
    loop->fds[fd].callback = callback;
    loop->fds[fd].data = data;
    loop->fds[fd].active = 1;

    return 0;
}

int event_loop_del(event_loop_t *loop, int fd) {
    if (!loop || fd < 0 || (size_t)fd >= loop->fd_cap) return -1;

    epoll_ctl(loop->epoll_fd, EPOLL_CTL_DEL, fd, NULL);

    loop->fds[fd].fd = -1;
    loop->fds[fd].active = 0;
    loop->fds[fd].callback = NULL;
    loop->fds[fd].data = NULL;

    return 0;
}

int event_loop_mod(event_loop_t *loop, int fd, uint32_t events) {
    if (!loop || fd < 0 || (size_t)fd >= loop->fd_cap) return -1;
    if (!loop->fds[fd].active) return -1;

    uint32_t epoll_events = 0;
    if (events & EV_READ)  epoll_events |= EPOLLIN;
    if (events & EV_WRITE) epoll_events |= EPOLLOUT;

    struct epoll_event ev = {
        .events = epoll_events,
        .data.fd = fd
    };

    if (epoll_ctl(loop->epoll_fd, EPOLL_CTL_MOD, fd, &ev) < 0) {
        return -1;
    }

    loop->fds[fd].events = events;
    return 0;
}

#else
// ============================================================================
// Running (select fallback for macOS/BSD/Windows)
// ============================================================================

void event_loop_run(event_loop_t *loop) {
    if (!loop) return;

    loop->running = 1;

    while (loop->running) {
        loop->now_ms = get_time_ms();
        process_timers(loop);

        fd_set read_fds, write_fds;
        FD_ZERO(&read_fds);
        FD_ZERO(&write_fds);

        int max_fd = -1;

        for (size_t i = 0; i < loop->fd_cap; i++) {
            fd_entry_t *entry = &loop->fds[i];
            if (!entry->active) continue;

            if (entry->events & EV_READ)  FD_SET(entry->fd, &read_fds);
            if (entry->events & EV_WRITE) FD_SET(entry->fd, &write_fds);

            if (entry->fd > max_fd) max_fd = entry->fd;
        }

        struct timeval tv = {
            .tv_sec = 0,
            .tv_usec = EPOLL_TIMEOUT * 1000
        };

        int n = select(max_fd + 1, &read_fds, &write_fds, NULL, &tv);

        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }

        loop->now_ms = get_time_ms();

        if (n > 0) {
            for (size_t i = 0; i < loop->fd_cap; i++) {
                fd_entry_t *entry = &loop->fds[i];
                if (!entry->active) continue;

                uint32_t ev = 0;
                if (FD_ISSET(entry->fd, &read_fds))  ev |= EV_READ;
                if (FD_ISSET(entry->fd, &write_fds)) ev |= EV_WRITE;

                if (ev && entry->callback) {
                    entry->callback(loop, entry->fd, ev, entry->data);
                }
            }
        }
    }
}

void event_loop_stop(event_loop_t *loop) {
    if (loop) loop->running = 0;
}

int event_loop_add(event_loop_t *loop, int fd, uint32_t events,
                   ev_callback_fn callback, void *data) {
    if (!loop || fd < 0 || (size_t)fd >= loop->fd_cap) return -1;

    loop->fds[fd].fd = fd;
    loop->fds[fd].events = events;
    loop->fds[fd].callback = callback;
    loop->fds[fd].data = data;
    loop->fds[fd].active = 1;

    return 0;
}

int event_loop_del(event_loop_t *loop, int fd) {
    if (!loop || fd < 0 || (size_t)fd >= loop->fd_cap) return -1;

    loop->fds[fd].fd = -1;
    loop->fds[fd].active = 0;
    loop->fds[fd].callback = NULL;
    loop->fds[fd].data = NULL;

    return 0;
}

int event_loop_mod(event_loop_t *loop, int fd, uint32_t events) {
    if (!loop || fd < 0 || (size_t)fd >= loop->fd_cap) return -1;
    if (!loop->fds[fd].active) return -1;

    loop->fds[fd].events = events;
    return 0;
}

#endif

// ============================================================================
// Timers
// ============================================================================

ev_timer_t *event_loop_timer_add(event_loop_t *loop, uint32_t interval_ms,
                                  ev_timer_fn callback, void *data) {
    if (!loop || !callback || interval_ms == 0) return NULL;

    // Find free slot
    for (size_t i = 0; i < loop->timer_cap; i++) {
        ev_timer_t *t = &loop->timers[i];
        if (!t->active) {
            t->interval_ms = interval_ms;
            t->next_fire = loop->now_ms + interval_ms;
            t->callback = callback;
            t->data = data;
            t->active = 1;
            loop->timer_count++;
            return t;
        }
    }

    return NULL;  // No free slots
}

void event_loop_timer_del(event_loop_t *loop, ev_timer_t *timer) {
    if (!loop || !timer) return;

    // Verify timer belongs to this loop
    if (timer < loop->timers || timer >= loop->timers + loop->timer_cap) {
        return;
    }

    if (timer->active) {
        timer->active = 0;
        timer->callback = NULL;
        timer->data = NULL;
        loop->timer_count--;
    }
}

// ============================================================================
// Time
// ============================================================================

uint64_t event_loop_now_ms(event_loop_t *loop) {
    return loop ? loop->now_ms : get_time_ms();
}