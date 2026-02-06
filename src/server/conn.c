/**
 * conn.c - Connection management implementation
 */

#include "conn.h"
#include "platform/platform.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include "event/event_loop.h"

// ============================================================================
// Defaults
// ============================================================================

#define DEFAULT_READ_BUF   4096
#define DEFAULT_WRITE_BUF  4096
#define DEFAULT_MAX_READ   (1024 * 1024)

// ============================================================================
// Internal Structure
// ============================================================================

struct conn {
    event_loop_t      *loop;
    lygus_socket_t     sock;
    conn_state_t       state;

    char               addr[64];

    // Read buffer
    char              *read_buf;
    size_t             read_len;
    size_t             read_cap;
    size_t             read_max;

    // Write buffer
    char              *write_buf;
    size_t             write_len;
    size_t             write_pos;
    size_t             write_cap;

    // Callbacks
    conn_on_message_fn on_message;
    conn_on_close_fn   on_close;
    void              *ctx;
};

// ============================================================================
// Forward Declarations
// ============================================================================

static void conn_on_event(event_loop_t *loop, int fd, uint32_t events, void *data);

// ============================================================================
// Lifecycle
// ============================================================================

conn_t *conn_create(event_loop_t *loop,
                    lygus_socket_t sock,
                    const char *addr_str,
                    const conn_config_t *cfg) {
    if (!loop || !cfg) return NULL;

    conn_t *c = calloc(1, sizeof(*c));
    if (!c) return NULL;

    c->loop = loop;
    c->sock = sock;
    c->state = CONN_STATE_READING;

    if (addr_str) {
        snprintf(c->addr, sizeof(c->addr), "%s", addr_str);
    }

    c->read_cap = cfg->read_buf_init > 0 ? cfg->read_buf_init : DEFAULT_READ_BUF;
    c->read_max = cfg->read_buf_max > 0 ? cfg->read_buf_max : DEFAULT_MAX_READ;
    c->write_cap = cfg->write_buf_init > 0 ? cfg->write_buf_init : DEFAULT_WRITE_BUF;

    c->read_buf = malloc(c->read_cap);
    c->write_buf = malloc(c->write_cap);

    if (!c->read_buf || !c->write_buf) {
        free(c->read_buf);
        free(c->write_buf);
        free(c);
        return NULL;
    }

    c->on_message = cfg->on_message;
    c->on_close = cfg->on_close;
    c->ctx = cfg->ctx;

    // Register with event loop
    if (event_loop_add(loop, lygus_socket_to_fd(sock), EV_READ, conn_on_event, c) < 0) {
        free(c->read_buf);
        free(c->write_buf);
        free(c);
        return NULL;
    }

    return c;
}

void conn_destroy(conn_t *c) {
    if (!c) return;

    event_loop_del(c->loop, lygus_socket_to_fd(c->sock));
    lygus_socket_close(c->sock);

    free(c->read_buf);
    free(c->write_buf);
    free(c);
}

// ============================================================================
// Line Processing
// ============================================================================

static void process_lines(conn_t *c) {
    char *buf = c->read_buf;
    size_t len = c->read_len;

    char *line_start = buf;
    char *newline;

    while ((newline = memchr(line_start, '\n', len - (size_t)(line_start - buf))) != NULL) {
        *newline = '\0';

        size_t line_len = (size_t)(newline - line_start);
        if (line_len > 0 && line_start[line_len - 1] == '\r') {
            line_start[--line_len] = '\0';
        }

        if (c->on_message && line_len > 0) {
            c->on_message(c, line_start, line_len, c->ctx);
        }

        // Check if connection was closed during callback
        if (c->state == CONN_STATE_CLOSED) return;

        line_start = newline + 1;
    }

    // Move remaining partial line to front
    size_t remaining = len - (size_t)(line_start - buf);
    if (remaining > 0 && line_start != buf) {
        memmove(buf, line_start, remaining);
    }
    c->read_len = remaining;
}

// ============================================================================
// Event Handling
// ============================================================================

static void handle_read(conn_t *c) {
    // Grow buffer if needed
    if (c->read_len >= c->read_cap - 1) {
        size_t new_cap = c->read_cap * 2;
        if (new_cap > c->read_max) {
            // Buffer overflow, close connection
            conn_close_now(c);
            return;
        }
        char *new_buf = realloc(c->read_buf, new_cap);
        if (!new_buf) {
            conn_close_now(c);
            return;
        }
        c->read_buf = new_buf;
        c->read_cap = new_cap;
    }

    int64_t n = lygus_socket_recv(c->sock,
                                  c->read_buf + c->read_len,
                                  c->read_cap - c->read_len - 1);

    if (n == -2) return;  // Would block

    if (n <= 0) {
        conn_close_now(c);
        return;
    }

    c->read_len += (size_t)n;
    c->read_buf[c->read_len] = '\0';

    process_lines(c);
}

static void handle_write(conn_t *c) {
    if (c->write_len == 0) {
        // Nothing to write, switch back to reading
        event_loop_mod(c->loop, lygus_socket_to_fd(c->sock), EV_READ);
        c->state = CONN_STATE_READING;
        return;
    }

    int64_t n = lygus_socket_send(c->sock,
                                  c->write_buf + c->write_pos,
                                  c->write_len - c->write_pos);

    if (n == -2) return;  // Would block

    if (n < 0) {
        conn_close_now(c);
        return;
    }

    c->write_pos += (size_t)n;

    if (c->write_pos >= c->write_len) {
        // All written
        c->write_len = 0;
        c->write_pos = 0;

        if (c->state == CONN_STATE_CLOSING) {
            conn_destroy(c);
            if (c->on_close) {
                c->on_close(c, c->ctx);
            }
        } else {
            event_loop_mod(c->loop, lygus_socket_to_fd(c->sock), EV_READ);
            c->state = CONN_STATE_READING;
        }
    }
}

static void conn_on_event(event_loop_t *loop, int fd, uint32_t events, void *data) {
    (void)loop;
    (void)fd;
    conn_t *c = (conn_t *)data;

    if (events & (EV_ERROR | EV_HANGUP)) {
        conn_close_now(c);
        return;
    }

    if (c->state == CONN_STATE_WRITING || c->state == CONN_STATE_CLOSING) {
        if (events & EV_WRITE) {
            handle_write(c);
        }
    } else {
        if (events & EV_READ) {
            handle_read(c);
        }
    }
}

// ============================================================================
// Sending
// ============================================================================

int conn_send(conn_t *c, const void *data, size_t len) {
    if (!c || !data || len == 0) return -1;
    if (c->state == CONN_STATE_CLOSED) return -1;

    // Grow buffer if needed
    size_t needed = c->write_len + len;
    if (needed > c->write_cap) {
        size_t new_cap = c->write_cap * 2;
        while (new_cap < needed) new_cap *= 2;

        char *new_buf = realloc(c->write_buf, new_cap);
        if (!new_buf) return -1;

        c->write_buf = new_buf;
        c->write_cap = new_cap;
    }

    memcpy(c->write_buf + c->write_len, data, len);
    c->write_len += len;

    // Switch to writing mode
    if (c->state == CONN_STATE_READING) {
        event_loop_mod(c->loop, lygus_socket_to_fd(c->sock), EV_READ | EV_WRITE);
        c->state = CONN_STATE_WRITING;
    }

    return 0;
}

int conn_sendf(conn_t *c, const char *fmt, ...) {
    char buf[4096];
    va_list ap;

    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    if (n < 0) return -1;
    if ((size_t)n >= sizeof(buf)) n = (int)sizeof(buf) - 1;

    return conn_send(c, buf, (size_t)n);
}

// ============================================================================
// Control
// ============================================================================

void conn_close(conn_t *c) {
    if (!c) return;

    if (c->write_len > 0 && c->state != CONN_STATE_CLOSED) {
        c->state = CONN_STATE_CLOSING;
        event_loop_mod(c->loop, lygus_socket_to_fd(c->sock), EV_WRITE);
    } else {
        conn_close_now(c);
    }
}

void conn_close_now(conn_t *c) {
    if (!c) return;

    conn_on_close_fn on_close = c->on_close;
    void *ctx = c->ctx;

    c->state = CONN_STATE_CLOSED;

    if (on_close) {
        on_close(c, ctx);
    }

    conn_destroy(c);
}

// ============================================================================
// Accessors
// ============================================================================

conn_state_t conn_get_state(const conn_t *c) {
    return c ? c->state : CONN_STATE_CLOSED;
}

const char *conn_get_addr(const conn_t *c) {
    return c ? c->addr : "";
}

int conn_get_fd(const conn_t *c) {
    return c ? lygus_socket_to_fd(c->sock) : -1;
}

void *conn_get_ctx(const conn_t *c) {
    return c ? c->ctx : NULL;
}

void conn_set_ctx(conn_t *c, void *ctx) {
    if (c) c->ctx = ctx;
}

event_loop_t *conn_get_loop(const conn_t *c) {
    return c ? c->loop : NULL;
}

size_t conn_write_pending(const conn_t *c) {
    return c ? (c->write_len - c->write_pos) : 0;
}