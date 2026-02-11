/**
 * mailbox.c - Thread-safe message queue implementation
 *
 * Simple mutex-protected ring buffer, Note: Not lock-free.
 */

#include "mailbox.h"

#include <stdlib.h>
#include <string.h>
#include <pthread.h>

struct mailbox {
    mail_t         *slots;      // Ring buffer
    size_t          capacity;   // Current slot count
    size_t          max_capacity; // Hard upper limit (0 = unlimited)
    size_t          head;       // Next write position
    size_t          tail;       // Next read position
    size_t          count;      // Current message count
    uint64_t        drops;      // Messages dropped (hit max_capacity)
    pthread_mutex_t lock;
};

mailbox_t *mailbox_create(size_t capacity)
{
    if (capacity == 0) {
        capacity = 256;  // Default initial
    }

    mailbox_t *mb = calloc(1, sizeof(mailbox_t));
    if (!mb) return NULL;

    mb->slots = calloc(capacity, sizeof(mail_t));
    if (!mb->slots) {
        free(mb);
        return NULL;
    }

    mb->capacity = capacity;
    mb->max_capacity = 0;  // 0 = grow without limit
    mb->head = 0;
    mb->tail = 0;
    mb->count = 0;
    mb->drops = 0;

    if (pthread_mutex_init(&mb->lock, NULL) != 0) {
        free(mb->slots);
        free(mb);
        return NULL;
    }

    return mb;
}

void mailbox_destroy(mailbox_t *mb)
{
    if (!mb) return;

    // Free any unread messages
    pthread_mutex_lock(&mb->lock);
    while (mb->count > 0) {
        mail_t *mail = &mb->slots[mb->tail];
        if (mail->data) {
            free(mail->data);
        }
        mb->tail = (mb->tail + 1) % mb->capacity;
        mb->count--;
    }
    pthread_mutex_unlock(&mb->lock);

    pthread_mutex_destroy(&mb->lock);
    free(mb->slots);
    free(mb);
}

/**
 * Grow the ring buffer under the lock.
 *
 * Should Linearizes the ring into a fresh allocation of 2× capacity,
 * preserving message order (tail → head).  Returns 0 on success.
 */
static int mailbox_grow_locked(mailbox_t *mb)
{
    size_t new_cap = mb->capacity * 2;

    // Hard limit check
    if (mb->max_capacity > 0 && new_cap > mb->max_capacity) {
        new_cap = mb->max_capacity;
        if (new_cap <= mb->capacity) return -1;  // Already at max
    }

    mail_t *new_slots = calloc(new_cap, sizeof(mail_t));
    if (!new_slots) return -1;

    // Linearize: copy tail..end, then 0..head
    for (size_t i = 0; i < mb->count; i++) {
        new_slots[i] = mb->slots[(mb->tail + i) % mb->capacity];
    }

    free(mb->slots);
    mb->slots = new_slots;
    mb->capacity = new_cap;
    mb->tail = 0;
    mb->head = mb->count;

    return 0;
}

int mailbox_push(mailbox_t *mb, const mail_t *mail)
{
    if (!mb || !mail) return -1;

    pthread_mutex_lock(&mb->lock);

    if (mb->count >= mb->capacity) {
        if (mailbox_grow_locked(mb) != 0) {
            // Can't grow (at max or OOM) — drop
            mb->drops++;
            pthread_mutex_unlock(&mb->lock);
            return -1;
        }
    }

    mb->slots[mb->head] = *mail;
    mb->head = (mb->head + 1) % mb->capacity;
    mb->count++;

    pthread_mutex_unlock(&mb->lock);
    return 0;
}

int mailbox_pop(mailbox_t *mb, mail_t *mail)
{
    if (!mb || !mail) return -1;

    pthread_mutex_lock(&mb->lock);

    if (mb->count == 0) {
        pthread_mutex_unlock(&mb->lock);
        return -1;
    }

    *mail = mb->slots[mb->tail];

    // Clear slot (don't free data, ownership transfers to caller)
    memset(&mb->slots[mb->tail], 0, sizeof(mail_t));

    mb->tail = (mb->tail + 1) % mb->capacity;
    mb->count--;

    pthread_mutex_unlock(&mb->lock);
    return 0;
}

int mailbox_empty(mailbox_t *mb)
{
    if (!mb) return 1;

    pthread_mutex_lock(&mb->lock);
    int empty = (mb->count == 0);
    pthread_mutex_unlock(&mb->lock);

    return empty;
}

size_t mailbox_count(mailbox_t *mb)
{
    if (!mb) return 0;

    pthread_mutex_lock(&mb->lock);
    size_t count = mb->count;
    pthread_mutex_unlock(&mb->lock);

    return count;
}

void mailbox_set_max_capacity(mailbox_t *mb, size_t max_cap)
{
    if (!mb) return;

    pthread_mutex_lock(&mb->lock);
    mb->max_capacity = max_cap;
    pthread_mutex_unlock(&mb->lock);
}

uint64_t mailbox_drops(mailbox_t *mb)
{
    if (!mb) return 0;

    pthread_mutex_lock(&mb->lock);
    uint64_t drops = mb->drops;
    pthread_mutex_unlock(&mb->lock);

    return drops;
}