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
    size_t          capacity;   // Total slots
    size_t          head;       // Next write position
    size_t          tail;       // Next read position
    size_t          count;      // Current message count
    pthread_mutex_t lock;
};

mailbox_t *mailbox_create(size_t capacity)
{
    if (capacity == 0) {
        capacity = 256;  // Default
    }

    mailbox_t *mb = calloc(1, sizeof(mailbox_t));
    if (!mb) return NULL;

    mb->slots = calloc(capacity, sizeof(mail_t));
    if (!mb->slots) {
        free(mb);
        return NULL;
    }

    mb->capacity = capacity;
    mb->head = 0;
    mb->tail = 0;
    mb->count = 0;

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

int mailbox_push(mailbox_t *mb, const mail_t *mail)
{
    if (!mb || !mail) return -1;

    pthread_mutex_lock(&mb->lock);

    if (mb->count >= mb->capacity) {
        pthread_mutex_unlock(&mb->lock);
        return -1;
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