/**
* mailbox.h - Thread-safe message queue for network layer
 * Simple bounded MPSC queue.
 * If queue is full, messages are dropped (Raft will retry).
 */

#ifndef LYGUS_MAILBOX_H
#define LYGUS_MAILBOX_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct mailbox mailbox_t;

    /**
     * Message envelope
     */
    typedef struct {
        int      peer_id;   // Source (inbox) or destination (outbox)
        uint8_t  msg_type;  // Message type
        uint32_t len;       // Data length
        uint8_t *data;      // Heap-allocated, caller frees after recv
    } mail_t;

    /**
     * Create a mailbox
     *
     * @param capacity  Max messages in queue (power of 2 recommended)
     * @return Mailbox handle, or NULL on error
     */
    mailbox_t *mailbox_create(size_t capacity);

    /**
     * Destroy a mailbox
     *
     * Frees any unread messages.
     */
    void mailbox_destroy(mailbox_t *mb);

    /**
     * Push a message (non-blocking)
     *
     * @param mb    Mailbox
     * @param mail  Message to push (data is NOT copied, ownership transfers)
     * @return 0 on success, -1 if full (message dropped)
     */
    int mailbox_push(mailbox_t *mb, const mail_t *mail);

    /**
     * Pop a message (non-blocking)
     *
     * @param mb    Mailbox
     * @param mail  Output message (caller must free mail->data)
     * @return 0 on success, -1 if empty
     */
    int mailbox_pop(mailbox_t *mb, mail_t *mail);

    /**
     * Check if mailbox is empty
     */
    int mailbox_empty(mailbox_t *mb);

    /**
     * Get number of messages in mailbox
     */
    size_t mailbox_count(mailbox_t *mb);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_MAILBOX_H