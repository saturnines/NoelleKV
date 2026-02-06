/**
 * network.h - Network layer for Lygus
 *
 * Uses ZeroMQ for messaging:
 *   - DEALER/ROUTER for Raft RPCs (reliable, async)
 *   - PUB/SUB for INV broadcasts (lossy, fire-and-forget)
 *
 * Port scheme:
 *   - Raft: 5000 + node_id
 *   - INV:  6000 + node_id
 */

#ifndef LYGUS_NETWORK_H
#define LYGUS_NETWORK_H

#include <stdint.h>
#include <stddef.h>
#include "platform/platform.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Types
// ============================================================================

typedef struct network network_t;

/**
 * Peer info (parsed from cluster.peers file)
 */
typedef struct {
    int  id;
    char address[128];      // e.g., "127.0.0.1"
    char raft_endpoint[256]; // e.g., "tcp://127.0.0.1:5001"
    char inv_endpoint[256];  // e.g., "tcp://127.0.0.1:6001"
    int raft_port;
} peer_info_t;

/**
 * Network configuration
 */
typedef struct {
    int          node_id;       // This node's ID
    peer_info_t *peers;         // All peers (including self)
    int          num_peers;     // Number of peers
    size_t       mailbox_size;  // Mailbox capacity (default: 256)
} network_config_t;

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create network layer
 *
 * Binds sockets and connects to peers.
 *
 * @param cfg  Network configuration
 * @return Network handle, or NULL on error
 */
network_t *network_create(const network_config_t *cfg);

/**
 * Destroy network layer
 *
 * Closes all sockets, drains mailboxes.
 */
void network_destroy(network_t *net);

/**
 * Start network threads
 *
 * Spawns background thread for send/recv.
 * Call after network_create().
 */
int network_start(network_t *net);

/**
 * Stop network threads
 *
 * Signals threads to exit and waits for them.
 */
void network_stop(network_t *net);

// ============================================================================
// Raft Messages (through mailbox)
// ============================================================================

/**
 * Send Raft message to a peer
 *
 * Non-blocking. Message goes into outbox, network thread sends it.
 *
 * @param net       Network handle
 * @param peer_id   Destination peer
 * @param msg_type  Message type (MSG_REQUESTVOTE_REQ, etc.)
 * @param data      Message payload
 * @param len       Payload length
 * @return 0 on success, -1 if outbox full
 */
int network_send_raft(network_t *net, int peer_id, uint8_t msg_type,
                      const void *data, size_t len);

/**
 * Receive Raft message from inbox
 *
 * Non-blocking.
 *
 * @param net       Network handle
 * @param from_id   Output: sender peer ID
 * @param msg_type  Output: message type
 * @param buf       Output buffer
 * @param buf_cap   Buffer capacity
 * @return Bytes received, 0 if empty, -1 on error
 */
int network_recv_raft(network_t *net, int *from_id, uint8_t *msg_type,
                      void *buf, size_t buf_cap);

// ============================================================================
// INV Broadcasts (PUB/SUB)
// ============================================================================

/**
 * Broadcast key invalidation to all peers
 *
 * Fire-and-forget. Lost messages are OK.
 *
 * @param net   Network handle
 * @param key   Key being invalidated
 * @param klen  Key length
 * @return 0 on success, -1 on error
 */
int network_broadcast_inv(network_t *net, const void *key, size_t klen);

/**
 * Receive INV broadcast from any peer
 *
 * Non-blocking.
 *
 * @param net       Network handle
 * @param from_id   Output: sender peer ID
 * @param key_buf   Output buffer for key
 * @param buf_cap   Buffer capacity
 * @return Key length, 0 if none, -1 on error
 */
int network_recv_inv(network_t *net, int *from_id, void *key_buf, size_t buf_cap);

// ============================================================================
// Event Loop Integration
// ============================================================================

/**
 * Get file descriptor for inbox notification
 *
 * @param net  Network handle
 * @return     Pollable file descriptor, or LYGUS_INVALID_FD if unavailable
 */
lygus_fd_t network_get_notify_fd(const network_t *net);

/**
 * Clear inbox notification
 *
 * @param net  Network handle
 */
void network_clear_notify(network_t *net);

// ============================================================================
// Utilities
// ============================================================================

/**
 * Load peers from file
 *
 * File format (one per line):
 *   <node_id> <address>
 *
 * Example:
 *   1 127.0.0.1
 *   2 127.0.0.1
 *   3 192.168.1.10
 *
 * @param path      Path to peers file
 * @param peers     Output array
 * @param max_peers Array capacity
 * @return Number of peers loaded, or -1 on error
 */
int network_load_peers(const char *path, peer_info_t *peers, int max_peers);

/**
 * Get this node's ID
 */
int network_get_node_id(const network_t *net);

/**
 * Get peer count
 */
int network_get_peer_count(const network_t *net);

/**
 * Check if connected to peer
 */
int network_peer_connected(const network_t *net, int peer_id);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_NETWORK_H