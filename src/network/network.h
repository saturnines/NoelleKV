/**
 * network.h - Network layer for Lygus
 *
 * Uses ZeroMQ for messaging:
 *   - DEALER/ROUTER for Raft RPCs + gossip (reliable, async)
 *   - PUB/SUB for INV broadcasts (lossy, fire-and-forget)
 *
 * Port scheme:
 *   - Raft + Gossip: 5000 + node_id  (shared DEALER/ROUTER)
 *   - INV:           6000 + node_id
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
 * Peer info
 */
typedef struct {
    int  id;
    char address[128];
    char raft_endpoint[256];
    char inv_endpoint[256];
    int raft_port;
} peer_info_t;

/**
 * Network configuration
 */
typedef struct {
    int          node_id;
    peer_info_t *peers;
    int          num_peers;
    size_t       mailbox_size;
} network_config_t;

// ============================================================================
// Lifecycle
// ============================================================================

network_t *network_create(const network_config_t *cfg);
void network_destroy(network_t *net);
int network_start(network_t *net);
void network_stop(network_t *net);

// ============================================================================
// Raft Messages (through mailbox)
// ============================================================================

int network_send_raft(network_t *net, int peer_id, uint8_t msg_type,
                      const void *data, size_t len);

int network_recv_raft(network_t *net, int *from_id, uint8_t *msg_type,
                      void *buf, size_t buf_cap);

// ============================================================================
// INV Broadcasts (PUB/SUB)
// ============================================================================

int network_broadcast_inv(network_t *net, const void *key, size_t klen);
int network_recv_inv(network_t *net, int *from_id, void *key_buf, size_t buf_cap);

// ============================================================================
// Gossip Messages
// ============================================================================

/**
 * Receive gossip message from inbox
 *
 * Gossip messages (types 30-35) arrive through the same DEALER/ROUTER
 * transport as Raft RPCs. This function checks the inbox for gossip
 * messages specifically.
 *
 * @param net       Network handle
 * @param from_id   Output: sender peer ID
 * @param msg_type  Output: gossip message type (30-35)
 * @param buf       Output buffer
 * @param buf_cap   Buffer capacity
 * @return Bytes received, 0 if no gossip messages, -1 on error
 */
int network_recv_gossip(network_t *net, int *from_id, uint8_t *msg_type,
                        void *buf, size_t buf_cap);

// ============================================================================
// Event Loop Integration
// ============================================================================

lygus_fd_t network_get_notify_fd(const network_t *net);
void network_clear_notify(network_t *net);

// ============================================================================
// Utilities
// ============================================================================

int network_load_peers(const char *path, peer_info_t *peers, int max_peers);
int network_get_node_id(const network_t *net);
int network_get_peer_count(const network_t *net);
int network_peer_connected(const network_t *net, int peer_id);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_NETWORK_H