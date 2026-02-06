/**
 * network.c - Network layer implementation
 *
 * Uses ZeroMQ for messaging with a background thread.
 * Inbox notification allows event-driven wakeup instead of polling.
 */

#include "network.h"
#include "mailbox.h"
#include "wire_format.h"
#include "../public/lygus_errors.h"

#include <zmq.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

// ============================================================================
// Constants
// ============================================================================

#define RAFT_PORT_BASE  5000
#define INV_PORT_BASE   6000
#define MAX_PEERS       16
#define POLL_TIMEOUT_MS 10

// ============================================================================
// Internal Structure
// ============================================================================

struct network {
    // Configuration
    int          node_id;
    peer_info_t  peers[MAX_PEERS];
    int          num_peers;

    // ZeroMQ
    void *zmq_ctx;
    void *raft_router;              // ROUTER - receives Raft msgs
    void *raft_dealers[MAX_PEERS];  // DEALER per peer - sends Raft msgs
    void *inv_pub;                  // PUB - broadcasts INV
    void *inv_sub;                  // SUB - receives INV from all

    // Internal signaling (The Doorbell)
    void *pipe_send;                // Main thread signals here
    void *pipe_recv;                // Network thread listens here

    // Mailboxes
    mailbox_t *raft_inbox;   // Incoming Raft messages
    mailbox_t *raft_outbox;  // Outgoing Raft messages
    mailbox_t *inv_inbox;    // Incoming INV messages

    // Event loop notification
    lygus_notify_t *inbox_notify;  // Signals main thread when inbox has data

    // Thread
    pthread_t net_thread;
    volatile int running;
};

// ============================================================================
// Peer Loading
// ============================================================================

int network_load_peers(const char *path, peer_info_t *peers, int max_peers)
{
    if (!path || !peers || max_peers <= 0) {
        return -1;
    }

    FILE *f = fopen(path, "r");
    if (!f) {
        return -1;
    }

    int count = 0;
    char line[256];

    while (fgets(line, sizeof(line), f) && count < max_peers) {
        if (line[0] == '#' || line[0] == '\n') {
            continue;
        }

        int id;
        char addr[128];
        int raft_port;  // <-- ADD THIS LINE

        if (sscanf(line, "%d %127s %d", &id, addr, &raft_port) >= 3) {
            peers[count].id = id;
            peers[count].raft_port = raft_port;
            snprintf(peers[count].address, sizeof(peers[count].address), "%s", addr);
            snprintf(peers[count].raft_endpoint, sizeof(peers[count].raft_endpoint),
                     "tcp://%s:%d", addr, raft_port);

            snprintf(peers[count].inv_endpoint, sizeof(peers[count].inv_endpoint),
                     "tcp://%s:%d", addr, INV_PORT_BASE + id);
            count++;
        }
    }

    fclose(f);
    return count;
}

// ============================================================================
// Network Thread
// ============================================================================

static void *network_thread_func(void *arg)
{
    network_t *net = (network_t *)arg;
    uint8_t buf[65536];

    while (net->running) {
        // 1. Drain outbox
        mail_t mail;
        while (mailbox_pop(net->raft_outbox, &mail) == 0) {
            int peer_id = mail.peer_id;
            void *dealer = NULL;

            for (int i = 0; i < net->num_peers; i++) {
                if (net->peers[i].id == peer_id) {
                    dealer = net->raft_dealers[i];
                    break;
                }
            }

            if (dealer) {
                size_t wire_len = wire_encode(buf, mail.msg_type, net->node_id,
                                              mail.data, mail.len);

                // ZMQ_DONTWAIT ensures we never block the event loop on sending
                if (zmq_send(dealer, buf, wire_len, ZMQ_DONTWAIT) == -1) {
                    // Fail silently on send error (Raft should retry)
                }
            }

            if (mail.data) {
                free(mail.data);
            }
        }

        // 2. Poll for incoming (Traffic OR Doorbell)
        zmq_pollitem_t items[] = {
            { net->raft_router, 0, ZMQ_POLLIN, 0 },
            { net->inv_sub,     0, ZMQ_POLLIN, 0 },
            { net->pipe_recv,   0, ZMQ_POLLIN, 0 }, // Check for wake-up signal
        };

        int rc = zmq_poll(items, 3, POLL_TIMEOUT_MS);
        if (rc < 0) {
            if (errno == EINTR) continue;
            break;
        }

        // Check if we were woken up by the main thread
        if (items[2].revents & ZMQ_POLLIN) {
            uint8_t dummy[1];
            zmq_recv(net->pipe_recv, dummy, 1, ZMQ_DONTWAIT);
            // No action needed, loop handles outbox drainage
        }

        // 3. Receive Raft messages (ROUTER)
        if (items[0].revents & ZMQ_POLLIN) {
            zmq_msg_t identity, data;
            zmq_msg_init(&identity);
            zmq_msg_init(&data);

            if (zmq_msg_recv(&identity, net->raft_router, 0) >= 0 &&
                zmq_msg_recv(&data, net->raft_router, 0) >= 0) {

                size_t len = zmq_msg_size(&data);
                if (len >= WIRE_HEADER_SIZE) {
                    wire_header_t hdr;
                    const void *payload = wire_decode(zmq_msg_data(&data), len, &hdr);

                    if (payload) {
                        uint8_t *payload_copy = NULL;
                        if (hdr.len > 0) {
                            payload_copy = malloc(hdr.len);
                            if (payload_copy) memcpy(payload_copy, payload, hdr.len);
                        }

                        mail_t incoming = {
                            .peer_id = hdr.from_id,
                            .msg_type = hdr.type,
                            .len = hdr.len,
                            .data = payload_copy,
                        };

                        if (mailbox_push(net->raft_inbox, &incoming) == 0) {
                            // Successfully pushed - notify main thread
                            if (net->inbox_notify) {
                                lygus_notify_signal(net->inbox_notify);
                            }
                        } else {
                            free(payload_copy); // Drop if inbox full, raft should retry
                        }
                    }
                }
            }
            zmq_msg_close(&identity);
            zmq_msg_close(&data);
        }

        // 4. Receive INV broadcasts (SUB)
        if (items[1].revents & ZMQ_POLLIN) {
            int len = zmq_recv(net->inv_sub, buf, sizeof(buf), ZMQ_DONTWAIT);
            if (len >= WIRE_HEADER_SIZE) {
                wire_header_t hdr;
                const void *payload = wire_decode(buf, len, &hdr);

                if (payload && hdr.type == MSG_INV) {
                    uint8_t *key_copy = NULL;
                    if (hdr.len > 0) {
                        key_copy = malloc(hdr.len);
                        if (key_copy) memcpy(key_copy, payload, hdr.len);
                    }

                    mail_t incoming = {
                        .peer_id = hdr.from_id,
                        .msg_type = MSG_INV,
                        .len = hdr.len,
                        .data = key_copy,
                    };

                    if (mailbox_push(net->inv_inbox, &incoming) != 0) {
                        free(key_copy);
                    }
                    // Note: We don't notify for INV messages since they're
                    // less latency-sensitive than Raft consensus messages
                }
            }
        }
    }
    return NULL;
}

// ============================================================================
// Lifecycle
// ============================================================================

network_t *network_create(const network_config_t *cfg)
{
    if (!cfg || !cfg->peers || cfg->num_peers <= 0) return NULL;

    network_t *net = calloc(1, sizeof(network_t));
    if (!net) return NULL;

    net->node_id = cfg->node_id;
    net->num_peers = cfg->num_peers;
    memcpy(net->peers, cfg->peers, cfg->num_peers * sizeof(peer_info_t));

    // Create inbox notification for event loop integration
    net->inbox_notify = lygus_notify_create();
    if (!net->inbox_notify) {
        // Non-fatal: fall back to timer-based polling
        fprintf(stderr, "Warning: couldn't create inbox notification, "
                        "falling back to polling\n");
    }

    net->zmq_ctx = zmq_ctx_new();
    if (!net->zmq_ctx) goto fail;

    int linger = 0;

    // Create wake-up pipe
    net->pipe_send = zmq_socket(net->zmq_ctx, ZMQ_PAIR);
    net->pipe_recv = zmq_socket(net->zmq_ctx, ZMQ_PAIR);
    if (!net->pipe_send || !net->pipe_recv) goto fail;

    // Bind unique inproc address per network instance
    char pipe_addr[64];
    snprintf(pipe_addr, sizeof(pipe_addr), "inproc://net_wakeup_%p", (void*)net);

    zmq_bind(net->pipe_send, pipe_addr);
    zmq_connect(net->pipe_recv, pipe_addr);

    // Raft ROUTER
    net->raft_router = zmq_socket(net->zmq_ctx, ZMQ_ROUTER);
    if (!net->raft_router) goto fail;
    zmq_setsockopt(net->raft_router, ZMQ_LINGER, &linger, sizeof(linger));

    char bind_addr[256];
    int my_raft_port = RAFT_PORT_BASE;  // fallback
    for (int i = 0; i < net->num_peers; i++) {
        if (net->peers[i].id == net->node_id) {
            my_raft_port = net->peers[i].raft_port;
            break;
        }
    }
    snprintf(bind_addr, sizeof(bind_addr), "tcp://*:%d", my_raft_port);
    if (zmq_bind(net->raft_router, bind_addr) != 0) {
        fprintf(stderr, "%s: %s\n", lygus_strerror(LYGUS_ERR_NET), bind_addr);
        goto fail;
    }

    // Raft DEALERs
    for (int i = 0; i < net->num_peers; i++) {
        if (net->peers[i].id == net->node_id) {
            net->raft_dealers[i] = NULL;
            continue;
        }

        net->raft_dealers[i] = zmq_socket(net->zmq_ctx, ZMQ_DEALER);
        if (!net->raft_dealers[i]) goto fail;
        zmq_setsockopt(net->raft_dealers[i], ZMQ_LINGER, &linger, sizeof(linger));

        char identity[16];
        snprintf(identity, sizeof(identity), "node%d", net->node_id);
        zmq_setsockopt(net->raft_dealers[i], ZMQ_IDENTITY, identity, strlen(identity));

        if (zmq_connect(net->raft_dealers[i], net->peers[i].raft_endpoint) != 0) {
            // Log connection failure but don't abort creation (soft fail)
            fprintf(stderr, "%s: %s\n", lygus_strerror(LYGUS_ERR_CONNECT), net->peers[i].raft_endpoint);
        }
    }

    // INV PUB
    net->inv_pub = zmq_socket(net->zmq_ctx, ZMQ_PUB);
    if (!net->inv_pub) goto fail;
    zmq_setsockopt(net->inv_pub, ZMQ_LINGER, &linger, sizeof(linger));

    snprintf(bind_addr, sizeof(bind_addr), "tcp://*:%d", INV_PORT_BASE + net->node_id);
    if (zmq_bind(net->inv_pub, bind_addr) != 0) goto fail;

    // INV SUB
    net->inv_sub = zmq_socket(net->zmq_ctx, ZMQ_SUB);
    if (!net->inv_sub) goto fail;
    zmq_setsockopt(net->inv_sub, ZMQ_LINGER, &linger, sizeof(linger));
    zmq_setsockopt(net->inv_sub, ZMQ_SUBSCRIBE, "", 0);

    for (int i = 0; i < net->num_peers; i++) {
        if (net->peers[i].id == net->node_id) continue;
        zmq_connect(net->inv_sub, net->peers[i].inv_endpoint);
    }

    size_t mb_size = cfg->mailbox_size > 0 ? cfg->mailbox_size : 256;
    net->raft_inbox = mailbox_create(mb_size);
    net->raft_outbox = mailbox_create(mb_size);
    net->inv_inbox = mailbox_create(mb_size);

    if (!net->raft_inbox || !net->raft_outbox || !net->inv_inbox) goto fail;

    return net;

fail:
    network_destroy(net);
    return NULL;
}

void network_destroy(network_t *net)
{
    if (!net) return;
    network_stop(net);

    if (net->raft_router) zmq_close(net->raft_router);
    if (net->inv_pub) zmq_close(net->inv_pub);
    if (net->inv_sub) zmq_close(net->inv_sub);
    if (net->pipe_send) zmq_close(net->pipe_send);
    if (net->pipe_recv) zmq_close(net->pipe_recv);

    for (int i = 0; i < net->num_peers; i++) {
        if (net->raft_dealers[i]) zmq_close(net->raft_dealers[i]);
    }

    if (net->zmq_ctx) zmq_ctx_destroy(net->zmq_ctx);

    mailbox_destroy(net->raft_inbox);
    mailbox_destroy(net->raft_outbox);
    mailbox_destroy(net->inv_inbox);
    lygus_notify_destroy(net->inbox_notify);

    free(net);
}

int network_start(network_t *net)
{
    if (!net || net->running) return -1;
    net->running = 1;
    if (pthread_create(&net->net_thread, NULL, network_thread_func, net) != 0) {
        net->running = 0;
        return -1;
    }
    return 0;
}

void network_stop(network_t *net)
{
    if (!net || !net->running) return;
    net->running = 0;

    // Wake up thread so it can exit
    if (net->pipe_send) zmq_send(net->pipe_send, "", 0, ZMQ_DONTWAIT);

    pthread_join(net->net_thread, NULL);
}

// ============================================================================
// Send/Receive
// ============================================================================

int network_send_raft(network_t *net, int peer_id, uint8_t msg_type,
                      const void *data, size_t len)
{
    if (!net) return -1;

    uint8_t *data_copy = NULL;
    if (data && len > 0) {
        data_copy = malloc(len);
        if (!data_copy) return -1;
        memcpy(data_copy, data, len);
    }

    mail_t mail = {
        .peer_id = peer_id,
        .msg_type = msg_type,
        .len = (uint32_t)len,
        .data = data_copy,
    };

    if (mailbox_push(net->raft_outbox, &mail) != 0) {
        free(data_copy);
        return -1;
    }

    // Wake up the network thread immediately
    zmq_send(net->pipe_send, "", 0, ZMQ_DONTWAIT);

    return 0;
}

int network_recv_raft(network_t *net, int *from_id, uint8_t *msg_type,
                      void *buf, size_t buf_cap)
{
    if (!net) return -1;

    mail_t mail;
    if (mailbox_pop(net->raft_inbox, &mail) != 0) return 0;

    if (from_id) *from_id = mail.peer_id;
    if (msg_type) *msg_type = mail.msg_type;

    size_t copy_len = mail.len < buf_cap ? mail.len : buf_cap;
    if (buf && mail.data && copy_len > 0) {
        memcpy(buf, mail.data, copy_len);
    }

    free(mail.data);
    return (int)mail.len;
}

int network_broadcast_inv(network_t *net, const void *key, size_t klen)
{
    if (!net || !net->inv_pub) return -1;

    uint8_t buf[1024];
    size_t wire_len = wire_encode(buf, MSG_INV, net->node_id, key, (uint16_t)klen);
    zmq_send(net->inv_pub, buf, wire_len, ZMQ_DONTWAIT);
    return 0;
}

int network_recv_inv(network_t *net, int *from_id, void *key_buf, size_t buf_cap)
{
    if (!net) return -1;

    mail_t mail;
    if (mailbox_pop(net->inv_inbox, &mail) != 0) return 0;

    if (from_id) *from_id = mail.peer_id;

    size_t copy_len = mail.len < buf_cap ? mail.len : buf_cap;
    if (key_buf && mail.data && copy_len > 0) {
        memcpy(key_buf, mail.data, copy_len);
    }

    free(mail.data);
    return (int)mail.len;
}

// ============================================================================
// Event Loop Integration
// ============================================================================

lygus_fd_t network_get_notify_fd(const network_t *net)
{
    if (!net || !net->inbox_notify) {
        return LYGUS_INVALID_FD;
    }
    return lygus_notify_fd(net->inbox_notify);
}

void network_clear_notify(network_t *net)
{
    if (net && net->inbox_notify) {
        lygus_notify_clear(net->inbox_notify);
    }
}

// ============================================================================
// Utilities
// ============================================================================

int network_get_node_id(const network_t *net) { return net ? net->node_id : -1; }
int network_get_peer_count(const network_t *net) { return net ? net->num_peers : 0; }
int network_peer_connected(const network_t *net, int peer_id)
{
    if (!net) return 0;
    for (int i = 0; i < net->num_peers; i++) {
        if (net->peers[i].id == peer_id) return net->raft_dealers[i] != NULL;
    }
    return 0;
}