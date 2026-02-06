#ifndef LYGUS_PLATFORM_H
#define LYGUS_PLATFORM_H

/**
 * Lygus Platform Abstraction Layer
 * Build system selects platform_posix.c or platform_win32.c
 */

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Types
// ============================================================================

/** File descriptor (opaque, but int-compatible for simplicity) */
typedef int lygus_fd_t;
#define LYGUS_INVALID_FD (-1)

/** Socket type - int on POSIX, but needs to handle SOCKET on Windows */
#ifdef _WIN32
    typedef uintptr_t lygus_socket_t;
    #define LYGUS_INVALID_SOCKET (~(lygus_socket_t)0)
#else
    typedef int lygus_socket_t;
    #define LYGUS_INVALID_SOCKET (-1)
#endif

/** Directory handle (opaque) */
typedef struct lygus_dir lygus_dir_t;

/** Async process handle for fork-based snapshots (opaque) */
typedef struct lygus_async_proc lygus_async_proc_t;

// ============================================================================
// Open Flags (portable subset)
// ============================================================================

#define LYGUS_O_RDONLY   0x0001
#define LYGUS_O_WRONLY   0x0002
#define LYGUS_O_RDWR     0x0004
#define LYGUS_O_CREAT    0x0010
#define LYGUS_O_EXCL     0x0020
#define LYGUS_O_TRUNC    0x0040
#define LYGUS_O_APPEND   0x0080

// ============================================================================
// Seek Whence (portable)
// ============================================================================

#define LYGUS_SEEK_SET   0
#define LYGUS_SEEK_CUR   1
#define LYGUS_SEEK_END   2

// ============================================================================
// Lock Flags
// ============================================================================

#define LYGUS_LOCK_SH    0x01   // Shared lock (read)
#define LYGUS_LOCK_EX    0x02   // Exclusive lock (write)
#define LYGUS_LOCK_NB    0x04   // Non-blocking (fail immediately if can't lock)
#define LYGUS_LOCK_UN    0x08   // Unlock

// ============================================================================
// File Operations
// ============================================================================

/**
 * Open a file
 *
 * @param path   File path
 * @param flags  LYGUS_O_* flags (OR'd together)
 * @param mode   Permission mode (e.g., 0644), ignored on Windows
 * @return       File descriptor, or LYGUS_INVALID_FD on error
 */
lygus_fd_t lygus_file_open(const char *path, int flags, int mode);

/**
 * Close a file descriptor
 *
 * @param fd  File descriptor
 * @return    0 on success, -1 on error
 */
int lygus_file_close(lygus_fd_t fd);

/**
 * Read from file descriptor
 *
 * @param fd   File descriptor
 * @param buf  Buffer to read into
 * @param len  Maximum bytes to read
 * @return     Bytes read (>=0), or -1 on error
 */
int64_t lygus_file_read(lygus_fd_t fd, void *buf, size_t len);

/**
 * Write to file descriptor
 *
 * @param fd   File descriptor
 * @param buf  Buffer to write from
 * @param len  Bytes to write
 * @return     Bytes written (>=0), or -1 on error
 */
int64_t lygus_file_write(lygus_fd_t fd, const void *buf, size_t len);

/**
 * Read from file at specific offset (does not change file position)
 *
 * @param fd      File descriptor
 * @param buf     Buffer to read into
 * @param len     Maximum bytes to read
 * @param offset  Absolute file offset
 * @return        Bytes read (>=0), or -1 on error
 */
int64_t lygus_file_pread(lygus_fd_t fd, void *buf, size_t len, uint64_t offset);

/**
 * Write to file at specific offset (does not change file position)
 *
 * @param fd      File descriptor
 * @param buf     Buffer to write from
 * @param len     Bytes to write
 * @param offset  Absolute file offset
 * @return        Bytes written (>=0), or -1 on error
 */
int64_t lygus_file_pwrite(lygus_fd_t fd, const void *buf, size_t len, uint64_t offset);

/**
 * Seek to position in file
 *
 * @param fd      File descriptor
 * @param offset  Offset (interpretation depends on whence)
 * @param whence  LYGUS_SEEK_SET, LYGUS_SEEK_CUR, or LYGUS_SEEK_END
 * @return        New absolute position, or -1 on error
 */
int64_t lygus_file_seek(lygus_fd_t fd, int64_t offset, int whence);

/**
 * Sync file data to disk (fdatasync semantics - data only, not metadata)
 *
 * @param fd  File descriptor
 * @return    0 on success, -1 on error
 */
int lygus_file_sync(lygus_fd_t fd);

/**
 * Truncate file to specified length
 *
 * @param fd    File descriptor
 * @param size  New file size
 * @return      0 on success, -1 on error
 */
int lygus_file_truncate(lygus_fd_t fd, uint64_t size);

/**
 * Acquire lock on file
 *
 * @param fd     File descriptor
 * @param flags  LYGUS_LOCK_* flags (EX or SH, optionally | NB)
 * @return       0 on success, -1 if already locked or error
 */
int lygus_file_lock(lygus_fd_t fd, int flags);

/**
 * Release lock on file
 *
 * @param fd  File descriptor
 * @return    0 on success, -1 on error
 */
int lygus_file_unlock(lygus_fd_t fd);

/**
 * Get file size
 *
 * @param fd  File descriptor
 * @return    File size in bytes, or -1 on error
 */
int64_t lygus_file_size(lygus_fd_t fd);

// ============================================================================
// Filesystem Operations
// ============================================================================

/**
 * Create directory (single level, not recursive)
 *
 * @param path  Directory path
 * @param mode  Permission mode (e.g., 0755), ignored on Windows
 * @return      0 on success, -1 on error (EEXIST is NOT an error)
 */
int lygus_mkdir(const char *path, int mode);

/**
 * Delete a file
 *
 * @param path  File path
 * @return      0 on success, -1 on error
 */
int lygus_unlink(const char *path);

/**
 * Rename/move a file (atomic if same filesystem)
 *
 * @param old_path  Current path
 * @param new_path  New path
 * @return          0 on success, -1 on error
 *
 * Note: On Windows, target is deleted first if it exists
 */
int lygus_rename(const char *old_path, const char *new_path);

/**
 * Check if path exists
 *
 * @param path  Path to check
 * @return      1 if exists, 0 if not
 */
int lygus_path_exists(const char *path);

/**
 * Force a visibility barrier for file metadata.
 *
 * Ensures that the kernel refreshes its view of the file size and
 * metadata. Call this if a read fails on a file that is currently
 * being grown by another handle.
 *
 * @param fd  File descriptor
 * @return    0 on success, -1 on error
 */
int lygus_file_barrier(lygus_fd_t fd);

// ============================================================================
// Directory Iteration
// ============================================================================

/**
 * Open directory for iteration
 *
 * @param path  Directory path
 * @return      Directory handle, or NULL on error
 */
lygus_dir_t* lygus_dir_open(const char *path);

/**
 * Read next entry from directory
 *
 * @param dir  Directory handle
 * @return     Filename (without path), or NULL if done/error
 *
 * Note: Skips "." and ".." automatically
 * Note: Returned string is valid until next lygus_dir_read() or lygus_dir_close()
 */
const char* lygus_dir_read(lygus_dir_t *dir);

/**
 * Close directory handle
 *
 * @param dir  Directory handle
 */
void lygus_dir_close(lygus_dir_t *dir);

// ============================================================================
// Path Utilities
// ============================================================================

/**
 * Get platform path separator
 *
 * @return  "/" on POSIX, "\\" on Windows
 */
const char* lygus_path_separator(void);

/**
 * Join directory and filename into path
 *
 * @param out      Output buffer
 * @param out_len  Output buffer size
 * @param dir      Directory path
 * @param file     Filename
 * @return         0 on success, -1 if buffer too small
 */
int lygus_path_join(char *out, size_t out_len, const char *dir, const char *file);

// ============================================================================
// Async Process (for fork-based snapshots)
// ============================================================================

/**
 * Check if async fork is supported on this platform
 *
 * @return  1 if fork() available (POSIX), 0 if not (Windows)
 */
int lygus_async_fork_supported(void);

/**
 * Fork a child process to run a function
 *
 * On POSIX: Actually forks, child runs func() and exits with its return value
 * On Windows: Runs func() synchronously in current process
 *
 * @param func      Function to run (should return 0 on success, 1 on failure)
 * @param ctx       Context passed to func
 * @param is_child  Output: set to 1 in child process (POSIX only), always 0 on Windows
 * @return          Async handle (parent), or NULL on error
 *
 * Note: On POSIX, child process should NOT return from this - it calls _exit()
 */
lygus_async_proc_t* lygus_async_fork(int (*func)(void *ctx), void *ctx, int *is_child);

/**
 * Poll for async process completion (non-blocking)
 *
 * @param proc     Async handle
 * @param done     Output: 1 if finished, 0 if still running
 * @param success  Output: 1 if func returned 0, 0 otherwise (only valid if done=1)
 * @return         0 on success, -1 on error
 */
int lygus_async_poll(lygus_async_proc_t *proc, int *done, int *success);

/**
 * Wait for async process to complete (blocking)
 *
 * @param proc     Async handle
 * @param success  Output: 1 if func returned 0, 0 otherwise
 * @return         0 on success, -1 on error
 */
int lygus_async_wait(lygus_async_proc_t *proc, int *success);

/**
 * Free async process handle
 *
 * @param proc  Async handle (safe to pass NULL)
 */
void lygus_async_free(lygus_async_proc_t *proc);

// ============================================================================
// Threading Utilities
// ============================================================================

/**
 * Get current thread ID (for logging)
 *
 * @return  Thread ID (platform-specific but consistent within process)
 */
uint32_t lygus_thread_id(void);

/**
 * Sleep for specified microseconds
 *
 * @param us  Microseconds to sleep
 */
void lygus_sleep_us(uint64_t us);

// ============================================================================
// Event Notification (for waking up event loops)
// ============================================================================

/**
 * Notification handle for cross thread signaling
 * Uses eventfd on Linux, pipes elsewhere.
 */
typedef struct lygus_notify lygus_notify_t;

/**
 * Create notification object
 *
 * @return  Notification handle, or NULL on error
 */
lygus_notify_t *lygus_notify_create(void);

/**
 * Destroy notification object
 *
 * @param notify  Notification handle (safe to pass NULL)
 */
void lygus_notify_destroy(lygus_notify_t *notify);

/**
 * Get pollable file descriptor
 *
 * This fd becomes readable when lygus_notify_signal() is called.
 * @param notify  Notification handle
 * @return        File descriptor, or LYGUS_INVALID_FD on error
 */
lygus_fd_t lygus_notify_fd(const lygus_notify_t *notify);

/**
 * Signal the notification
 * @param notify  Notification handle
 * @return        0 on success, -1 on error
 */
int lygus_notify_signal(lygus_notify_t *notify);

/**
 * Clear the notification
 * @param notify  Notification handle
 * @return        0 on success, -1 on error
 */
int lygus_notify_clear(lygus_notify_t *notify);

// ============================================================================
// Time Utilities
// ============================================================================

uint64_t lygus_monotonic_ns(void);
uint64_t lygus_realtime_ns(void);

// ============================================================================
// Error Helpers
// ============================================================================

/**
 * Check if last I/O error was "disk full" (ENOSPC)
 *
 * Call immediately after a write/sync returns -1.
 * Useful for distinguishing "pause and wait for space" vs "disk dead".
 *
 * @return  1 if last error was disk full, 0 otherwise
 */
int lygus_errno_is_disk_full(void);

// ============================================================================
// Socket Abstraction Layer
// ============================================================================

/**
 * Initialize socket subsystem (required on Windows, no-op on POSIX)
 *
 * @return  0 on success, -1 on error
 */
int lygus_socket_init(void);

/**
 * Cleanup socket subsystem (required on Windows, no-op on POSIX)
 */
void lygus_socket_cleanup(void);

/**
 * Create a TCP socket
 *
 * @return  Socket handle, or LYGUS_INVALID_SOCKET on error
 */
lygus_socket_t lygus_socket_tcp(void);

/**
 * Close a socket
 *
 * @param sock  Socket handle
 * @return      0 on success, -1 on error
 */
int lygus_socket_close(lygus_socket_t sock);

/**
 * Set socket to non-blocking mode
 *
 * @param sock  Socket handle
 * @return      0 on success, -1 on error
 */
int lygus_socket_set_nonblocking(lygus_socket_t sock);

/**
 * Enable SO_REUSEADDR on socket
 *
 * @param sock  Socket handle
 * @return      0 on success, -1 on error
 */
int lygus_socket_set_reuseaddr(lygus_socket_t sock);

/**
 * Enable TCP_NODELAY (disable Nagle's algorithm)
 *
 * @param sock  Socket handle
 * @return      0 on success, -1 on error
 */
int lygus_socket_set_nodelay(lygus_socket_t sock);

/**
 * Bind socket to address and port
 *
 * @param sock  Socket handle
 * @param addr  Address string (NULL or "" for INADDR_ANY)
 * @param port  Port number
 * @return      0 on success, -1 on error
 */
int lygus_socket_bind(lygus_socket_t sock, const char *addr, uint16_t port);

/**
 * Start listening for connections
 *
 * @param sock     Socket handle
 * @param backlog  Connection backlog
 * @return         0 on success, -1 on error
 */
int lygus_socket_listen(lygus_socket_t sock, int backlog);

/**
 * Accept incoming connection
 *
 * @param sock      Listening socket
 * @param addr_out  Output buffer for remote address string (can be NULL)
 * @param addr_len  Size of addr_out buffer
 * @return          New socket for connection, or LYGUS_INVALID_SOCKET on error/would-block
 */
lygus_socket_t lygus_socket_accept(lygus_socket_t sock, char *addr_out, size_t addr_len);

/**
 * Receive data from socket
 *
 * @param sock  Socket handle
 * @param buf   Buffer to receive into
 * @param len   Maximum bytes to receive
 * @return      Bytes received (>0), 0 on disconnect, -1 on error, -2 on would-block
 */
int64_t lygus_socket_recv(lygus_socket_t sock, void *buf, size_t len);

/**
 * Send data on socket
 *
 * @param sock  Socket handle
 * @param buf   Data to send
 * @param len   Bytes to send
 * @return      Bytes sent (>=0), -1 on error, -2 on would-block
 */
int64_t lygus_socket_send(lygus_socket_t sock, const void *buf, size_t len);

/**
 * Check if last socket error was "would block" (EAGAIN/EWOULDBLOCK)
 *
 * @return  1 if would block, 0 otherwise
 */
int lygus_socket_would_block(void);

/**
 * Get socket as file descriptor for polling
 *
 * @param sock  Socket handle
 * @return      File descriptor suitable for event loop
 */
lygus_fd_t lygus_socket_to_fd(lygus_socket_t sock);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_PLATFORM_H