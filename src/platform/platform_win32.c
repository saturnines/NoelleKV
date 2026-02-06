/**
 * platform_win32.c - Windows implementation of platform abstraction
 *
 * Covers: Windows 7+ (uses modern Win32 APIs where beneficial)
 */

#include "platform.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <io.h>
#include <fcntl.h>
#include <direct.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#pragma comment(lib, "ws2_32.lib")

// ============================================================================
// Winsock Initialization
// ============================================================================

static int g_winsock_initialized = 0;

static int ensure_winsock_init(void) {
    if (!g_winsock_initialized) {
        WSADATA wsa;
        if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
            return -1;
        }
        g_winsock_initialized = 1;
    }
    return 0;
}

// ============================================================================
// Error Helpers
// ============================================================================

int lygus_errno_is_disk_full(void) {
    // Check both errno and Windows error
    if (errno == ENOSPC) return 1;

    DWORD err = GetLastError();
    return (err == ERROR_DISK_FULL || err == ERROR_HANDLE_DISK_FULL);
}

// ============================================================================
// File Operations
// ============================================================================

lygus_fd_t lygus_file_open(const char *path, int flags, int mode) {
    if (!path) return LYGUS_INVALID_FD;

    (void)mode;  // Windows ignores Unix permission modes

    int win_flags = _O_BINARY;  // Always binary mode

    // Convert portable flags to Windows
    if ((flags & LYGUS_O_RDWR) == LYGUS_O_RDWR) {
        win_flags |= _O_RDWR;
    } else if (flags & LYGUS_O_WRONLY) {
        win_flags |= _O_WRONLY;
    } else if (flags & LYGUS_O_RDONLY) {
        win_flags |= _O_RDONLY;
    }

    if (flags & LYGUS_O_CREAT)  win_flags |= _O_CREAT;
    if (flags & LYGUS_O_EXCL)   win_flags |= _O_EXCL;
    if (flags & LYGUS_O_TRUNC)  win_flags |= _O_TRUNC;
    if (flags & LYGUS_O_APPEND) win_flags |= _O_APPEND;

    // Windows permission flags (simplified)
    int perm = _S_IREAD | _S_IWRITE;

    return _open(path, win_flags, perm);
}

int lygus_file_close(lygus_fd_t fd) {
    if (fd < 0) return -1;
    return _close(fd);
}

int64_t lygus_file_read(lygus_fd_t fd, void *buf, size_t len) {
    if (fd < 0 || !buf) return -1;

    // Windows _read takes unsigned int, so we may need multiple calls for large reads
    if (len > (size_t)INT_MAX) {
        len = (size_t)INT_MAX;
    }

    return (int64_t)_read(fd, buf, (unsigned int)len);
}

int64_t lygus_file_write(lygus_fd_t fd, const void *buf, size_t len) {
    if (fd < 0 || !buf) return -1;

    if (len > (size_t)INT_MAX) {
        len = (size_t)INT_MAX;
    }

    return (int64_t)_write(fd, buf, (unsigned int)len);
}

int64_t lygus_file_pread(lygus_fd_t fd, void *buf, size_t len, uint64_t offset) {
    if (fd < 0 || !buf) return -1;

    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) return -1;

    OVERLAPPED overlapped = {0};
    overlapped.Offset = (DWORD)(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = (DWORD)(offset >> 32);

    DWORD bytesRead = 0;
    if (!ReadFile(h, buf, (DWORD)len, &bytesRead, &overlapped)) {
        DWORD err = GetLastError();
        if (err == ERROR_HANDLE_EOF) return 0;
        return -1;
    }

    return (int64_t)bytesRead;
}

int64_t lygus_file_pwrite(lygus_fd_t fd, const void *buf, size_t len, uint64_t offset) {
    if (fd < 0 || !buf) return -1;

    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) return -1;

    OVERLAPPED ov = {0};
    ov.Offset = (DWORD)(offset & 0xFFFFFFFF);
    ov.OffsetHigh = (DWORD)(offset >> 32);

    DWORD bytes_written = 0;
    if (!WriteFile(h, buf, (DWORD)len, &bytes_written, &ov)) {
        return -1;
    }

    return (int64_t)bytes_written;
}

int64_t lygus_file_seek(lygus_fd_t fd, int64_t offset, int whence) {
    if (fd < 0) return -1;

    int win_whence;
    switch (whence) {
        case LYGUS_SEEK_SET: win_whence = SEEK_SET; break;
        case LYGUS_SEEK_CUR: win_whence = SEEK_CUR; break;
        case LYGUS_SEEK_END: win_whence = SEEK_END; break;
        default: return -1;
    }

    return _lseeki64(fd, offset, win_whence);
}

int lygus_file_sync(lygus_fd_t fd) {
    if (fd < 0) return -1;
    return _commit(fd);
}

int lygus_file_barrier(lygus_fd_t fd) {
    if (fd < 0) return -1;

    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) return -1;

    BY_HANDLE_FILE_INFORMATION info;
    if (!GetFileInformationByHandle(h, &info)) {
        return -1;
    }
    return 0;
}

int lygus_file_truncate(lygus_fd_t fd, uint64_t size) {
    if (fd < 0) return -1;

    // Save current position
    int64_t old_pos = _lseeki64(fd, 0, SEEK_CUR);
    if (old_pos < 0) return -1;

    // Seek to desired size
    if (_lseeki64(fd, (int64_t)size, SEEK_SET) < 0) {
        return -1;
    }

    // Truncate at current position
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) {
        _lseeki64(fd, old_pos, SEEK_SET);
        return -1;
    }

    if (!SetEndOfFile(h)) {
        _lseeki64(fd, old_pos, SEEK_SET);
        return -1;
    }

    // Restore position (or stay at end if old_pos > size)
    if (old_pos < (int64_t)size) {
        _lseeki64(fd, old_pos, SEEK_SET);
    }

    return 0;
}

int lygus_file_lock(lygus_fd_t fd, int flags) {
    if (fd < 0) return -1;

    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) return -1;

    OVERLAPPED ov = {0};

    // Handle unlock
    if (flags & LYGUS_LOCK_UN) {
        if (!UnlockFileEx(h, 0, 1, 0, &ov)) {
            return -1;
        }
        return 0;
    }

    // Build lock flags
    DWORD lock_flags = 0;

    if (flags & LYGUS_LOCK_EX) {
        lock_flags |= LOCKFILE_EXCLUSIVE_LOCK;
    }
    // LYGUS_LOCK_SH = shared lock, no LOCKFILE_EXCLUSIVE_LOCK flag

    if (flags & LYGUS_LOCK_NB) {
        lock_flags |= LOCKFILE_FAIL_IMMEDIATELY;
    }

    if (!LockFileEx(h, lock_flags, 0, 1, 0, &ov)) {
        return -1;
    }

    return 0;
}

int lygus_file_unlock(lygus_fd_t fd) {
    return lygus_file_lock(fd, LYGUS_LOCK_UN);
}

int64_t lygus_file_size(lygus_fd_t fd) {
    if (fd < 0) return -1;

    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) return -1;

    LARGE_INTEGER size;
    if (!GetFileSizeEx(h, &size)) {
        return -1;
    }

    return (int64_t)size.QuadPart;
}

// ============================================================================
// Filesystem Operations
// ============================================================================

int lygus_mkdir(const char *path, int mode) {
    if (!path) return -1;

    (void)mode;  // Windows ignores Unix permission modes

    int ret = _mkdir(path);
    if (ret < 0 && errno == EEXIST) {
        return 0;  // Directory already exists is OK
    }
    return ret;
}

int lygus_unlink(const char *path) {
    if (!path) return -1;
    return _unlink(path);
}

int lygus_rename(const char *old_path, const char *new_path) {
    if (!old_path || !new_path) return -1;

    // Windows rename fails if target exists, so delete it first
    _unlink(new_path);  // Ignore errors (file may not exist)

    return rename(old_path, new_path);
}

int lygus_path_exists(const char *path) {
    if (!path) return 0;
    return _access(path, 0) == 0;
}

// ============================================================================
// Directory Iteration
// ============================================================================

struct lygus_dir {
    HANDLE          handle;
    WIN32_FIND_DATAA find_data;
    int             first;
    int             done;
};

lygus_dir_t* lygus_dir_open(const char *path) {
    if (!path) return NULL;

    lygus_dir_t *dir = malloc(sizeof(lygus_dir_t));
    if (!dir) return NULL;

    // Append \* for FindFirstFile
    char pattern[MAX_PATH];
    snprintf(pattern, sizeof(pattern), "%s\\*", path);

    dir->handle = FindFirstFileA(pattern, &dir->find_data);
    if (dir->handle == INVALID_HANDLE_VALUE) {
        free(dir);
        return NULL;
    }

    dir->first = 1;
    dir->done = 0;

    return dir;
}

const char* lygus_dir_read(lygus_dir_t *dir) {
    if (!dir || dir->done) return NULL;

    while (1) {
        const char *name;

        if (dir->first) {
            // Return first entry from FindFirstFile
            dir->first = 0;
            name = dir->find_data.cFileName;
        } else {
            // Get next entry
            if (!FindNextFileA(dir->handle, &dir->find_data)) {
                dir->done = 1;
                return NULL;
            }
            name = dir->find_data.cFileName;
        }

        // Skip . and ..
        if (name[0] == '.') {
            if (name[1] == '\0') continue;
            if (name[1] == '.' && name[2] == '\0') continue;
        }

        return name;
    }
}

void lygus_dir_close(lygus_dir_t *dir) {
    if (!dir) return;
    if (dir->handle != INVALID_HANDLE_VALUE) {
        FindClose(dir->handle);
    }
    free(dir);
}

// ============================================================================
// Path Utilities
// ============================================================================

const char* lygus_path_separator(void) {
    return "\\";
}

int lygus_path_join(char *out, size_t out_len, const char *dir, const char *file) {
    if (!out || !dir || !file || out_len == 0) return -1;

    // Use / which Windows also accepts
    int n = snprintf(out, out_len, "%s/%s", dir, file);
    if (n < 0 || (size_t)n >= out_len) {
        return -1;
    }
    return 0;
}

// ============================================================================
// Async Process (synchronous fallback - no fork on Windows)
// ============================================================================

struct lygus_async_proc {
    int completed;
    int success;
};

int lygus_async_fork_supported(void) {
    return 0;  // Windows does not support fork()
}

lygus_async_proc_t* lygus_async_fork(int (*func)(void *ctx), void *ctx, int *is_child) {
    if (!func || !is_child) return NULL;

    *is_child = 0;  // Never a child on Windows

    // Run synchronously
    int ret = func(ctx);

    lygus_async_proc_t *proc = malloc(sizeof(lygus_async_proc_t));
    if (!proc) return NULL;

    proc->completed = 1;
    proc->success = (ret == 0) ? 1 : 0;

    return proc;
}

int lygus_async_poll(lygus_async_proc_t *proc, int *done, int *success) {
    if (!proc || !done || !success) return -1;

    // Always complete on Windows (synchronous)
    *done = proc->completed;
    *success = proc->success;
    return 0;
}

int lygus_async_wait(lygus_async_proc_t *proc, int *success) {
    if (!proc || !success) return -1;

    // Already complete on Windows
    *success = proc->success;
    return 0;
}

void lygus_async_free(lygus_async_proc_t *proc) {
    free(proc);
}

// ============================================================================
// Threading Utilities
// ============================================================================

uint32_t lygus_thread_id(void) {
    return (uint32_t)GetCurrentThreadId();
}

void lygus_sleep_us(uint64_t us) {
    // Windows Sleep() takes milliseconds
    DWORD ms = (DWORD)((us + 999) / 1000);  // Round up
    if (ms == 0) ms = 1;
    Sleep(ms);
}

// ============================================================================
// Event Notification
// ============================================================================

struct lygus_notify {
    SOCKET read_sock;
    SOCKET write_sock;
    struct sockaddr_in addr;
};

lygus_notify_t *lygus_notify_create(void) {
    if (ensure_winsock_init() < 0) {
        return NULL;
    }

    lygus_notify_t *notify = malloc(sizeof(lygus_notify_t));
    if (!notify) return NULL;

    // Create a UDP socket pair using loopback
    notify->read_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (notify->read_sock == INVALID_SOCKET) {
        free(notify);
        return NULL;
    }

    // Bind to loopback with ephemeral port
    memset(&notify->addr, 0, sizeof(notify->addr));
    notify->addr.sin_family = AF_INET;
    notify->addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    notify->addr.sin_port = 0;

    if (bind(notify->read_sock, (struct sockaddr *)&notify->addr,
             sizeof(notify->addr)) == SOCKET_ERROR) {
        closesocket(notify->read_sock);
        free(notify);
        return NULL;
    }

    // Get the bound port
    int addrlen = sizeof(notify->addr);
    if (getsockname(notify->read_sock, (struct sockaddr *)&notify->addr,
                    &addrlen) == SOCKET_ERROR) {
        closesocket(notify->read_sock);
        free(notify);
        return NULL;
    }

    // Create write socket and connect to read socket
    notify->write_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (notify->write_sock == INVALID_SOCKET) {
        closesocket(notify->read_sock);
        free(notify);
        return NULL;
    }

    if (connect(notify->write_sock, (struct sockaddr *)&notify->addr,
                sizeof(notify->addr)) == SOCKET_ERROR) {
        closesocket(notify->read_sock);
        closesocket(notify->write_sock);
        free(notify);
        return NULL;
    }

    // Set read socket non-blocking
    u_long mode = 1;
    ioctlsocket(notify->read_sock, FIONBIO, &mode);

    return notify;
}

void lygus_notify_destroy(lygus_notify_t *notify) {
    if (!notify) return;

    if (notify->read_sock != INVALID_SOCKET) {
        closesocket(notify->read_sock);
    }
    if (notify->write_sock != INVALID_SOCKET) {
        closesocket(notify->write_sock);
    }

    free(notify);
}

lygus_fd_t lygus_notify_fd(const lygus_notify_t *notify) {
    if (!notify) return LYGUS_INVALID_FD;
    // Note: This returns a SOCKET cast to int. Works with select().
    return (lygus_fd_t)notify->read_sock;
}

int lygus_notify_signal(lygus_notify_t *notify) {
    if (!notify) return -1;

    char c = 1;
    if (send(notify->write_sock, &c, 1, 0) == SOCKET_ERROR) {
        int err = WSAGetLastError();
        if (err == WSAEWOULDBLOCK) return 0;
        return -1;
    }
    return 0;
}

int lygus_notify_clear(lygus_notify_t *notify) {
    if (!notify) return -1;

    char buf[64];
    while (recv(notify->read_sock, buf, sizeof(buf), 0) > 0) {
        // Drain
    }
    return 0;
}

// ============================================================================
// Time Utilities
// ============================================================================

uint64_t lygus_monotonic_ns(void) {
    static LARGE_INTEGER freq = {0};
    static int freq_initialized = 0;

    if (!freq_initialized) {
        QueryPerformanceFrequency(&freq);
        freq_initialized = 1;
    }

    LARGE_INTEGER counter;
    QueryPerformanceCounter(&counter);

    // Convert to nanoseconds carefully to avoid overflow
    uint64_t seconds = counter.QuadPart / freq.QuadPart;
    uint64_t remainder = counter.QuadPart % freq.QuadPart;

    return seconds * 1000000000ULL + (remainder * 1000000000ULL) / freq.QuadPart;
}

uint64_t lygus_realtime_ns(void) {
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);

    // FILETIME is 100-nanosecond intervals since 1601-01-01
    // Unix epoch is 1970-01-01
    ULARGE_INTEGER uli;
    uli.LowPart = ft.dwLowDateTime;
    uli.HighPart = ft.dwHighDateTime;

    // Subtract Windows-to-Unix epoch difference
    const uint64_t EPOCH_DIFF = 116444736000000000ULL;
    uint64_t unix_100ns = uli.QuadPart - EPOCH_DIFF;

    // Convert 100-ns to nanoseconds
    return unix_100ns * 100;
}

// ============================================================================
// Socket Abstraction
// ============================================================================
// Code smell here will fix layer
int lygus_socket_init(void) {
    return ensure_winsock_init();
}

void lygus_socket_cleanup(void) {
    if (g_winsock_initialized) {
        WSACleanup();
        g_winsock_initialized = 0;
    }
}

lygus_socket_t lygus_socket_tcp(void) {
    if (ensure_winsock_init() < 0) {
        return LYGUS_INVALID_SOCKET;
    }
    return socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
}

int lygus_socket_close(lygus_socket_t sock) {
    if (sock == LYGUS_INVALID_SOCKET) return -1;
    return closesocket(sock) == 0 ? 0 : -1;
}

int lygus_socket_set_nonblocking(lygus_socket_t sock) {
    if (sock == LYGUS_INVALID_SOCKET) return -1;

    u_long mode = 1;
    return ioctlsocket(sock, FIONBIO, &mode) == 0 ? 0 : -1;
}

int lygus_socket_set_reuseaddr(lygus_socket_t sock) {
    if (sock == LYGUS_INVALID_SOCKET) return -1;

    int opt = 1;
    return setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt, sizeof(opt));
}

int lygus_socket_set_nodelay(lygus_socket_t sock) {
    if (sock == LYGUS_INVALID_SOCKET) return -1;

    int opt = 1;
    return setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (const char *)&opt, sizeof(opt));
}

int lygus_socket_bind(lygus_socket_t sock, const char *addr, uint16_t port) {
    if (sock == LYGUS_INVALID_SOCKET) return -1;

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);

    if (addr && addr[0] != '\0') {
        // Use InetPtonA for Windows
        if (InetPtonA(AF_INET, addr, &sa.sin_addr) <= 0) {
            return -1;
        }
    } else {
        sa.sin_addr.s_addr = INADDR_ANY;
    }

    return bind(sock, (struct sockaddr *)&sa, sizeof(sa)) == 0 ? 0 : -1;
}

int lygus_socket_listen(lygus_socket_t sock, int backlog) {
    if (sock == LYGUS_INVALID_SOCKET) return -1;
    return listen(sock, backlog) == 0 ? 0 : -1;
}

lygus_socket_t lygus_socket_accept(lygus_socket_t sock, char *addr_out, size_t addr_len) {
    if (sock == LYGUS_INVALID_SOCKET) return LYGUS_INVALID_SOCKET;

    struct sockaddr_in client_addr;
    int client_len = sizeof(client_addr);

    SOCKET client = accept(sock, (struct sockaddr *)&client_addr, &client_len);
    if (client == INVALID_SOCKET) {
        return LYGUS_INVALID_SOCKET;
    }

    if (addr_out && addr_len > 0) {
        char ip[INET_ADDRSTRLEN];
        InetNtopA(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
        snprintf(addr_out, addr_len, "%s:%d", ip, ntohs(client_addr.sin_port));
    }

    return (lygus_socket_t)client;
}

int64_t lygus_socket_recv(lygus_socket_t sock, void *buf, size_t len) {
    if (sock == LYGUS_INVALID_SOCKET || !buf) return -1;

    int n = recv(sock, (char *)buf, (int)len, 0);
    if (n == SOCKET_ERROR) {
        int err = WSAGetLastError();
        if (err == WSAEWOULDBLOCK) {
            return -2;  // Would block
        }
        return -1;
    }
    return (int64_t)n;
}

int64_t lygus_socket_send(lygus_socket_t sock, const void *buf, size_t len) {
    if (sock == LYGUS_INVALID_SOCKET || !buf) return -1;

    int n = send(sock, (const char *)buf, (int)len, 0);
    if (n == SOCKET_ERROR) {
        int err = WSAGetLastError();
        if (err == WSAEWOULDBLOCK) {
            return -2;  // Would block
        }
        return -1;
    }
    return (int64_t)n;
}

int lygus_socket_would_block(void) {
    return (WSAGetLastError() == WSAEWOULDBLOCK);
}

lygus_fd_t lygus_socket_to_fd(lygus_socket_t sock) {
    // Return as int for compatibility with event loop code that uses select()
    return (lygus_fd_t)sock;
}