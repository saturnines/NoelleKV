#include "lygus_errors.h"

const char* lygus_strerror(lygus_err_t err) {
    switch (err) {
        case LYGUS_OK:                return "OK";

        // Generic errors
        case LYGUS_ERR_INVALID_ARG:   return "Invalid argument";
        case LYGUS_ERR_NOMEM:         return "Out of memory";
        case LYGUS_ERR_NOT_INIT:      return "Not initialized";
        case LYGUS_ERR_ALREADY_INIT:  return "Already initialized";
        case LYGUS_ERR_INTERNAL:      return "Internal error";
        case LYGUS_ERR_BUSY:      return "Operation already in progress";

        // I/O and Storage errors
        case LYGUS_ERR_IO:            return "I/O error";
        case LYGUS_ERR_DISK_FULL:     return "Disk full";
        case LYGUS_ERR_READ:          return "Read failed";
        case LYGUS_ERR_WRITE:         return "Write failed";
        case LYGUS_ERR_FSYNC:         return "Fsync failed";
        case LYGUS_ERR_OPEN_FILE:     return "Failed to open file";
        case LYGUS_ERR_CORRUPT:       return "Data corruption detected";
        case LYGUS_ERR_TRUNCATED:     return "File truncated";
        case LYGUS_ERR_OVERFLOW:      return "Integer overflow";
        case LYGUS_ERR_MALFORMED:     return "Malformed data";
        case LYGUS_ERR_INCOMPLETE:    return "Incomplete data";
        case LYGUS_ERR_BUFFER_TOO_SMALL: return "Buffer too small";

        // WAL-specific errors
        case LYGUS_ERR_WAL_FULL:      return "WAL segment full";
        case LYGUS_ERR_WAL_SEALED:    return "WAL sealed";
        case LYGUS_ERR_WAL_RECOVERY:  return "WAL recovery failed";
        case LYGUS_ERR_BAD_BLOCK:     return "Bad block header";
        case LYGUS_ERR_DECOMPRESS:    return "Decompression failed";
        case LYGUS_ERR_COMPRESS:      return "Compression failed";
        case LYGUS_ERR_OUT_OF_ORDER:  return "Index out of order";

        // Snapshot errors
        case LYGUS_ERR_SNAPSHOT:      return "Snapshot creation failed";
        case LYGUS_ERR_BAD_SNAPSHOT:  return "Invalid snapshot";
        case LYGUS_ERR_LOAD_SNAPSHOT: return "Failed to load snapshot";

        // Raft/Consensus errors
        case LYGUS_ERR_NO_LEADER:     return "No leader elected";
        case LYGUS_ERR_NOT_LEADER:    return "Not the leader";
        case LYGUS_ERR_TIMEOUT:       return "Operation timed out";
        case LYGUS_ERR_RAFT:          return "Raft error";
        case LYGUS_ERR_NO_QUORUM:     return "Cannot reach quorum";
        case LYGUS_ERR_STALE_TERM:    return "Term is stale";
        case LYGUS_ERR_LOG_MISMATCH:  return "Log index/term mismatch";
        case LYGUS_ERR_COMMIT:        return "Commit failed";

        // ALR errors
        case LYGUS_ERR_STALE_READ:    return "Read would be stale";
        case LYGUS_ERR_SYNC_FAILED:   return "NOOP sync failed";
        case LYGUS_ERR_NOT_FRESH:     return "Replica not fresh enough";
        case LYGUS_ERR_BATCH_FULL:    return "Read batch full";
        case LYGUS_ERR_TRY_LEADER: return "no pending sync, try leader";

        // Network/Transport errors
        case LYGUS_ERR_NET:           return "Network error";
        case LYGUS_ERR_CONNECT:       return "Connection failed";
        case LYGUS_ERR_SEND:          return "Send failed";
        case LYGUS_ERR_RECV:          return "Receive failed";
        case LYGUS_ERR_PARSE:         return "Message parse error";
        case LYGUS_ERR_BAD_MESSAGE:   return "Invalid message format";

        // KV Store errors
        case LYGUS_ERR_KEY_NOT_FOUND: return "Key not found";
        case LYGUS_ERR_KEY_TOO_LARGE: return "Key exceeds max size";
        case LYGUS_ERR_VAL_TOO_LARGE: return "Value exceeds max size";
        case LYGUS_ERR_APPLY:         return "Apply to state machine failed";

        default:                      return "Unknown error";
    }
}

int lygus_is_retryable(lygus_err_t err) {
    switch (err) {
        // Transient errors that may resolve on retry
        case LYGUS_ERR_TIMEOUT:
        case LYGUS_ERR_NO_LEADER:
        case LYGUS_ERR_NOT_LEADER:
        case LYGUS_ERR_NO_QUORUM:
        case LYGUS_ERR_STALE_TERM:
        case LYGUS_ERR_NET:
        case LYGUS_ERR_CONNECT:
        case LYGUS_ERR_SEND:
        case LYGUS_ERR_RECV:
        case LYGUS_ERR_STALE_READ:
        case LYGUS_ERR_SYNC_FAILED:
        case LYGUS_ERR_NOT_FRESH:
        case LYGUS_ERR_BATCH_FULL:
        case LYGUS_ERR_DISK_FULL:
        case LYGUS_ERR_BUSY:
        case LYGUS_ERR_TRY_LEADER:
            return 1;

        default:
            return 0;
    }
}
int lygus_is_fatal(lygus_err_t err) {
    switch (err) {
        // Fatal errors that indicate unrecoverable state
        case LYGUS_ERR_CORRUPT:
        case LYGUS_ERR_INTERNAL:
        case LYGUS_ERR_WAL_RECOVERY:
            return 1;

        default:
            return 0;
    }
}