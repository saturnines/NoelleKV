#ifndef LYGUS_ERRORS_H
#define LYGUS_ERRORS_H

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Lygus Error Codes
 *
 * Convention:
 * - 0 = success (LYGUS_OK)
 * - Negative values = errors
 * - If this is ever used in the future group it by subsystem
 *
 * Usage with ssize_t pattern:
 *   ssize_t n = some_function(...);
 *   if (n < 0) {
 *     lygus_err_t err = (lygus_err_t)n;
 *     fprintf(stderr, "Error: %s\n", lygus_strerror(err));
 *   }
 */

typedef enum {
  LYGUS_OK = 0,

  // ============================================================================
  // Generic errors (1-9)
  // ============================================================================
  LYGUS_ERR_INVALID_ARG   = -1,   // Invalid argument (NULL pointer, bad range)
  LYGUS_ERR_NOMEM         = -2,   // Out of memory
  LYGUS_ERR_NOT_INIT      = -3,   // Lygus not initialized (lygus_open not called)
  LYGUS_ERR_ALREADY_INIT  = -4,   // Already initialized
  LYGUS_ERR_INTERNAL      = -5,   // Internal error (should not happen)
  LYGUS_ERR_BUSY          = -6,   // Operation in progress (for snapshotting)

  // ============================================================================
  // I/O and Storage errors (10-29)
  // ============================================================================
  LYGUS_ERR_IO            = -10,  // Generic I/O error
  LYGUS_ERR_DISK_FULL     = -11,  // Disk full (ENOSPC)
  LYGUS_ERR_READ          = -12,  // Read failed
  LYGUS_ERR_WRITE         = -13,  // Write failed
  LYGUS_ERR_FSYNC         = -14,  // fsync/fdatasync failed
  LYGUS_ERR_OPEN_FILE     = -15,  // Failed to open file
  LYGUS_ERR_CORRUPT       = -16,  // Data corruption detected (bad CRC, invalid format)
  LYGUS_ERR_TRUNCATED     = -17,  // File/block truncated
  LYGUS_ERR_OVERFLOW      = -18,  // Integer overflow (varint, index)
  LYGUS_ERR_MALFORMED     = -19,  // Malformed data (invalid varint, bad block header)
  LYGUS_ERR_INCOMPLETE    = -20,  // Incomplete data (buffer too short)
  LYGUS_ERR_BUFFER_TOO_SMALL = -21,

  // ============================================================================
  // WAL-specific errors (30-39)
  // ============================================================================
  LYGUS_ERR_WAL_FULL      = -30,  // WAL segment full
  LYGUS_ERR_WAL_SEALED    = -31,  // WAL sealed, cannot write
  LYGUS_ERR_WAL_RECOVERY  = -32,  // WAL recovery failed
  LYGUS_ERR_BAD_BLOCK     = -33,  // Bad block header (magic, CRC)
  LYGUS_ERR_DECOMPRESS    = -34,  // Decompression failed
  LYGUS_ERR_COMPRESS      = -35,  // Compression failed
  LYGUS_ERR_OUT_OF_ORDER  = -36,

  // ============================================================================
  // Snapshot errors (40-49)
  // ============================================================================
  LYGUS_ERR_SNAPSHOT      = -40,  // Snapshot creation failed
  LYGUS_ERR_BAD_SNAPSHOT  = -41,  // Invalid snapshot (corrupt, wrong version)
  LYGUS_ERR_LOAD_SNAPSHOT = -42,  // Failed to load snapshot

  // ============================================================================
  // Raft/Consensus errors (50-69)
  // ============================================================================
  LYGUS_ERR_NO_LEADER     = -50,  // No leader elected
  LYGUS_ERR_NOT_LEADER    = -51,  // This node is not the leader
  LYGUS_ERR_TIMEOUT       = -52,  // Operation timed out
  LYGUS_ERR_RAFT          = -53,  // Generic Raft error
  LYGUS_ERR_NO_QUORUM     = -54,  // Cannot reach quorum
  LYGUS_ERR_STALE_TERM    = -55,  // Term is stale
  LYGUS_ERR_LOG_MISMATCH  = -56,  // Log index/term mismatch
  LYGUS_ERR_COMMIT        = -57,  // Commit failed

  // ============================================================================
  // ALR (Almost-Local Reads) errors (70-79)
  // ============================================================================
  LYGUS_ERR_STALE_READ    = -70,  // Read would be stale
  LYGUS_ERR_SYNC_FAILED   = -71,  // NOOP sync failed
  LYGUS_ERR_NOT_FRESH     = -72,  // Replica not fresh enough
  LYGUS_ERR_BATCH_FULL    = -73,  // Read batch full
  LYGUS_ERR_TRY_LEADER    = -74,  // Follower has nothing to piggyback, try leader

  // ============================================================================
  // Network/Transport errors (80-89)
  // ============================================================================
  LYGUS_ERR_NET           = -80,  // Generic network error
  LYGUS_ERR_CONNECT       = -81,  // Connection failed
  LYGUS_ERR_SEND          = -82,  // Send failed
  LYGUS_ERR_RECV          = -83,  // Receive failed
  LYGUS_ERR_PARSE         = -84,  // Message parse error
  LYGUS_ERR_BAD_MESSAGE   = -85,  // Invalid message format

  // ============================================================================
  // KV Store errors (90-99)
  // ============================================================================
  LYGUS_ERR_KEY_NOT_FOUND = -90,  // Key not found
  LYGUS_ERR_KEY_TOO_LARGE = -91,  // Key exceeds max size
  LYGUS_ERR_VAL_TOO_LARGE = -92,  // Value exceeds max size
  LYGUS_ERR_APPLY         = -93,  // Apply to state machine failed

} lygus_err_t;

/**
 * Get human-readable error message
 *
 * @param err  Error code
 * @return     Static string describing the error
 */
const char* lygus_strerror(lygus_err_t err);

/**
 * Check if an error is retryable (transient)
 *
 * Retryable errors: TIMEOUT, NO_LEADER, NO_QUORUM, etc.
 * Non-retryable: CORRUPT, INVALID_ARG, MALFORMED, etc.
 *
 * @param err  Error code
 * @return     1 if retryable, 0 if permanent
 */
int lygus_is_retryable(lygus_err_t err);

/**
 * Check if an error indicates a fatal condition (should shut down)
 *
 * Fatal errors: CORRUPT, INTERNAL, WAL_RECOVERY
 *
 * @param err  Error code
 * @return     1 if fatal, 0 otherwise
 */
int lygus_is_fatal(lygus_err_t err);

#ifdef __cplusplus
}
#endif

#endif // LYGUS_ERRORS_H