/**
* raft_errors.h - Error codes for lil-raft
 */

#ifndef RAFT_ERRORS_H
#define RAFT_ERRORS_H

// Success
#define RAFT_OK  0

// Generic errors
#define RAFT_ERR_INVALID_ARG   -1   // NULL pointer, bad parameter
#define RAFT_ERR_NOMEM         -2   // Memory allocation failed
#define RAFT_ERR_SHUTDOWN      -3   // Raft is shutting down

// State errors
#define RAFT_ERR_NOT_LEADER    -10  // Only leader can process this
#define RAFT_ERR_STALE_TERM    -11  // Term too old
#define RAFT_ERR_ONE_VOTING_CHANGE_ONLY  -12  // Config change in progress

// Log errors
#define RAFT_ERR_LOG_MISMATCH  -20  // Prev log entry doesn't match
#define RAFT_ERR_SNAPSHOT_ALREADY_LOADED  -21

// Snapshot errors
#define RAFT_ERR_SNAPSHOT_FAILED       -30  // Failed to create snapshot
#define RAFT_ERR_SNAPSHOT_NOT_FOUND    -31  // No snapshot exists
#define RAFT_ERR_SNAPSHOT_CORRUPT      -32  // Snapshot data is corrupt
#define RAFT_ERR_SNAPSHOT_IN_PROGRESS  -33  // Snapshot already in progress


#endif // RAFT_ERRORS_H