# lygus

Distributed KV store with Raft consensus and with linearizable reads.

Features:

Almost Local Reads

Raft Follower Reads with ReadIndex 

Linearizable reads and writes


## Quick Start
```bash
docker compose up --build
```

This starts a 5-node cluster:
- Node 0: localhost:8080
- Node 1: localhost:8081  
- Node 2: localhost:8082
- Node 3: localhost:8083
- Node 4: localhost:8084

## Usage
```bash
# Check status (shows role, leader id, term) This is an example assuming the leader is under Node 0.
echo "STATUS" | nc localhost 8080

# Write to leader
echo "PUT $(echo -n 'hello' | base64) $(echo -n 'world' | base64)" | nc localhost 8080

# Read 
echo "GET $(echo -n 'hello' | base64)" | nc localhost 8080
```

## Testing Failover
```bash
# Find the leader
echo "STATUS" | nc localhost 8080  # LEADER or FOLLOWER

# Kill a node
docker stop lygus-node0

# Watch new leader election
docker logs -f lygus-node1

# Bring it back
docker start lygus-node0
```


Benchmark


Move from base64
Cleanup

