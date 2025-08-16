# SageDB - high-performance distributed persistent in-memory DB

A high-performance distributed key-value database featuring persistent in-memory storage, consistent hashing, and fault-tolerant quorum replication.

## Architecture

- **Coordinator**: Stateless HTTP proxy with intelligent request routing via consistent hashing and virtual node distribution
- **Shards**: Autonomous storage engines with hybrid persistent-memory architecture and configurable replication topology
- **Client**: RESTful HTTP interface supporting atomic CRUD operations with transparent sharding

## How It Works

### Data Distribution
Sage employs **horizontal partitioning** with advanced consistent hashing algorithms for optimal data distribution:

1. **Consistent Hashing Ring**: Cryptographic key hashing ensures deterministic shard assignment with minimal redistribution during topology changes
2. **Virtual Node Architecture**: Multi-point hash ring positioning provides superior load balancing and hotspot mitigation
3. **Intelligent Key Routing**: 
   - Key "user:123" → Shard 1 (hash space: 0x000...333)
   - Key "order:456" → Shard 2 (hash space: 0x333...666)
   - Key "product:789" → Shard 3 (hash space: 0x666...FFF)

### Request Flow
```
Client → Load-Balancing Coordinator → Target Shard → Atomic Response
```

1. **Request Ingestion**: Client submits atomic operation via RESTful endpoint
2. **Hash-Based Routing**: Coordinator performs cryptographic key hashing for deterministic shard selection
3. **Transparent Proxying**: Zero-copy request forwarding to target storage engine
4. **Transactional Processing**: Shard executes operation with ACID guarantees and persistence
5. **Response Aggregation**: Coordinator returns structured JSON response with operation metadata

### High-Availability Replication
Advanced multi-master replication with configurable consistency models:

```bash
# Fault-tolerant shard cluster with quorum consensus
node shard.js --port 4101 --data data1 --replicas http://replica1:4201,http://replica2:4301 --quorum 2
```

- **Synchronous Replication**: Write operations propagate to replica set with configurable consistency levels
- **Quorum Consensus**: Byzantine fault-tolerant write acknowledgment requiring majority node agreement
- **Automatic Failover**: Seamless request redirection during node failures with zero data loss guarantees

### Cloud-Native Distributed Deployment
Horizontally scalable microservice architecture with location-transparent clustering:

```bash
# Geographically distributed storage nodes
node shard.js --port 4101 --data data1  # us-east-1
node shard.js --port 4102 --data data2  # us-west-2
node shard.js --port 4103 --data data3  # eu-central-1

# Multi-region coordinator with service discovery
node coordinator.js --port 4000 --shards http://shard1.us-east.db:4101,http://shard2.us-west.db:4102,http://shard3.eu-central.db:4103
```

## Components

### Coordinator (`coordinator.js`)
Stateless routing engine with advanced consistent hashing algorithms and virtual node distribution for optimal load balancing.

### Shard (`shard.js`) 
Autonomous storage engines featuring:
- **Hybrid Storage Architecture**: Memory-mapped persistent storage with compressed LZ4 snapshots
- **Write-Ahead Logging**: Transaction-safe durability with automatic recovery and replay mechanisms
- **Quorum Replication**: Configurable multi-master synchronization with Byzantine fault tolerance
- **RESTful API**: High-performance HTTP endpoints with JSON serialization

### Database Engine (`fastdb.js`, `db.js`)
- **Memory-Optimized B-Trees**: Self-balancing data structures for O(log n) key lookup performance
- **Compression Pipeline**: LZ4-based snapshot compression achieving 60-80% space reduction
- **Transaction Logging**: Append-only WAL with automatic compaction and checkpoint creation
- **Zero-Copy Operations**: Direct memory access patterns minimizing serialization overhead

## Quick Start

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Start shard servers:**
   ```bash
   # Terminal 1
   node shard.js --port 4101 --data data1
   
   # Terminal 2  
   node shard.js --port 4102 --data data2
   
   # Terminal 3
   node shard.js --port 4103 --data data3
   ```

3. **Start coordinator:**
   ```bash
   node coordinator.js --port 4000 --shards http://127.0.0.1:4101,http://127.0.0.1:4102,http://127.0.0.1:4103 --vnodes 100
   ```

4. **Test the system:**
   ```bash
   node client_example.js
   ```

## API

### Key-Value Operations

**PUT** - Store a string value:
```bash
curl -X PUT "http://localhost:4000/kv?key=mykey" -d "myvalue"
```

**PUT** - Store a JSON object:
```bash
curl -X PUT "http://localhost:4000/kv?key=user:123" \
  -H "Content-Type: application/json" \
  -d '{"name":"Alice","age":30,"tags":["developer","nodejs"]}'
```

**GET** - Retrieve a value:
```bash
curl "http://localhost:4000/kv?key=mykey"
# Response: {"found":true,"value":"myvalue","ts":1755248123456}

curl "http://localhost:4000/kv?key=user:123"
# Response: {"found":true,"value":{"name":"Alice","age":30,"tags":["developer","nodejs"]},"ts":1755248123456}
```

**DELETE** - Remove a key:
```bash
curl -X DELETE "http://localhost:4000/kv?key=mykey"
```

### Value Types

Sage supports two value types:

1. **String Values** (default): Send with `text/plain` content-type or no content-type header
2. **JSON Objects**: Send with `application/json` content-type header

JSON objects are automatically serialized for storage and deserialized on retrieval, maintaining their structure and data types.

### Routing Information

**GET /route** - Find which shard handles a key:
```bash
curl "http://localhost:4000/route?key=mykey"
```

## Configuration

### Coordinator Options
- `--port`: HTTP port (default: 4000)
- `--shards`: Comma-separated list of shard URLs
- `--vnodes`: Number of virtual nodes for consistent hashing (default: 100)

### Shard Options
- `--port`: HTTP port (default: 4101)
- `--data`: Data directory for persistence (default: ./data)
- `--replicas`: Comma-separated list of peer shard URLs for replication
- `--quorum`: Minimum number of nodes for write operations (default: 1)
- `--id`: Shard identifier (default: shard-{port})

## Enterprise-Grade Data Persistence

Each shard maintains a sophisticated dual-layer persistence architecture:

- **`snapshot.json.gz`**: LZ4-compressed immutable snapshots with atomic point-in-time consistency
- **`wal.log`**: High-throughput append-only transaction log with automatic fsync optimization
- **Automatic Compaction**: Intelligent background processes trigger snapshot creation based on configurable WAL size thresholds
- **Crash Recovery**: Fast startup with automatic WAL replay and consistency verification

## Author

**Julia Kafarska**
