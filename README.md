# Mini-KV Server
## CprE 3080 — Project 2 | Lukas Hessling

---

## Build and Run

```bash
make
./kvserver <port> <num_workers> <num_buckets> [sweeper_interval_ms]
```

Example:
```bash
./kvserver 9000 8 1024 500
```

To run the benchmark client:
```bash
make bench
./bench_client <host> <port> <num_clients> <ops_per_client> <read_pct>
```

Example:
```bash
./bench_client localhost 9000 16 10000 90
```

To check for memory leaks:
```bash
valgrind --leak-check=full --show-leak-kinds=all ./kvserver 9000 8 1024
```

---

## Stage Status

- **Stage 1** — Complete. Sequential TCP server with full protocol support: GET, PUT, DEL, STATS, QUIT. Hash table with chaining. Graceful error handling for malformed commands.
- **Stage 2** — Complete. Fixed-size thread pool with bounded FIFO work queue. Producer-consumer synchronization via one mutex and two condition variables. Clean shutdown via poison pill pattern.
- **Stage 3** — Complete. Hash table protected by a single `pthread_rwlock_t`. GET acquires read lock, PUT and DEL acquire write lock. Stats counters use `stdatomic.h` for lock-free updates.
- **Stage 4** — Complete. TTL support on PUT with absolute expiration timestamps. Background sweeper thread scans and removes expired entries every `sweeper_interval_ms` milliseconds. Full STATS response implemented.
- **Bonus** — Complete. Append-only write-ahead log. Every successful PUT and DEL is written to `kv.log` before the response is sent. On startup the log is replayed to reconstruct state; TTL'd entries that have already elapsed are dropped during replay.

---

## Design Decisions

### 1. Lock Granularity

The hash table is protected by a single table-wide `pthread_rwlock_t`. This means all concurrent GETs share one read lock and any PUT or DEL acquires an exclusive write lock across the entire table. The tradeoff versus bucket-level locking is straightforward: a single lock is simpler to reason about, eliminates any risk of deadlock from acquiring multiple locks in the wrong order, and has lower overhead per operation since there is only one lock to acquire and release. The downside is contention — a write on any key blocks reads on all keys, not just the affected bucket. In practice, the benchmark results showed throughput scaling from ~26,000 ops/sec at 1 client to ~120,000 ops/sec at 16 clients, which suggests the single lock is not a serious bottleneck at this scale with a 90% read workload. At 64 clients throughput dropped slightly to ~114,000 ops/sec, indicating the write lock is beginning to create contention. Bucket-level locking would reduce this contention but would add significant implementation complexity and would only matter at much higher write ratios or client counts.

### 2. Worker Pool Sizing

The server was benchmarked with 8 workers fixed across all client counts. At 1 client throughput was ~26,000 ops/sec, limited by the round-trip latency of a single connection. At 4 clients throughput jumped to ~81,600 ops/sec as multiple connections were served in parallel. At 16 clients throughput reached its peak at ~120,000 ops/sec. At 64 clients throughput dropped slightly to ~114,000 ops/sec, indicating the thread pool and lock contention are becoming the bottleneck rather than the number of workers. Adding more workers beyond 8 would not help here because the bottleneck has shifted to the shared rwlock and the OS scheduler overhead of managing many threads. The optimal worker count for this workload on this machine is approximately equal to the number of logical CPU cores, beyond which additional threads add context-switch overhead without improving throughput.

### 3. Sweeper Coordination

The sweeper thread wakes every `sweeper_interval_ms` milliseconds using `nanosleep`, acquires the table-wide write lock, scans all buckets removing expired entries, then releases the lock. Holding the write lock for the entire sweep means no reads or writes can proceed during the scan, which could freeze the server briefly on a very large table. For the scale of this project this is acceptable — with 1024 buckets the scan completes in microseconds. The alternative would be to acquire and release the write lock per bucket, allowing interleaving with worker threads, but this adds complexity and is unnecessary at this scale. Races between a GET and the sweeper are impossible by construction: the sweeper holds the write lock exclusively, so any GET that arrives during a sweep blocks on `tryrdlock` until the sweep completes. There is no window where a GET can observe a key mid-deletion.