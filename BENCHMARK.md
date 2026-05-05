# Benchmark Results
## CprE 3080 — Project 2 | Lukas Hessling

---

## Setup

- **Server:** `./kvserver 9000 8 1024 500` (8 workers, 1024 buckets, 500ms sweeper)
- **Workload:** 90% GET / 10% PUT, 10,000 ops per client
- **Keys:** drawn from a pool of 1000 keys so GETs actually hit
- **Machine:** Coover 2061 lab workstation, Linux
- **Command:** `./bench_client localhost 9000 <clients> 10000 90`

---

## Results

| Concurrent Clients | Total Operations | Throughput (ops/sec) |
|--------------------|------------------|----------------------|
| 1                  | 10,000           | 42,659               |
| 4                  | 40,000           | 174,211              |
| 16                 | 160,000          | 249,854              |
| 64                 | 640,000          | 235,489              |

---

## Analysis

Throughput scaled strongly from 1 to 16 concurrent clients, growing from ~42,600 ops/sec to ~249,900 ops/sec — roughly a 5.9x improvement. This near-linear scaling reflects the thread pool effectively parallelizing connection handling, with the 90% read workload allowing multiple GETs to proceed concurrently under the shared read lock. At 64 clients throughput dropped to ~235,500 ops/sec, indicating the server has reached saturation. The plateau and slight decline at 64 clients is explained by two factors: first, the 8-worker thread pool becomes the bottleneck as 64 clients compete for only 8 workers, causing connections to queue in the bounded work queue; second, the single table-wide write lock creates brief contention as the 10% PUT operations block all concurrent reads. The lab machine numbers are significantly higher than WSL2 results (~2-3x), which is expected given bare Linux has lower syscall overhead and better scheduler performance than a virtualized environment.