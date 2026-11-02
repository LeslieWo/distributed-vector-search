# Benchmarks

All measurements were taken on one laptop: Apple M4 Pro (10 performance + 4 efficiency cores), 48 GB RAM,
macOS, Java 21. Kafka and Redis run in a colima VM (4 vCPU, 6 GiB) on the same machine. k6, the coordinator, the
three shard nodes and (during loading) the embed-workers are host processes. **Every number here includes that
co-location.** Load generation, embedding, HNSW search and the brokers all compete for the same cores and memory.

Rules followed for every timing run:
- AC power with low power mode off (checked by `scripts/k6_run.sh` and `scripts/chaos_run.sh`, which refuse to run
  otherwise).
- The 1-minute load average had to fall below 20 before a run started. It never reached the usual "< 5": background
  iCloud processes kept it around 5-15 even with nothing else running.
- A 30 s warm-up at 200-300 QPS before each measured run (not counted).
- k6 constant-arrival-rate (open model), one scenario per step, 60 s per step unless stated. Failed = any non-OK gRPC
  status, timeouts included. Partial answers are counted separately. Iterations k6 could not start are reported as
  "dropped".
- Queries are drawn uniformly at random (with replacement) from the 101,093 MS MARCO dev queries, k = 10, client
  deadline 1 s.

## Corpus and index

- First 2,000,000 MS MARCO passages, split into 128-word windows with 32-word overlap: 2,015,656 chunks.
- all-MiniLM-L6-v2, 384 dimensions, L2-normalized, inner product. Output matches sentence-transformers (PyTorch) to
  about 2e-7 per dimension.
- HNSW (hnswlib-core): M = 16, efConstruction = 200, efSearch = 80 at query time.
- 3 nodes x 2 replicas: partitions `node-1.node-2` = 682,954, `node-1.node-3` = 582,925 and `node-2.node-3` = 749,777
  vectors, each stored on both of its nodes.

## Loading

| stage | result |
|---|---|
| `scripts/bulk_ingest.py` to ingest to Kafka (1,000 docs/request, 4 client threads) | 2,000,000 docs / 2,015,656 chunks in 7.9 s; offsets over the 12 partitions sum to exactly 2,015,656 |
| embed + index, end to end, by worker count N and ONNX Runtime threads T | 1x(all cores) 478/s, 1x4 354/s, 2x4 557/s, 4x3 692/s, 4x2 659/s, 6x2 720/s, 6x1 700/s, **8x1 775/s**, 12x1 806/s |
| sustained with 8x1 over 10 min | 739 chunks/s, including snapshot pauses |

Throughput levels off past four workers. During the 8-worker run the CPU was 96-100% busy, workers were inside ONNX
Runtime and shard gRPC threads were idle, so embedding plus two HNSW inserts per chunk saturate 14 cores. 8x1 is
the knee (12 workers add 4%). At 739/s the full corpus takes about 45 minutes.

After loading: indexed chunks = ingested chunks = committed Kafka offsets = 2,015,656, both holders of each
partition held identical counts, and the duplicate and replace counters were 0.

Snapshots: 1,262 bytes/vector with vectors stored as fp16 on disk (2,007 with plain Java serialization of fp32).
4.8 GB for all three nodes. Restart from snapshot takes 1.7-1.9 s per node (1.33-1.43M vectors). WAL replay
measured ~6.7 MB/s, so the 256 MB early-snapshot threshold bounds crash replay at roughly 40 s.

## Recall

recall@10 of the full query path (coordinator embedding, 3 partitions, heap merge) against exact brute force over
every vector stored in the cluster (read back via `Scan`), 500 dev queries, cache off:

| efSearch | 1,016,514 chunks | 2,015,656 chunks |
|---|---|---|
| 40 | 0.9436 | 0.9400 |
| 80 (default) | 0.9702 | 0.9726 |
| 160 | 0.9878 | 0.9854 |
| 320 | 0.9938 | 0.9944 |

On a 100k sample, the fp16 snapshot format changed recall@10 from 0.9670 to 0.9669 after reload.

## Query throughput and latency

### Baseline (as first built)

ONNX Runtime used its default thread pool (one thread per core) in the coordinator.

Cache off:

| target QPS | achieved | failed | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| 400 | 400 | 0 | 2.80 | 6.99 | 14.87 |
| 600 | 600 | 0 | 4.34 | 11.30 | 20.43 |
| 800 | 800 | 0 | 8.18 | 54.21 | 143.74 |
| 1000 | 999 | 1 | 24.91 | 159.3 | 309.1 |
| 1200 | 1112 | 1262 | 758.5 | 966.8 | 997.5 |

Cache on (cache emptied before the run; overall hit rate 43.6%, rising with QPS):

| target QPS | achieved | failed | p50 ms | p99 ms |
|---|---|---|---|---|
| 800 | 800 | 0 | 6.49 | 26.39 |
| 1000 | 1000 | 0 | 7.06 | 35.59 |
| 1200 | 1198 | 0 (99 dropped) | 8.68 | 188.58 |
| 1400 | 1400 | 0 | 11.01 | 62.94 |
| 1600 | 1592 | 1 | 17.16 | 359.2 |

### Where the CPU goes

Measured at 200 QPS from server-side counters: one partition search takes 1.29 ms and one query embedding 1.39 ms.
A query costs about 3 x 1.29 + 1.39 = 5.3 ms of CPU: 74% HNSW, 26% embedding.

### Tuning (one variable at a time, back to back, cache off)

| change | 800 QPS p99 | 1000 QPS p99 | 1200 QPS | 1400 QPS | 1600 QPS |
|---|---|---|---|---|---|
| ONNX Runtime default threads | 16.45 | 127.8 | saturated (154 failed) | saturated | - |
| **ONNX Runtime 1 thread per query** (adopted) | 4.34 | 5.22 | p99 7.16 | p99 33.56 | **p99 62.57, 0 failed** |
| + efSearch 40 (not adopted, recall 0.940) | 3.99 | 3.80 | 4.34 | 5.67 | 7.97 (1800 QPS: 12.88) |
| shards on generational ZGC (not adopted) | 5.0 / 473 | 7.1 / 170 | 126 / 210 | 330 / 163 | - |

- With the default ONNX Runtime pool, every concurrent query tried to split a small matrix multiply across 14
  threads. One thread per query moves the parallelism to across queries and raises the cache-off ceiling from
  600-800 QPS to 1,600 QPS.
- The two ZGC runs disagreed by up to 90x. No stable benefit over G1, and ZGC grows the heap toward `-Xmx` on a
  machine that was already memory-pressured.
- The biggest source of run-to-run noise was macOS memory compression, not GC. With the compressor at 16 GB (other
  applications open), the same configuration varied 3-10x in p99. The back-to-back runs above were taken while it sat
  at 8-9 GB. GC pauses during measured windows were at most 15 ms.

### Cache on, final configuration

Cache emptied first, then 60 s steps: 1200 / 1600 / 2000 / 2400 / 2800 / 3200 QPS, all with 0 failed. p99 was
8.87 / 8.38 / 11.24 / 14.73 / 17.63 / 22.27 ms. Overall hit rate 54.6%. With uniform sampling the steady-state hit
rate at R QPS is about 1 - exp(-60R / 101093), which is ~85% at 3,200 QPS, so that step is mostly served by Redis.
Starting at 3,200 QPS from an empty cache, the run saturates: 2,706 QPS achieved, p99 616 ms, 50% hits. The
cache-off number (1,600 QPS) is the honest measure of search capacity.

## Hedged requests

Hedge after max(3 ms, rolling p95 of that partition), first answer wins, budget 10% of requests.

**Healthy cluster, 1,200 QPS, 120 s, cache off.** Original design, which cancels the slower attempt:

| run | hedging | p99 ms | extra partition requests |
|---|---|---|---|
| 1 | on | 7.58 | 0.51% |
| 1 | off | 6.32 | 0 |
| 2 | on | 6.99 | 0.45% |
| 2 | off | 6.54 | 0 |

No benefit. All replicas share one CPU, so when one is slow the other usually is too, and a hedge cannot help with
correlated slowness.

**One replica stalling.** shard-2 receives `SIGSTOP` for 200 ms every 3 s (a stand-in for long GC pauses or a noisy
neighbour), at 1,000 QPS for 120 s. Requests slower than 100 ms, on AC power:

| configuration | > 100 ms | p99.9 ms |
|---|---|---|
| hedging off | 426 | 173.5 |
| hedging on, cancel the loser (original) | 6 | 8.3 |
| hedging on, keep the loser running (current default) | 0 | 6.7 |

Earlier and later rounds, partly taken on battery, varied: "cancel" ranged from 6 to 464 slow requests, "off" from
44 to 426, and "keep" was 0 every time. Cause: the power-of-two-choices balancer avoids a slow replica through its
in-flight count and latency samples. Cancelling the loser removed it from the in-flight count right away and
recorded only the time until the hedge, which hid the stalled replica. Keeping the loser running fixed that
(`HedgingIT.keepingTheLoserRecordsItsRealLatency`: the estimate for a 100 ms replica is 3.8 ms with cancel and
110.4 ms without).

## Node loss

`scripts/chaos_run.sh kill-test <node> 1200`: 180 s at 1,200 QPS, `kill -9` of one shard node at 60 s, restart at 120 s,
once per node.

- Result: 0 failed queries in all three runs. Two runs had 0 partial results; one had 8 partial results right after
  the killed node rejoined.
- Cause of the partial results: a per-attempt timeout fired while the query still had time left, and the attempt was
  not retried on the other replica. Fixed in `ScatterGather` (retry on the other replica while at least
  `MIN_RETRY_MILLIS` of the query deadline remains), reproduced by `DeadlineIT`.
- Status: these three runs were taken on battery power, so the numbers are provisional. Re-runs on AC power with the
  fix are pending and will replace this section.
