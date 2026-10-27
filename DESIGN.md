# Design decisions (source of truth for all build steps)

## Modules
| module | kind | role |
|---|---|---|
| `proto` | library | `.proto` files + generated gRPC stubs (package `vectorsearch.v1`) |
| `common` | library | shared types, consistent-hash ring, Redis membership client |
| `embedding` | library | ONNX all-MiniLM-L6-v2 (384-d, L2-normalized) + tokenizer; used by embed-worker AND coordinator |
| `shard-node` | Spring Boot | holds HNSW indexes, gRPC `ShardService` |
| `coordinator` | Spring Boot | gRPC `SearchService`: embed query, scatter-gather, hedging |
| `ingest` | Spring Boot | REST `POST /documents` -> chunk -> Kafka `raw-chunks` |
| `embed-worker` | Spring Boot | consume `raw-chunks`, batch-embed, route Upserts to replicas |

## Ports
| service | gRPC | HTTP (actuator) |
|---|---|---|
| shard-node-1/2/3 | 9101/9102/9103 | 8081/8082/8083 |
| coordinator | 9100 | 8090 |
| ingest | - | 8070 |
| embed-worker | - | 8075 |
Kafka `localhost:9092`, Redis `localhost:6379`, Prometheus 9090, Grafana 3000.

## Data placement: "3 nodes x 2 replicas"
- Consistent-hash ring over physical node ids, 128 virtual nodes each, hash = Murmur3 64-bit of the key.
- `chunkId` -> clockwise first vnode = **primary** node P; next vnode clockwise owned by a *different* physical node = **replica** R. Replication factor 2.
- A shard-node keeps **one HNSW index per partition it stores**, where partition = the replica pair {P, R} (changed in step 22: with 128 vnodes, keying partitions by primary id scatters that primary's replicas over several nodes, leaving no single "other replica" for hedging/failover). With 3 nodes there are 3 pairs, each node stores 2 of them. Partition count grows as N(N-1)/2.
- Query covers every vector exactly once: for each partition {P, R}, ask exactly one holder, then merge top-k.
- Node loss: each partition still has its other holder. Hedged request = after the hedge delay, send the same partition query to the other holder and take whichever answers first.
- Write ack = ALL (both holders) before the Kafka offset is committed. While a holder is down, indexing stalls (Kafka lag grows) but queries and ingest keep working. Hinted handoff deferred.
- Snapshots: every 5 min, or early once the WAL passes 64 MB (re-measure at 2M scale).

## Kafka
- Topic `raw-chunks`: 12 partitions, key = chunkId (same chunk always lands in the same partition -> ordering per chunk).
- Topic `raw-chunks.dlq` for poison messages after retries.
- Locally replication factor 1 (single broker); on AWS 3.
- Delivery: at-least-once; Upsert is idempotent by chunkId.

## Redis
- Membership: `vs:nodes:<nodeId>` -> `host:grpcPort`, TTL 3 s, refreshed every 1 s by each shard-node (heartbeat).
- Query cache: key = hash(query text + k), TTL 60 s.

## Conventions
- Java 21, Spring Boot 3.5.x, Gradle wrapper 8.14.3. JUnit 5, Testcontainers for integration tests.
- Commit per build step: message `Step N: <summary>`, add files by explicit path (never `git add -A` / `.`), end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Never commit data, models, snapshots (`data/`, `*.onnx`, `snapshots/`).
- Disk is tight (~26 GB free): keep total data+model+snapshots under 12 GB, delete intermediates.
- Each step appends a section to `BUILDLOG.md`: what was built, key decision + why, rejected alternative, how verified (exact command + result). This is the study material for the quiz later, so write it in plain Chinese.
