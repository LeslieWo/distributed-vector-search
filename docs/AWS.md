# AWS test session: setup and cost estimate

Status: **step 46 written; step 47 pending owner approval (decision on 2026-10-27: no-go for now, the owner
decides later).** No AWS resource has been created and no money has been spent. `infra/aws/main.tf` is a plan
only and has not been run through `terraform validate` (Terraform is not installed on the laptop). Running
`terraform apply` starts billing.

## Why bother

Every number in the README was measured on one laptop. k6, the coordinator, three shard-nodes, the embed-workers,
Kafka (in a 4-CPU colima VM) and Redis all compete for the same 14 cores. The laptop numbers are a lower bound on
what the design can do. They also mix load-generator cost into server cost. Separate machines fix both problems.

## Cheapest configuration that reproduces the test

All machines go in one AZ (us-east-1a) and one cluster placement group. Graviton (arm64) instances are used,
because the code already runs on arm64 (the laptop is Apple silicon, and the onnxruntime / DJL tokenizer jars
ship linux-aarch64 natives).

| role | count | type | vCPU / RAM | $/h each | why this size |
|---|---|---|---|---|---|
| shard-node | 3 | m7g.2xlarge | 8 / 32 GiB | 0.3264 | 1.33-1.43M vectors per node at 2M chunks: ~3.5 GB live heap (measured ~2.5 KB/vector), -Xmx8g. HNSW search is CPU-bound. |
| coordinator | 1 | c7g.4xlarge | 16 / 32 GiB | 0.580 | query embedding (MiniLM on CPU) runs once per query; the CPU cost per query is measured in step 40, so revisit this size then. |
| load generator (k6) | 1 | c7g.2xlarge | 8 / 16 GiB | 0.290 | keeps k6 off the servers it measures. |
| Kafka + Redis + Prometheus/Grafana | 1 | m7g.large | 2 / 8 GiB | 0.0816 | one KRaft broker plus Redis, the same as docker-compose. Idle during query tests. |
| embed-worker | 2 | c7g.4xlarge | 16 / 32 GiB | 0.580 | only during the 2M load (~1 h), then destroyed. |
| EBS gp3 | 230 GB total | | | ~0.025 total | $0.08/GB-month / 730 h. |
| public IPv4 | 8 | | | 0.005 | AWS charges for every in-use public IPv4. |

Prices: Linux on-demand, us-east-1, read on 2026-10-27 from instances.vantage.sh (EC2, ElastiCache) and
aws.amazon.com/msk/pricing, aws.amazon.com/vpc/pricing. The vantage pages do not print the region next to the
price. Their default is us-east-1, so re-check in the AWS Pricing Calculator before approving.

### Hourly and session cost

- Steady part (3 shards + coordinator + k6 + infra + EBS + IPs):
  3 x 0.3264 + 0.580 + 0.290 + 0.0816 + 0.025 + 8 x 0.005 = **$1.996/h**
- Embed-workers during the load: 2 x 0.580 = $1.16/h, for ~1 h.

| session | cost |
|---|---|
| 3 h session + 1 h of workers | 3 x 1.996 + 1.16 = **$7.15** |
| 4 h session + 1 h of workers | 4 x 1.996 + 1.16 = **$9.14** |
| forgot to destroy for 24 h (no workers) | 24 x 1.996 = $47.90 |

Data transfer inside one AZ over private IPs is free. Traffic in from the internet (image pulls, the 1 GB
MS MARCO download) is free. Egress is only SSH and Grafana, a few MB.

### Rejected: MSK + ElastiCache

| | self-hosted on m7g.large | managed |
|---|---|---|
| Kafka | included | MSK: 3 x kafka.m7g.large x $0.204 = $0.612/h, plus storage $0.10/GB-month |
| Redis | included | ElastiCache cache.t4g.small $0.032/h |
| total | $0.0816/h | ~$0.65/h |

For a 3-4 hour benchmark, managed services cost ~8x more for this part. They also add nothing to what is
measured: Kafka only matters during the load, and Redis holds membership plus the cache. MSK is the right
answer for a production deployment (replication factor 3, broker failover), not for this test.

### Rejected: one big box

A single c7g.16xlarge (64 vCPU, ~$2.32/h) would cost about the same, but it repeats the laptop's main flaw:
everything shares one machine, so node loss and network hops are not real.

## How the session would go (step 47, after approval)

1. `cd infra/aws && terraform apply -var key_name=... -var my_ip=$(curl -s ifconfig.me)/32`
2. Copy the jars (or push the Jib images, step 45, to ECR or Docker Hub). Copy `data/models` and
   `data/msmarco` (or run `scripts/fetch-data.sh` on the infra box).
3. Infra box: `docker compose up -d kafka redis prometheus grafana`, with Prometheus targets pointed at the
   private IPs.
4. Shards: `java -Xmx8g -jar shard-node.jar --shard.node-id=node-N --shard.advertise-host=<private ip> --shard.redis-uri=redis://<infra>:6379`
5. `clusterAdmin init node-1,node-2,node-3`, ingest with `scripts/bulk_ingest.py`, and run embed-workers on the
   two worker boxes (12 Kafka partitions = up to 12 worker processes). Destroy the workers after lag = 0:
   `terraform apply -var worker_count=0`.
6. Run k6 from the load generator box with the same `loadtest/search.js` steps as on the laptop: cache on, then
   cache off, then hedging off, then a kill -9 of one shard during a sustained run.
7. `terraform destroy` right after. Expected total: $7-10.

## What changes on AWS vs the laptop

- Kafka replication factor stays 1 with one broker. The ingest test is about embedding throughput, not broker
  failover.
- The network is real: a hop between machines costs ~0.05-0.1 ms in a cluster placement group, against ~0.02 ms
  on loopback.
- k6 stops competing with the coordinator for CPU.
