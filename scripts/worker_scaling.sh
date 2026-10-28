#!/usr/bin/env bash
# Embed-worker scaling: for each "N:T" (N workers in one consumer group, T onnxruntime intra-op threads each),
# start the workers, wait WARM seconds (JVM + model load + group join), then measure chunks indexed per second
# over MEASURE seconds from the shard-nodes' partition sizes (sum over nodes / 2 replicas). Workers are stopped
# gracefully between configs (they commit what they wrote). Needs the shards running and data in raw-chunks.
# usage: scripts/worker_scaling.sh "1:0 1:4 2:4 4:3 6:2" [warm=40] [measure=60]
cd "$(dirname "$0")/.."
configs=$1; warm=${2:-40}; measure=${3:-60}

indexed() {
  local total=0
  for n in 1 2 3; do
    s=$(curl -s --max-time 2 "localhost:808$n/actuator/prometheus" | awk '/^vs_shard_index_size_vectors/ {s+=$2} END {printf "%d", s}')
    total=$((total + s))
  done
  echo $((total / 2))
}

for c in $configs; do
  N=${c%%:*}; T=${c##*:}
  for i in $(seq 1 "$N"); do
    JAVA_OPTS="-Xmx1g" scripts/cluster.sh worker "$i" --worker.intra-op-threads="$T" > /dev/null
  done
  sleep "$warm"
  a=$(indexed); ta=$(date +%s)
  sleep "$measure"
  b=$(indexed); tb=$(date +%s)
  load=$(sysctl -n vm.loadavg | awk '{print $2}')
  echo "workers=$N intraOpThreads=$T chunks/s=$(( (b - a) / (tb - ta) )) indexed=$a->$b load1=$load"
  for i in $(seq 1 "$N"); do scripts/cluster.sh stop "worker-$i" > /dev/null; done
done
