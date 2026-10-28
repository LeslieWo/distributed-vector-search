#!/usr/bin/env bash
# Prints one line every INTERVAL seconds while a bulk load runs:
#   time, chunks indexed (sum of partition sizes over all shard-nodes / 2 replicas), chunks/s since the last line,
#   per-node WAL bytes, free disk, 1-minute load average.
# usage: scripts/load_progress.sh [interval_s=10] [nodes=3]
interval=${1:-10}; nodes=${2:-3}
prev=""; prev_t=""
while true; do
  t=$(date +%s)
  total=0; wals=""
  for n in $(seq 1 "$nodes"); do
    m=$(curl -s --max-time 2 "localhost:808$n/actuator/prometheus" || true)
    s=$(echo "$m" | awk '/^vs_shard_index_size_vectors/ {s+=$2} END {printf "%d", s}')
    w=$(echo "$m" | awk '/^vs_shard_wal_size_bytes/ {printf "%d", $2/1048576}')
    total=$((total + s)); wals="$wals n$n:${w:-?}MB"
  done
  chunks=$((total / 2))
  rate=""
  if [[ -n $prev ]]; then rate=$(( (chunks - prev) / (t - prev_t) )); fi
  free=$(df -g ~ | awk 'NR==2 {print $4}')
  load=$(sysctl -n vm.loadavg | awk '{print $2}')
  echo "$(date +%H:%M:%S) indexed=$chunks rate=${rate:-?}/s wal[$wals ] diskFreeGB=$free load1=$load"
  prev=$chunks; prev_t=$t
  sleep "$interval"
done
