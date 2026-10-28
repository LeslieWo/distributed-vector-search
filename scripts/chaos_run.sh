#!/usr/bin/env bash
# Chaos: constant-rate k6 load; kill -9 one shard-node at KILL_AT seconds, restart it at RESTART_AT seconds.
# usage: scripts/chaos_run.sh <label> <shard n> <qps> [seconds=180] [kill_at=60] [restart_at=120]
# Writes data/run/k6-<label>.{txt,json,csv.gz} and data/run/chaos-<label>.events (epoch ms of kill / restart / back).
cd "$(dirname "$0")/.."
label=$1; n=$2; qps=$3; secs=${4:-180}; kill_at=${5:-60}; restart_at=${6:-120}
[[ $n =~ ^[1-9]$ && $qps =~ ^[0-9]+$ ]] || { echo "usage: $0 <label> <shard n> <qps> ..." >&2; exit 2; }
if [[ -z ${ALLOW_BATTERY:-} ]] && { ! pmset -g batt | grep -q "AC Power" || pmset -g | grep -q "powermode *1"; }; then
  echo "refusing to run: not on AC power or low power mode is on" >&2; exit 3
fi
ev=data/run/chaos-$label.events; : > "$ev"
(
  sleep "$kill_at"
  echo "kill=$(python3 -c 'import time;print(int(time.time()*1000))')" >> "$ev"
  scripts/cluster.sh kill9 "shard-$n" > /dev/null
  sleep $((restart_at - kill_at))
  mv "data/run/shard-$n.log" "data/run/shard-$n.$label.killed.log"
  echo "restart=$(python3 -c 'import time;print(int(time.time()*1000))')" >> "$ev"
  JAVA_OPTS="-Xmx8g -Xlog:gc:file=data/run/gc-shard-$n.log" scripts/cluster.sh shard "$n" > /dev/null
  until grep -q "Started ShardNodeApplication" "data/run/shard-$n.log" 2>/dev/null; do sleep 0.2; done
  echo "back=$(python3 -c 'import time;print(int(time.time()*1000))')" >> "$ev"
) &
K6_CSV_TIME_FORMAT=unix_milli scripts/k6_run.sh "$label" --out csv="data/run/k6-$label.csv.gz" -e STEPS="$qps" -e STEP_SECONDS="$secs" -e MAX_VUS=2000
wait
cat "$ev"
