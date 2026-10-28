#!/usr/bin/env bash
# Runs loadtest/search.js and records the test conditions and coordinator counters around it.
# usage: scripts/k6_run.sh <label> [k6 -e args...]   e.g. scripts/k6_run.sh baseline -e STEPS=200,400 -e STEP_SECONDS=60
# Writes data/run/k6-<label>.json (per-step table) and data/run/k6-<label>.txt (conditions + table + counter deltas).
cd "$(dirname "$0")/.."
label=$1; shift
out=data/run/k6-$label
counters() {
  curl -s localhost:8090/actuator/prometheus | awk '
    /^vs_coord_cache_(hits|misses)_total/ || /^vs_coord_hedge_(sent|won|denied)_total/ ||
    /^vs_coord_partition_requests_total/ || /^vs_coord_retries_total/ || /^vs_coord_partial_total/ ||
    /^vs_coord_node_marked_down_total/ { split($1, n, "{"); s[n[1]] += $2 }
    END { for (k in s) printf "%s %d\n", k, s[k] }' | sort
}
{
  echo "label=$label start=$(date '+%F %T')"
  pmset -g batt | head -1
  pmset -g | grep powermode
  uptime
  echo "args: $*"
} | tee "$out.txt"
counters > "$out.before"
k6 run -q -e OUT="$out.json" "$@" loadtest/search.js 2>&1 | grep -v '^\s*$' | tee -a "$out.txt"
counters > "$out.after"
echo "coordinator counter deltas:" | tee -a "$out.txt"
join "$out.before" "$out.after" | awk '{printf "  %s %d\n", $1, $3 - $2}' | tee -a "$out.txt"
echo "end=$(date '+%F %T') $(uptime | sed 's/.*load/load/')" | tee -a "$out.txt"
rm -f "$out.before" "$out.after"
