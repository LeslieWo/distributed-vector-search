#!/usr/bin/env bash
# Local cluster from the built jars (./gradlew build first). Logs and pids in data/run/.
#   scripts/cluster.sh shard <n> [extra args]   shard-node-n on gRPC 910n / HTTP 808n
#   scripts/cluster.sh worker [n] [extra args]     embed-worker-n (default 1), actuator on 8075 + 100*(n-1)
#   scripts/cluster.sh ingest|coordinator [extra args]
#   JAVA_OPTS="-Xmx8g" scripts/cluster.sh ...      JVM options (default -Xmx2g)
#   scripts/cluster.sh kill9 <name>             e.g. kill9 shard-2 (crash, no shutdown hooks)
#   scripts/cluster.sh stop [name]              graceful (SIGTERM) stop of one or all
#   scripts/cluster.sh status
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=data/run
mkdir -p "$RUN"
JAVA_OPTS=${JAVA_OPTS:-"-Xmx2g"}

start() { # name jar args...
  local name=$1 jar=$2; shift 2
  if [[ -f $RUN/$name.pid ]] && kill -0 "$(cat $RUN/$name.pid)" 2>/dev/null; then echo "$name already running"; return; fi
  nohup java $JAVA_OPTS -jar "$jar" "$@" > "$RUN/$name.log" 2>&1 &
  echo $! > "$RUN/$name.pid"
  echo "started $name pid $! (log $RUN/$name.log)"
}

case ${1:-} in
  shard)
    n=$2; shift 2
    start "shard-$n" shard-node/build/libs/shard-node-0.1.0.jar \
      --shard.node-id=node-$n --shard.grpc-port=910$n --server.port=808$n "$@" ;;
  worker)
    n=1; shift
    if [[ ${1:-} =~ ^[0-9]+$ ]]; then n=$1; shift; fi
    start "worker-$n" embed-worker/build/libs/embed-worker-0.1.0.jar --server.port=$((8075 + 100 * (n - 1))) "$@" ;;
  ingest)      shift; start ingest ingest/build/libs/ingest-0.1.0.jar "$@" ;;
  coordinator) shift; start coordinator coordinator/build/libs/coordinator-0.1.0.jar "$@" ;;
  kill9)
    kill -9 "$(cat $RUN/$2.pid)" && rm -f "$RUN/$2.pid" && echo "kill -9 $2" ;;
  stop)
    for f in $RUN/${2:-*}.pid; do
      [[ -f $f ]] || continue
      pid=$(cat "$f"); kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
      rm -f "$f"; echo "stopped $(basename "$f" .pid)"
    done ;;
  status)
    for f in $RUN/*.pid; do [[ -f $f ]] || continue; pid=$(cat "$f")
      if kill -0 "$pid" 2>/dev/null; then echo "$(basename "$f" .pid) up ($pid)"; else echo "$(basename "$f" .pid) DEAD"; fi; done ;;
  *) sed -n 2,9p "$0"; exit 2 ;;
esac
