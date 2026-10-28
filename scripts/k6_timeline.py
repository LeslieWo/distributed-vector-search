#!/usr/bin/env python3
"""Per-second timeline from a k6 CSV output (k6 run --out csv=FILE.csv.gz, K6_CSV_TIME_FORMAT=unix_milli).

usage: scripts/k6_timeline.py FILE.csv.gz [--events "kill=1730000000000,restart=..."] [--window START_S END_S]
Prints, for every second since the first request: requests, failed (non-OK gRPC status), partial, p50/p99/max
latency (ms), and a summary for the optional window (seconds relative to the first request).
"""
import argparse, csv, gzip, math, sys
from collections import defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("file")
ap.add_argument("--events", default="")
ap.add_argument("--window", nargs=2, type=float, default=None)
a = ap.parse_args()

lat = defaultdict(list)
failed = defaultdict(int)
partial = defaultdict(int)
t0 = None
rows = []
with gzip.open(a.file, "rt") as f:
    for r in csv.DictReader(f):
        name = r["metric_name"]
        if name not in ("grpc_req_duration", "search_failed", "search_partial"):
            continue
        ts = int(r["timestamp"])
        rows.append((ts, name, float(r["metric_value"])))
rows.sort()
t0 = rows[0][0]
for ts, name, v in rows:
    s = (ts - t0) // 1000
    if name == "grpc_req_duration":
        lat[s].append(v)
    elif name == "search_failed":
        failed[s] += int(v)
    else:
        partial[s] += int(v)

def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, math.ceil(p / 100 * len(xs)) - 1))] if xs else float("nan")

events = {}
for e in filter(None, a.events.split(",")):
    k, v = e.split("=")
    events[(int(v) - t0) // 1000] = events.get((int(v) - t0) // 1000, "") + " <- " + k
print("sec requests failed partial p50_ms p99_ms max_ms")
for s in range(0, max(lat) + 1):
    xs = lat.get(s, [])
    print(f"{s} {len(xs)} {failed[s]} {partial[s]} {pct(xs,50):.2f} {pct(xs,99):.2f} {max(xs) if xs else float('nan'):.2f}{events.get(s, '')}")
all_lat = [v for xs in lat.values() for v in xs]
print(f"TOTAL requests={len(all_lat)} failed={sum(failed.values())} partial={sum(partial.values())} "
      f"p50={pct(all_lat,50):.2f} p99={pct(all_lat,99):.2f} max={max(all_lat):.2f}")
if a.window:
    lo, hi = a.window
    w = [v for s, xs in lat.items() if lo <= s < hi for v in xs]
    wf = sum(n for s, n in failed.items() if lo <= s < hi)
    wp = sum(n for s, n in partial.items() if lo <= s < hi)
    print(f"WINDOW [{lo},{hi}) s requests={len(w)} failed={wf} partial={wp} p50={pct(w,50):.2f} p99={pct(w,99):.2f} max={max(w):.2f}")
