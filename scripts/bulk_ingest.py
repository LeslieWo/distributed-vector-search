#!/usr/bin/env python3
"""Stream the whole MS MARCO subset into the ingest service (stdlib only, constant memory).

usage: scripts/bulk_ingest.py [--start 0] [--count N] [--batch 1000] [--threads 4] [--prefix ""]
docId = <prefix><pid>. Requests run on a small thread pool; a request that fails is retried (ingest is
idempotent by chunk id). Prints progress every 100 requests and the total chunk count accepted by ingest.
"""
import argparse, json, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

ap = argparse.ArgumentParser()
ap.add_argument("--start", type=int, default=0)
ap.add_argument("--count", type=int, default=10**9)
ap.add_argument("--batch", type=int, default=1000)
ap.add_argument("--threads", type=int, default=4)
ap.add_argument("--prefix", default="")
ap.add_argument("--url", default="http://localhost:8070/documents")
ap.add_argument("--corpus", default="data/msmarco/collection.2M.tsv")
a = ap.parse_args()


def batches():
    docs = []
    with open(a.corpus, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i < a.start:
                continue
            if i >= a.start + a.count:
                break
            pid, text = line.rstrip("\n").split("\t", 1)
            docs.append({"docId": a.prefix + pid, "text": text})
            if len(docs) == a.batch:
                yield docs
                docs = []
    if docs:
        yield docs


def post(docs):
    body = json.dumps({"documents": docs}).encode()
    for attempt in range(10):
        try:
            req = urllib.request.Request(a.url, data=body, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=120) as resp:
                out = json.load(resp)
            return out["documents"], out["chunks"], attempt
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            print(f"retry after {e}", file=sys.stderr, flush=True)
            time.sleep(1 + attempt)
    raise RuntimeError("request failed 10 times")


docs_total = chunks_total = retries = 0
t0 = time.time()
with ThreadPoolExecutor(a.threads) as pool:
    pending = []
    for n, b in enumerate(batches(), 1):
        pending.append(pool.submit(post, b))
        if len(pending) >= a.threads * 2:  # bounded in-flight requests: constant memory
            d, c, r = pending.pop(0).result()
            docs_total += d; chunks_total += c; retries += r
        if n % 100 == 0:
            el = time.time() - t0
            print(f"{n} requests submitted, accepted docs={docs_total} chunks={chunks_total} "
                  f"({docs_total / el:.0f} docs/s)", flush=True)
    for f in pending:
        d, c, r = f.result()
        docs_total += d; chunks_total += c; retries += r
el = time.time() - t0
print(f"total docs={docs_total} chunks={chunks_total} retries={retries} in {el:.1f}s "
      f"({docs_total / el:.0f} docs/s, {chunks_total / el:.0f} chunks/s)")
