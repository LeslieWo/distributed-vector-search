#!/usr/bin/env python3
"""POST MS MARCO passages to the ingest service in batches (stdlib only).

usage: scripts/ingest_passages.py START COUNT [--prefix P] [--batch 100] [--url http://localhost:8070/documents]
       [--pause-after N --pause-cmd "scripts/cluster.sh kill9 shard-2"]
docId = <prefix><pid>. Prints one line per request and the total chunk count.
"""
import argparse, json, subprocess, sys, time, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("start", type=int)
ap.add_argument("count", type=int)
ap.add_argument("--prefix", default="p")
ap.add_argument("--batch", type=int, default=100)
ap.add_argument("--url", default="http://localhost:8070/documents")
ap.add_argument("--corpus", default="data/msmarco/collection.2M.tsv")
ap.add_argument("--after-request", type=int, default=-1, help="run --cmd after this many requests")
ap.add_argument("--cmd", default="")
a = ap.parse_args()

docs = []
with open(a.corpus, encoding="utf-8") as f:
    for i, line in enumerate(f):
        if i < a.start:
            continue
        if i >= a.start + a.count:
            break
        pid, text = line.rstrip("\n").split("\t", 1)
        docs.append({"docId": a.prefix + pid, "text": text})

chunks = 0
t0 = time.time()
for r, off in enumerate(range(0, len(docs), a.batch), 1):
    body = json.dumps({"documents": docs[off:off + a.batch]}).encode()
    req = urllib.request.Request(a.url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        out = json.load(resp)
    chunks += out["chunks"]
    print(f"request {r}: HTTP {resp.status} docs={out['documents']} chunks={out['chunks']}", flush=True)
    if r == a.after_request and a.cmd:
        print("running:", a.cmd, flush=True)
        subprocess.run(a.cmd, shell=True, check=True)
print(f"total docs={len(docs)} chunks={chunks} in {time.time() - t0:.1f}s")
