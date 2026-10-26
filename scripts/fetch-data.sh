#!/usr/bin/env bash
# Downloads the MS MARCO passage subset, dev queries and the embedding model into data/ (gitignored).
# Streams the 1 GB collection archive and keeps only the first N passages, so the archive never hits disk.
set -euo pipefail

N="${1:-2000000}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="$ROOT/data"
MSMARCO_BASE="https://msmarco.z22.web.core.windows.net/msmarcoranking"
HF_BASE="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main"

mkdir -p "$DATA/msmarco" "$DATA/models/all-MiniLM-L6-v2"

if [[ ! -s "$DATA/msmarco/collection.2M.tsv" ]]; then
  echo "streaming first $N passages ..."
  # head exits after N lines; tar/curl then die on SIGPIPE (or get killed below if SIGPIPE is ignored).
  ( curl -sL "$MSMARCO_BASE/collection.tar.gz" | tar -xzOf - collection.tsv | head -n "$N" > "$DATA/msmarco/collection.2M.tsv" ) || true
fi
wc -l "$DATA/msmarco/collection.2M.tsv"

if [[ ! -s "$DATA/msmarco/queries.dev.tsv" ]]; then
  curl -sL -o "$DATA/msmarco/queries.tar.gz" "$MSMARCO_BASE/queries.tar.gz"
  tar -xzf "$DATA/msmarco/queries.tar.gz" -C "$DATA/msmarco"
  # only the dev queries are used (ground truth / recall); drop train + eval
  rm "$DATA/msmarco/queries.tar.gz" "$DATA/msmarco/queries.train.tsv" "$DATA/msmarco/queries.eval.tsv"
fi

for f in onnx/model.onnx tokenizer.json config.json tokenizer_config.json special_tokens_map.json; do
  out="$DATA/models/all-MiniLM-L6-v2/$(basename "$f")"
  [[ -s "$out" ]] || curl -sL -o "$out" "$HF_BASE/$f"
done
ls -la "$DATA/msmarco" "$DATA/models/all-MiniLM-L6-v2"
