#!/usr/bin/env bash
set -euo pipefail

# Run a small demonstration on the bundled dataset.
echo "[demo] Running openfpv-compat on examples/parts.min.json"

openfpv-compat \
  --in examples/parts.min.json \
  --out edges_out \
  --print-summary

echo "[demo] Wrote CSVs into ./edges_out"

openfpv-compat \
  --in examples/parts.min.json \
  --out edges.xlsx \
  --merge

echo "[demo] Wrote Excel workbook edges.xlsx"
