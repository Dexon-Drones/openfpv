#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
openfpv-compat CLI

Examples
--------
# Minimal: read parts JSON, write one CSV per pair into ./edges_out
openfpv-compat --in examples/parts.min.json --out edges_out

# Single merged CSV (adds a 'pair_type' column)
openfpv-compat -i examples/parts.min.json -o edges.csv --merge

# Write an Excel workbook (one sheet per pair) with stricter headroom
openfpv-compat -i examples/parts.min.json -o edges.xlsx --headroom 1.3

# PASS-only edges + a compact textual summary
openfpv-compat -i examples/parts.min.json -o edges_out --pass-only --print-summary
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# Package-local imports (implemented in openfpv_compat/)
from openfpv_compat import __version__ as PKG_VERSION
from openfpv_compat.engine import load_parts, build_compat
from openfpv_compat.summarize import summarize as summarize_edges
from openfpv_compat.schema import PASS_VALUES


# ------------------------------ helpers ------------------------------

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def _ext_of(path: Path) -> str:
    return path.suffix.lower()


def _is_dir_like(p: Path) -> bool:
    # Treat paths with no suffix as directories
    return (not p.suffix) or p.as_posix().endswith("/")


def _safe_sheet_name(name: str) -> str:
    """Excel sheet names must be <= 31 chars."""
    return name[:31] if len(name) > 31 else name


def _filter_pass_only(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return df
    if "status" not in df.columns:
        # If no status column, keep as-is (structural pairs like mounts usually have it)
        return df
    mask = df["status"].astype(str).str.upper().isin(PASS_VALUES)
    out = df[mask].copy()
    return out


def _concat_with_pair_type(compat: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    parts = []
    for key, df in compat.items():
        if df is None or df.empty:
            continue
        d = df.copy()
        d.insert(0, "pair_type", key)
        parts.append(d)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _write_csv_dir(out_dir: Path, compat: Dict[str, pd.DataFrame]) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []
    for key, df in compat.items():
        if df is None or df.empty:
            continue
        fp = out_dir / f"{key}.csv"
        df.to_csv(fp, index=False)
        written.append(fp)
    return written


def _write_json(out_path: Path, compat: Dict[str, pd.DataFrame], merge: bool) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if merge:
        big = _concat_with_pair_type(compat)
        payload = big.to_dict(orient="records")
    else:
        payload = {k: (v.to_dict(orient="records") if v is not None else []) for k, v in compat.items()}
    out_path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return out_path


def _write_xlsx(out_path: Path, compat: Dict[str, pd.DataFrame], merge: bool) -> Path:
    try:
        import openpyxl  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise SystemExit("openpyxl is required for .xlsx output. Install with: pip install openpyxl") from e

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as writer:
        if merge:
            big = _concat_with_pair_type(compat)
            (big if not big.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name=_safe_sheet_name("CompatEdges"))
        else:
            # one sheet per pair
            # keep a deterministic order: Compat_* first, then any others
            keys = sorted(compat.keys(), key=lambda k: (not k.startswith("Compat_"), k))
            for key in keys:
                df = compat.get(key)
                if df is None:
                    continue
                (df if not df.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name=_safe_sheet_name(key.replace("Compat_", "")))
    return out_path


# ------------------------------ CLI core ------------------------------

def run_cli(args: argparse.Namespace) -> int:
    in_paths = [Path(p) for p in (args.input if isinstance(args.input, list) else [args.input])]
    headroom = float(args.headroom)

    # 1) Load parts (engine handles CSV/JSON and directory expansion)
    df = load_parts(in_paths)
    if df is None or df.empty:
        eprint("No parts loaded. Check --in path(s) and format (CSV/JSON).")
        return 2

    eprint(f"[compat] Loaded parts: {len(df):,}")

    # 2) Build compat edges
    compat = build_compat(df, headroom=headroom)
    if args.pass_only:
        compat = {k: _filter_pass_only(v) for k, v in compat.items()}

    # 3) Optional print summary to stdout
    if args.print_summary:
        summary = summarize_edges(compat)
        if summary is not None and not summary.empty:
            # minimal, aligned stdout table
            pair_width = int(summary["pair"].astype(str).map(len).max())
            widths = {
                "pair": max(16, pair_width),
                "pass": 5, "fail": 5, "unknown": 7,
            }
            header = f"{'PAIR'.ljust(widths['pair'])}  PASS  FAIL  UNK"
            print(header)
            print("-" * len(header))
            for _, r in summary.iterrows():
                print(
                    f"{str(r['pair']).ljust(widths['pair'])}  "
                    f"{int(r['pass']):>4}  {int(r['fail']):>4}  {int(r['unknown']):>3}"
                )

    # 4) Write outputs
    out_path = Path(args.out)
    fmt = (args.format or "").lower().strip()
    if not fmt:
        # Infer from extension; default to CSV directory
        if _is_dir_like(out_path):
            fmt = "csv"
        else:
            ext = _ext_of(out_path)
            fmt = "xlsx" if ext == ".xlsx" else ("json" if ext == ".json" else "csv")

    if fmt == "csv":
        if args.merge and not _is_dir_like(out_path):
            # merged CSV into a single file
            big = _concat_with_pair_type(compat)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            big.to_csv(out_path, index=False)
            eprint(f"[compat] Wrote merged CSV: {out_path}")
        else:
            # directory with one CSV per pair
            dir_path = out_path if _is_dir_like(out_path) else out_path.parent / out_path.stem
            written = _write_csv_dir(dir_path, compat)
            eprint(f"[compat] Wrote {len(written)} CSV(s) → {dir_path}")
    elif fmt == "xlsx":
        fp = _write_xlsx(out_path, compat, merge=bool(args.merge))
        eprint(f"[compat] Wrote workbook: {fp}")
    elif fmt == "json":
        fp = _write_json(out_path, compat, merge=bool(args.merge))
        eprint(f"[compat] Wrote JSON: {fp}")
    else:
        eprint(f"Unsupported --format '{fmt}'. Use csv|json|xlsx.")
        return 2

    return 0


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="openfpv-compat",
        description="Compute FPV parts compatibility edges from a typed parts table.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "-i", "--in", dest="input", nargs="+", required=True,
        help="Input file(s) or directory(ies). CSV or JSON. If a directory is provided, "
             "all *.csv and *.json files will be loaded and concatenated."
    )
    p.add_argument(
        "-o", "--out", required=True,
        help="Output file or directory. "
             "• For CSV: a directory (one CSV per pair) OR a single file with --merge. "
             "• For JSON/XLSX: a single file path."
    )
    p.add_argument(
        "-f", "--format", choices=["csv", "json", "xlsx"], default=None,
        help="Output format. If omitted, inferred from --out (directory → csv, .json → json, .xlsx → xlsx)."
    )
    p.add_argument(
        "--merge", action="store_true",
        help="Write a single merged table with a 'pair_type' column (CSV/JSON/XLSX)."
    )
    p.add_argument(
        "--pass-only", action="store_true",
        help="Keep only PASS rows (when a 'status' column is present)."
    )
    p.add_argument(
        "--print-summary", action="store_true",
        help="Print a compact PASS/FAIL/UNKNOWN summary to stdout after computing edges."
    )
    p.add_argument(
        "-H", "--headroom", type=float, default=1.2,
        help="ESC↔Motor strict headroom factor."
    )
    p.add_argument(
        "-V", "--version", action="version",
        version=f"%(prog)s {PKG_VERSION}",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = _parse_args(argv)
    try:
        code = run_cli(args)
    except KeyboardInterrupt:
        eprint("Interrupted.")
        code = 130
    sys.exit(code)


if __name__ == "__main__":
    main()