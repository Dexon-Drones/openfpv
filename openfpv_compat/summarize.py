# -*- coding: utf-8 -*-
"""Summarize a dict of compatibility DataFrames into PASS/FAIL/UNKNOWN counts."""
from __future__ import annotations

from typing import Dict

import pandas as pd

from .schema import PASS_VALUES


def summarize(compat: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for key, df in compat.items():
        if df is None or df.empty:
            rows.append({"pair": key, "pass": 0, "fail": 0, "unknown": 0})
            continue
        if "status" not in df.columns:
            # Structural pair with no status → treat all as unknown
            rows.append({"pair": key, "pass": 0, "fail": 0, "unknown": len(df)})
            continue
        s = df["status"].astype(str).str.upper()
        rows.append({
            "pair": key,
            "pass": int(s.isin(PASS_VALUES).sum()),
            "fail": int((s == "FAIL").sum()),
            "unknown": int(((~s.isin(PASS_VALUES)) & (s != "FAIL")).sum()),
        })
    # Keep a stable order: Compat_* first
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(by=["pair"]).reset_index(drop=True)
    return out