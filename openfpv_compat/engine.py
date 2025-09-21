# -*- coding: utf-8 -*-
"""
Engine: load parts → DataFrame, then compute compatibility joins.

This module intentionally mirrors the public, open algorithm surface:
- load_parts(paths): robust reader for CSV/JSON (with optional attrs flattening)
- build_compat(df, headroom): returns a dict[str, DataFrame] named 'Compat_*'
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

from .schema import (
    ALL_FIELDS, ALLOWED_MOUNTS,
    normalize_type,
)

# ------------------------- Normalization helpers -------------------------

def _canon_mount_list(val: object) -> List[str]:
    """Normalize a single string or a list of tokens to canonical mount patterns."""
    if val is None:
        return []
    toks: List[str]
    if isinstance(val, list):
        toks = [str(x) for x in val]
    else:
        toks = str(val).replace("×", "x").replace(" ", "").split(",")
    out: List[str] = []
    for t in toks:
        t = t.strip().lower()
        if not t:
            continue
        if t == "30x30":
            t = "30.5x30.5"
        if t == "25x25":
            t = "25.5x25.5"
        if t in ALLOWED_MOUNTS and t not in out:
            out.append(t)
    return out


def _canon_connector(value: Optional[str]) -> Optional[str]:
    """Canonicalize RF connector tokens; power connectors handled separately."""
    if value is None:
        return None
    s = str(value).strip().upper().replace("_", " ").replace("-", " ").replace(".", " ")
    s = " ".join(s.split())
    if "RP SMA" in s:
        return "RP-SMA"
    if "SMA" in s:
        return "SMA"
    if "MMCX" in s:
        return "MMCX"
    if "MCX" in s and "MMCX" not in s:
        return "MCX"
    if "MHF4" in s or "IPEX 4" in s:
        return "IPEX MHF4"
    if "MHF1" in s or "IPEX 1" in s:
        return "IPEX MHF1"
    if "U FL" in s or "UFL" in s or "IPEX" in s:
        return "U.FL"
    return None


def _canon_power_conn(s: object) -> Optional[str]:
    """Canonicalize battery/pigtail/ESC power connectors (XT30/60/90, EC3/EC5, JST-RCY...)."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    t = str(s).strip().upper().replace(" ", "").replace("-", "")
    if t in {"XT30", "AMASSXT30", "XT30U"}: return "XT30"
    if t in {"XT60", "AMASSXT60", "XT60H"}: return "XT60"
    if t in {"XT90", "XT90S"}: return "XT90"
    if t in {"EC3"}: return "EC3"
    if t in {"EC5"}: return "EC5"
    if t in {"JSTRCY", "JST"}: return "JST-RCY"
    return t  # uppercase, no spaces/hyphens as generic fallback


def _canon_prop_hub(s: object) -> Optional[str]:
    """Return a coarse prop hub class: 'M5' or 'T-MOUNT' (else None)."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    v = str(s).strip().lower().replace("-", "").replace("_", "")
    if "m5" in v or "5mm" in v:
        return "M5"
    if "tmount" in v or v in {"t", "tmnt"}:
        return "T-MOUNT"
    return None


def _motor_hub_from_shaft(shaft_mm: object) -> Optional[str]:
    """Infer motor prop interface from shaft diameter."""
    try:
        x = float(shaft_mm)
    except Exception:
        return None
    if pd.isna(x):
        return None
    if x >= 4.8:
        return "M5"
    if x <= 2.1:
        return "T-MOUNT"
    return None


def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _to_int(x) -> float:
    try:
        return int(float(x))
    except Exception:
        return float("nan")


def _is_dir(p: Path) -> bool:
    try:
        return p.is_dir()
    except Exception:
        return False


# ------------------------- Data loading -------------------------

def _read_one_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # If there's an 'attrs' column with JSON, expand it.
    if "attrs" in df.columns:
        try:
            expanded = df["attrs"].apply(
                lambda v: (json.loads(v) if isinstance(v, str) and v.strip().startswith("{") else v)
            )
            attrs_df = pd.json_normalize(expanded)
            df = pd.concat([df.drop(columns=["attrs"]), attrs_df], axis=1)
        except Exception:
            pass
    return df


def _extract_items(obj) -> List[dict]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        if isinstance(obj.get("parts"), list):
            return [x for x in obj["parts"] if isinstance(x, dict)]
        if isinstance(obj.get("data"), list):
            return [x for x in obj["data"] if isinstance(x, dict)]
    return []


def _read_one_json(path: Path) -> pd.DataFrame:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return pd.DataFrame()
    rows = _extract_items(payload)
    if not rows:
        # maybe it's a dict representing a single row
        if isinstance(payload, dict):
            rows = [payload]
    df = pd.json_normalize(rows)

    # If a row has nested attrs.*, flatten into top-level
    if "attrs" in df.columns:
        try:
            attrs_df = pd.json_normalize(df["attrs"])
            df = pd.concat([df.drop(columns=["attrs"]), attrs_df], axis=1)
        except Exception:
            pass
    return df


def _collect_files(paths: Iterable[Path]) -> List[Path]:
    files: List[Path] = []
    for p in paths:
        if _is_dir(p):
            # pick *.csv and *.json in the directory (non-recursive keeps it simple)
            files.extend(sorted(p.glob("*.csv")))
            files.extend(sorted(p.glob("*.json")))
        else:
            files.append(p)
    # de-dup while preserving order
    seen = set()
    uniq = []
    for f in files:
        key = str(f.resolve())
        if key not in seen:
            seen.add(key)
            uniq.append(f)
    return uniq


def load_parts(paths: Iterable[Path]) -> pd.DataFrame:
    """
    Read one or more CSV/JSON sources and return a normalized DataFrame with at least:
      - sku (string)
      - type (canonical lowercase)
      - name (generic name / title)
    Plus any typed fields required by the rules in build_compat().
    """
    files = _collect_files(paths)
    if not files:
        return pd.DataFrame(columns=["sku", "type", "name"])

    frames: List[pd.DataFrame] = []
    for f in files:
        if f.suffix.lower() == ".csv":
            frames.append(_read_one_csv(f))
        elif f.suffix.lower() == ".json":
            frames.append(_read_one_json(f))

    if not frames:
        return pd.DataFrame(columns=["sku", "type", "name"])

    df = pd.concat(frames, ignore_index=True, sort=False)

    # Canonicalize core columns
    # allow 'part_type' or 'type'
    if "type" not in df.columns and "part_type" in df.columns:
        df = df.rename(columns={"part_type": "type"})
    if "name" not in df.columns and "generic_name" in df.columns:
        df = df.rename(columns={"generic_name": "name"})

    for c in ("sku", "type", "name"):
        if c not in df.columns:
            df[c] = ""

    df["sku"] = df["sku"].astype(str)
    df["name"] = df["name"].astype(str)
    df["type"] = df["type"].astype(str).map(normalize_type)

    # Ensure all fields exist as columns (so downstream code can select safely)
    for col in ALL_FIELDS:
        if col not in df.columns:
            df[col] = np.nan

    # Make sure numeric-ish columns are floats (safe for NaN)
    numeric_cols = [
        # frame/prop
        "frame_max_prop_in", "prop_diameter_in", "prop_pitch_in", "prop_blade_count",
        # mounts (leave string), power/cells
        "shaft_mm", "max_current_a", "cells_min", "cells_max",
        "esc_continuous_current_a", "esc_cells_min", "esc_cells_max",
        "cell_count",
        "fc_bec_5v_a", "fc_bec_9v_a", "fc_bec_10v_a", "fc_bec_12v_a",
        "fc_vbat_min_s", "fc_vbat_max_s",
        "vtx_5v_current_a", "vtx_9v_current_a", "vtx_10v_current_a", "vtx_12v_current_a",
        "vtx_input_s_min", "vtx_input_s_max",
        "camera_input_v_min", "camera_input_v_max",
        "camera_5v_current_a", "camera_10v_current_a", "camera_12v_current_a",
        "antenna_band_ghz",
        "pigtail_awg", "pigtail_length_mm",
        "capacitor_voltage_v", "capacitor_uf",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(_to_float)

    # Normalize mount pattern lists to comma-separated canonical form for string columns we expect
    for col in ["frame_fc_mount_pattern_eff", "frame_motor_mount_pattern_eff", "fc_mount_pattern_eff", "esc_mount_pattern_eff", "motor_mount_pattern_eff"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: ", ".join(_canon_mount_list(v)) if isinstance(v, (str, list)) else np.nan)

    # Normalize RF connectors where appropriate (produce an *_eff that is canonical)
    if "vtx_ant_conn" in df.columns and "vtx_ant_conn_eff" not in df.columns:
        df["vtx_ant_conn_eff"] = df["vtx_ant_conn"].apply(_canon_connector)

    if "antenna_conn" in df.columns:
        df["antenna_conn_eff"] = df["antenna_conn"].apply(_canon_connector)

    # Normalize power connectors used for joins
    df["battery_connector_eff"] = df["battery_connector"].apply(_canon_power_conn) if "battery_connector" in df.columns else np.nan
    df["esc_batt_connector_eff"] = df["esc_batt_connector"].apply(_canon_power_conn) if "esc_batt_connector" in df.columns else np.nan
    df["pigtail_connector_eff"]  = df["pigtail_connector"].apply(_canon_power_conn)  if "pigtail_connector"  in df.columns else np.nan

    # Normalize prop hub for prop rows (M5/T-MOUNT coarse buckets)
    if "prop_hub" in df.columns:
        df["prop_hub_eff"] = df["prop_hub"].apply(_canon_prop_hub)
    else:
        df["prop_hub_eff"] = np.nan

    return df


# ------------------------- Compat primitives -------------------------

def _cartesian(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    if a.empty or b.empty:
        return pd.DataFrame()
    a_ = a.copy(); b_ = b.copy()
    a_["__k"] = 1; b_["__k"] = 1
    return a_.merge(b_, on="__k").drop(columns="__k")


def _s_overlap(a_min, a_max, b_min, b_max) -> Optional[bool]:
    if any(pd.isna(v) for v in [a_min, a_max, b_min, b_max]):
        return None
    return max(a_min, b_min) <= min(a_max, b_max)


# ------------------------- Core algorithm -------------------------

def build_compat(parts: pd.DataFrame, headroom: float = 1.2) -> Dict[str, pd.DataFrame]:
    """
    Compute pairwise compatibility DataFrames. Keys follow the "Compat_*" naming.
    Each DataFrame includes a 'status' column ('PASS'/'FAIL'/'WARN') and a 'reason'.
    """
    df = parts.copy()

    # Slices
    frames  = df[df["type"] == "frame"].copy()
    motors  = df[df["type"] == "motor"].copy()
    escs    = df[df["type"] == "esc"].copy()
    bats    = df[df["type"] == "battery"].copy()
    fcs     = df[df["type"] == "fc"].copy()
    vtxs    = df[df["type"] == "vtx"].copy()
    cams    = df[df["type"] == "camera"].copy()
    props   = df[df["type"] == "prop"].copy()
    ants    = df[df["type"] == "antenna"].copy()
    rxs     = df[df["type"] == "rx"].copy()
    pigtail = df[df["type"] == "pigtail"].copy()
    caps    = df[df["type"] == "capacitor"].copy()

    out: Dict[str, pd.DataFrame] = {}

    # frame ↔ prop (size)
    frp = frames.dropna(subset=["frame_max_prop_in"])[["sku", "name", "frame_max_prop_in"]]
    prp = props.dropna(subset=["prop_diameter_in"])[["sku", "name", "prop_diameter_in"]]
    j = _cartesian(frp, prp)
    if not j.empty:
        j = j.rename(columns={"sku_x":"frame_sku","name_x":"frame_name","sku_y":"prop_sku","name_y":"prop_name"})
        j["status"] = np.where(j["prop_diameter_in"] <= j["frame_max_prop_in"], "PASS", "FAIL")
        j["reason"] = np.where(j["status"]=="PASS", "Prop diameter ≤ frame max", "Prop diameter > frame max")
    out["Compat_frame_prop"] = j[["frame_sku","frame_name","prop_sku","prop_name","frame_max_prop_in","prop_diameter_in","status","reason"]] if not j.empty else pd.DataFrame(columns=["frame_sku","frame_name","prop_sku","prop_name","frame_max_prop_in","prop_diameter_in","status","reason"])

    # frame ↔ fc (mount)
    fr_fc = frames.dropna(subset=["frame_fc_mount_pattern_eff"])[["sku","name","frame_fc_mount_pattern_eff"]].copy()
    fc_m  = fcs.dropna(subset=["fc_mount_pattern_eff"])[["sku","name","fc_mount_pattern_eff"]].copy()
    if not fr_fc.empty and not fc_m.empty:
        fr_fc["mounts"] = fr_fc["frame_fc_mount_pattern_eff"].apply(lambda s: set(_canon_mount_list(s)))
        fc_m["mounts"]  = fc_m["fc_mount_pattern_eff"].apply(lambda s: set(_canon_mount_list(s)))
        x = _cartesian(fr_fc, fc_m).rename(columns={"sku_x":"frame_sku","name_x":"frame_name","sku_y":"fc_sku","name_y":"fc_name"})
        x["overlap"] = x.apply(lambda r: bool(r["mounts_x"] & r["mounts_y"]), axis=1)
        x["status"] = np.where(x["overlap"], "PASS", "FAIL")
        x["reason"] = np.where(x["overlap"], "Mount pattern overlaps", "No common mount pattern")
        out["Compat_frame_fc"] = x[["frame_sku","frame_name","fc_sku","fc_name","status","reason"]]
    else:
        out["Compat_frame_fc"] = pd.DataFrame(columns=["frame_sku","frame_name","fc_sku","fc_name","status","reason"])

    # frame ↔ motor (arm mount)
    fr_mo = frames.dropna(subset=["frame_motor_mount_pattern_eff"])[["sku","name","frame_motor_mount_pattern_eff"]].copy()
    mo_m  = motors.dropna(subset=["motor_mount_pattern_eff"])[["sku","name","motor_mount_pattern_eff"]].copy()
    if not fr_mo.empty and not mo_m.empty:
        fr_mo["mounts"] = fr_mo["frame_motor_mount_pattern_eff"].apply(lambda s: set(_canon_mount_list(s)))
        mo_m["mounts"]  = mo_m["motor_mount_pattern_eff"].apply(lambda s: set(_canon_mount_list(s)))
        x = _cartesian(fr_mo, mo_m).rename(columns={"sku_x":"frame_sku","name_x":"frame_name","sku_y":"motor_sku","name_y":"motor_name"})
        x["overlap"] = x.apply(lambda r: bool(r["mounts_x"] & r["mounts_y"]), axis=1)
        x["status"] = np.where(x["overlap"], "PASS", "FAIL")
        x["reason"] = np.where(x["overlap"], "Mount pattern overlaps", "No common mount pattern")
        out["Compat_frame_motor"] = x[["frame_sku","frame_name","motor_sku","motor_name","status","reason"]]
    else:
        out["Compat_frame_motor"] = pd.DataFrame(columns=["frame_sku","frame_name","motor_sku","motor_name","status","reason"])

    # esc ↔ fc (stack/mount)
    es_m = escs.dropna(subset=["esc_mount_pattern_eff"])[["sku","name","esc_mount_pattern_eff"]].copy()
    fc_m = fcs.dropna(subset=["fc_mount_pattern_eff"])[["sku","name","fc_mount_pattern_eff"]].copy()
    if not es_m.empty and not fc_m.empty:
        es_m["mounts"] = es_m["esc_mount_pattern_eff"].apply(lambda s: set(_canon_mount_list(s)))
        fc_m["mounts"] = fc_m["fc_mount_pattern_eff"].apply(lambda s: set(_canon_mount_list(s)))
        x = _cartesian(es_m, fc_m).rename(columns={"sku_x":"esc_sku","name_x":"esc_name","sku_y":"fc_sku","name_y":"fc_name"})
        x["overlap"] = x.apply(lambda r: bool(r["mounts_x"] & r["mounts_y"]), axis=1)
        x["status"] = np.where(x["overlap"], "PASS", "FAIL")
        x["reason"] = np.where(x["overlap"], "Form factor matches", "No common mount pattern")
        out["Compat_esc_fc"] = x[["esc_sku","esc_name","fc_sku","fc_name","status","reason"]]
    else:
        out["Compat_esc_fc"] = pd.DataFrame(columns=["esc_sku","esc_name","fc_sku","fc_name","status","reason"])

    # battery ↔ esc (connector + cells if available)
    ba = bats[["sku","name","battery_connector_eff","cell_count"]].dropna(subset=["battery_connector_eff"]).copy()
    es = escs[["sku","name","esc_batt_connector_eff","esc_cells_min","esc_cells_max"]].dropna(subset=["esc_batt_connector_eff"]).copy()
    j = ba.merge(es, left_on="battery_connector_eff", right_on="esc_batt_connector_eff", how="inner")
    if not j.empty:
        j["status"] = "PASS"
        j["reason"] = "Connector matched"
        # if cells known, refine status
        mask_known = j["cell_count"].notna() & j["esc_cells_min"].notna() & j["esc_cells_max"].notna()
        within = (j["cell_count"] >= j["esc_cells_min"]) & (j["cell_count"] <= j["esc_cells_max"])
        j.loc[mask_known & ~within, "status"] = "FAIL"
        j.loc[mask_known & ~within, "reason"] = "Cells out of ESC range"
        j.loc[mask_known & within, "reason"]  = "Connector + cells OK"
    out["Compat_battery_esc"] = j.rename(columns={"sku_x":"battery_sku","name_x":"battery_name","sku_y":"esc_sku","name_y":"esc_name"})[[
        "battery_sku","battery_name","esc_sku","esc_name","battery_connector_eff","esc_cells_min","esc_cells_max","cell_count","status","reason"
    ]] if not j.empty else pd.DataFrame(columns=["battery_sku","battery_name","esc_sku","esc_name","battery_connector_eff","esc_cells_min","esc_cells_max","cell_count","status","reason"])

    # esc ↔ motor (basic) and strict headroom
    es2 = escs[["sku","name","esc_continuous_current_a","esc_cells_min","esc_cells_max"]].copy()
    mo2 = motors[["sku","name","max_current_a","cells_min","cells_max"]].copy()
    base = _cartesian(es2, mo2)
    if not base.empty:
        # Basic
        def _eval_basic(r):
            cur_ok = (pd.notna(r["esc_continuous_current_a"]) and pd.notna(r["max_current_a"]) and r["esc_continuous_current_a"] >= r["max_current_a"])
            cells_ok = _s_overlap(r["esc_cells_min"], r["esc_cells_max"], r["cells_min"], r["cells_max"])
            if cells_ok is False: return ("FAIL", "Cell ranges do not overlap")
            if cur_ok: return ("PASS", "current OK" + (" & cells overlap" if cells_ok else ""))
            if cells_ok: return ("PASS", "cells overlap (current unknown)")
            return ("WARN", "insufficient data")
        b = base.copy()
        b[["status","reason"]] = b.apply(_eval_basic, axis=1, result_type="expand")
        out["Compat_esc_motor"] = b.rename(columns={"sku_x":"esc_sku","name_x":"esc_name","sku_y":"motor_sku","name_y":"motor_name"})[[
            "esc_sku","esc_name","motor_sku","motor_name","esc_continuous_current_a","max_current_a","esc_cells_min","esc_cells_max","cells_min","cells_max","status","reason"
        ]]
        # Headroom
        def _eval_head(r):
            if pd.isna(r["esc_continuous_current_a"]) or pd.isna(r["max_current_a"]):
                return ("FAIL", "missing current rating(s)")
            cells_ok = _s_overlap(r["esc_cells_min"], r["esc_cells_max"], r["cells_min"], r["cells_max"])
            if cells_ok is False: return ("FAIL", "Cell ranges do not overlap")
            if cells_ok is None: return ("FAIL", "missing cell ranges")
            ok = r["esc_continuous_current_a"] >= headroom * r["max_current_a"]
            return ("PASS" if ok else "FAIL", f"current {'≥' if ok else '<'} {headroom:.1f}× & cells overlap")
        h = base.copy()
        h[["status","reason"]] = h.apply(_eval_head, axis=1, result_type="expand")
        out["Compat_esc_motor_headroom"] = h.rename(columns={"sku_x":"esc_sku","name_x":"esc_name","sku_y":"motor_sku","name_y":"motor_name"})[[
            "esc_sku","esc_name","motor_sku","motor_name","esc_continuous_current_a","max_current_a","esc_cells_min","esc_cells_max","cells_min","cells_max","status","reason"
        ]]
    else:
        out["Compat_esc_motor"] = pd.DataFrame(columns=["esc_sku","esc_name","motor_sku","motor_name","esc_continuous_current_a","max_current_a","esc_cells_min","esc_cells_max","cells_min","cells_max","status","reason"])
        out["Compat_esc_motor_headroom"] = out["Compat_esc_motor"].copy()

    # vtx ↔ camera (ecosystem/system)
    vt = vtxs[["sku","name","vtx_system"]].dropna()
    cm = cams[["sku","name","camera_system"]].dropna()
    if not vt.empty and not cm.empty:
        vt["sys"] = vt["vtx_system"].astype(str).str.strip().str.lower()
        cm["sys"] = cm["camera_system"].astype(str).str.strip().str.lower()
        j = vt.merge(cm, on="sys", how="inner", suffixes=("_vtx","_cam"))
        j["status"] = "PASS"; j["reason"] = "Systems match"
        out["Compat_vtx_camera"] = j.rename(columns={"sku_vtx":"vtx_sku","name_vtx":"vtx_name","sku_cam":"camera_sku","name_cam":"camera_name"})[[
            "vtx_sku","vtx_name","camera_sku","camera_name","sys","status","reason"
        ]]
    else:
        out["Compat_vtx_camera"] = pd.DataFrame(columns=["vtx_sku","vtx_name","camera_sku","camera_name","sys","status","reason"])

    # vtx ↔ antenna (RF connector; require FPV-ish band if provided)
    vt2 = vtxs[["sku","name","vtx_ant_conn","vtx_ant_conn_eff"]].copy()
    an  = ants[["sku","name","antenna_conn","antenna_conn_eff","antenna_band_ghz","antenna_use"]].copy()
    if not vt2.empty and not an.empty:
        vt2["conn"] = vt2["vtx_ant_conn_eff"].combine_first(vt2["vtx_ant_conn"])
        an["conn"]  = an["antenna_conn_eff"].combine_first(an["antenna_conn"])
        # band/use gate
        ant_gate = (an["antenna_use"].astype(str).str.lower().eq("fpv")) | an["antenna_band_ghz"].between(5.4, 6.2, inclusive="both")
        an2 = an[ant_gate].copy()
        j = vt2.merge(an2, on="conn", how="inner", suffixes=("_vtx","_ant"))
        j["status"] = "PASS"; j["reason"] = "RF connector matches"
        out["Compat_vtx_antenna"] = j.rename(columns={"sku_vtx":"vtx_sku","name_vtx":"vtx_name","sku_ant":"antenna_sku","name_ant":"antenna_name"})[[
            "vtx_sku","vtx_name","antenna_sku","antenna_name","conn","status","reason"
        ]]
    else:
        out["Compat_vtx_antenna"] = pd.DataFrame(columns=["vtx_sku","vtx_name","antenna_sku","antenna_name","conn","status","reason"])

    # fc ↔ vtx VBAT S-overlap
    fcv = fcs.dropna(subset=["fc_vbat_min_s","fc_vbat_max_s"])[["sku","name","fc_vbat_min_s","fc_vbat_max_s"]]
    vti = vtxs.dropna(subset=["vtx_input_s_min","vtx_input_s_max"])[["sku","name","vtx_input_s_min","vtx_input_s_max"]]
    j = _cartesian(fcv, vti)
    if not j.empty:
        j = j.rename(columns={"sku_x":"fc_sku","name_x":"fc_name","sku_y":"vtx_sku","name_y":"vtx_name"})
        j["ok"] = j.apply(lambda r: _s_overlap(r["fc_vbat_min_s"], r["fc_vbat_max_s"], r["vtx_input_s_min"], r["vtx_input_s_max"]), axis=1)
        j["status"] = np.where(j["ok"] == True, "PASS", "FAIL")  # noqa: E712
        j["reason"] = np.where(j["status"]=="PASS", "VBAT ranges overlap", "VBAT ranges do not overlap")
    out["Compat_fc_vtx_vbat"] = j[["fc_sku","fc_name","vtx_sku","vtx_name","fc_vbat_min_s","fc_vbat_max_s","vtx_input_s_min","vtx_input_s_max","status","reason"]] if not j.empty else pd.DataFrame(columns=["fc_sku","fc_name","vtx_sku","vtx_name","fc_vbat_min_s","fc_vbat_max_s","vtx_input_s_min","vtx_input_s_max","status","reason"])

    # fc ↔ vtx rails (5/9/10/12V)
    def _rail(pair_key: str, fc_col: str, vtx_col: str, V: int) -> pd.DataFrame:
        a = fcs.dropna(subset=[fc_col])[["sku","name",fc_col]]
        b = vtxs.dropna(subset=[vtx_col])[["sku","name",vtx_col]]
        j = _cartesian(a, b)
        if j.empty:
            return pd.DataFrame(columns=["fc_sku","fc_name","vtx_sku","vtx_name",fc_col,vtx_col,"status","reason"])
        j = j.rename(columns={"sku_x":"fc_sku","name_x":"fc_name","sku_y":"vtx_sku","name_y":"vtx_name"})
        j["status"] = np.where(j[fc_col] >= j[vtx_col], "PASS", "FAIL")
        j["reason"] = np.where(j["status"]=="PASS", f"FC {V}V BEC ≥ VTX current", f"FC {V}V BEC < VTX current")
        return j[["fc_sku","fc_name","vtx_sku","vtx_name",fc_col,vtx_col,"status","reason"]]
    out["Compat_fc_vtx_5v"]  = _rail("Compat_fc_vtx_5v",  "fc_bec_5v_a",  "vtx_5v_current_a", 5)
    out["Compat_fc_vtx_9v"]  = _rail("Compat_fc_vtx_9v",  "fc_bec_9v_a",  "vtx_9v_current_a", 9)
    out["Compat_fc_vtx_10v"] = _rail("Compat_fc_vtx_10v", "fc_bec_10v_a", "vtx_10v_current_a", 10)
    out["Compat_fc_vtx_12v"] = _rail("Compat_fc_vtx_12v", "fc_bec_12v_a", "vtx_12v_current_a", 12)

    # fc ↔ camera (5V current match + 10/12V acceptance)
    # 5V current
    a = fcs.dropna(subset=["fc_bec_5v_a"])[["sku","name","fc_bec_5v_a"]]
    b = cams.dropna(subset=["camera_5v_current_a"])[["sku","name","camera_5v_current_a"]]
    j = _cartesian(a, b)
    if not j.empty:
        j = j.rename(columns={"sku_x":"fc_sku","name_x":"fc_name","sku_y":"camera_sku","name_y":"camera_name"})
        j["status"] = np.where(j["fc_bec_5v_a"] >= j["camera_5v_current_a"], "PASS", "FAIL")
        j["reason"] = np.where(j["status"]=="PASS", "FC 5V BEC ≥ camera current", "FC 5V BEC < camera current")
    out["Compat_fc_cam_5v"] = j[["fc_sku","fc_name","camera_sku","camera_name","fc_bec_5v_a","camera_5v_current_a","status","reason"]] if not j.empty else pd.DataFrame(columns=["fc_sku","fc_name","camera_sku","camera_name","fc_bec_5v_a","camera_5v_current_a","status","reason"])

    # 10/12V acceptance (voltage-only)
    def _cam_accepts(V: int, fc_col: str, key: str) -> pd.DataFrame:
        a = fcs.dropna(subset=[fc_col])[["sku","name",fc_col]]
        b = cams.dropna(subset=["camera_input_v_min","camera_input_v_max"])[["sku","name","camera_input_v_min","camera_input_v_max"]]
        j = _cartesian(a, b)
        if j.empty:
            return pd.DataFrame(columns=["fc_sku","fc_name","camera_sku","camera_name","camera_input_v_min","camera_input_v_max","status","reason"])
        j = j.rename(columns={"sku_x":"fc_sku","name_x":"fc_name","sku_y":"camera_sku","name_y":"camera_name"})
        ok = (j["camera_input_v_min"] <= V) & (j["camera_input_v_max"] >= V)
        j["status"] = np.where(ok, "PASS", "FAIL")
        j["reason"] = np.where(ok, f"Camera accepts {V}V", f"Camera {V}V out of range")
        return j[["fc_sku","fc_name","camera_sku","camera_name","camera_input_v_min","camera_input_v_max","status","reason"]]
    out["Compat_fc_cam_10v"] = _cam_accepts(10, "fc_bec_10v_a", "Compat_fc_cam_10v")
    out["Compat_fc_cam_12v"] = _cam_accepts(12, "fc_bec_12v_a", "Compat_fc_cam_12v")

    # rx ↔ antenna
    rx = rxs.dropna(subset=["rx_ant_conn","rx_band_ghz"])[["sku","name","rx_ant_conn","rx_band_ghz"]].copy()
    an = ants.dropna(subset=["antenna_conn","antenna_band_ghz"])[["sku","name","antenna_conn","antenna_band_ghz"]].copy()
    if not rx.empty and not an.empty:
        rx["conn"] = rx["rx_ant_conn"].apply(_canon_connector)
        an["conn"] = an["antenna_conn"].apply(_canon_connector)
        # coarse band buckets
        def bucket(x):
            if pd.isna(x): return np.nan
            x = float(x)
            if abs(x - 5.8) < 0.4: return "5.8"
            if abs(x - 2.4) < 0.3: return "2.4"
            if 0.7 <= x <= 1.0:   return "0.9"
            return np.nan
        rx["bb"] = rx["rx_band_ghz"].apply(bucket)
        an["bb"] = an["antenna_band_ghz"].apply(bucket)
        j = rx.merge(an, on=["conn","bb"], how="inner", suffixes=("_rx","_ant"))
        j["status"] = "PASS"; j["reason"] = "Connector + band"
        out["Compat_rx_antenna"] = j.rename(columns={"sku_rx":"rx_sku","name_rx":"rx_name","sku_ant":"antenna_sku","name_ant":"antenna_name"})[[
            "rx_sku","rx_name","antenna_sku","antenna_name","conn","bb","status","reason"
        ]]
    else:
        out["Compat_rx_antenna"] = pd.DataFrame(columns=["rx_sku","rx_name","antenna_sku","antenna_name","conn","bb","status","reason"])

    # pigtail ↔ esc / battery (connector)
    pg = pigtail.dropna(subset=["pigtail_connector_eff"])[["sku","name","pigtail_connector_eff"]].copy()
    es = escs.dropna(subset=["esc_batt_connector_eff"])[["sku","name","esc_batt_connector_eff"]].copy()
    bt = bats.dropna(subset=["battery_connector_eff"])[["sku","name","battery_connector_eff"]].copy()
    if not pg.empty and not es.empty:
        j = pg.merge(es, left_on="pigtail_connector_eff", right_on="esc_batt_connector_eff", how="inner")
        j["status"] = "PASS"; j["reason"] = "Connector matched"
        out["Compat_pigtail_esc"] = j.rename(columns={"sku_x":"pigtail_sku","name_x":"pigtail_name","sku_y":"esc_sku","name_y":"esc_name"})[[
            "pigtail_sku","pigtail_name","esc_sku","esc_name","pigtail_connector_eff","status","reason"
        ]]
    else:
        out["Compat_pigtail_esc"] = pd.DataFrame(columns=["pigtail_sku","pigtail_name","esc_sku","esc_name","pigtail_connector_eff","status","reason"])

    if not pg.empty and not bt.empty:
        j = pg.merge(bt, left_on="pigtail_connector_eff", right_on="battery_connector_eff", how="inner")
        j["status"] = "PASS"; j["reason"] = "Connector matched"
        out["Compat_battery_pigtail"] = j.rename(columns={"sku_x":"pigtail_sku","name_x":"pigtail_name","sku_y":"battery_sku","name_y":"battery_name"})[[
            "pigtail_sku","pigtail_name","battery_sku","battery_name","pigtail_connector_eff","status","reason"
        ]]
    else:
        out["Compat_battery_pigtail"] = pd.DataFrame(columns=["pigtail_sku","pigtail_name","battery_sku","battery_name","pigtail_connector_eff","status","reason"])

    # capacitor ↔ esc (cap voltage ≥ ESC VBAT max)
    cap = caps.dropna(subset=["capacitor_voltage_v"])[["sku","name","capacitor_voltage_v"]].copy()
    esv = escs.dropna(subset=["esc_cells_max"])[["sku","name","esc_cells_max"]].copy()
    if not cap.empty and not esv.empty:
        esv["esc_vmax"] = esv["esc_cells_max"] * 4.2
        esv["esc_v_need"] = esv["esc_vmax"] * 1.05
        j = _cartesian(cap, esv).rename(columns={"sku_x":"capacitor_sku","name_x":"capacitor_name","sku_y":"esc_sku","name_y":"esc_name"})
        j["status"] = np.where(j["capacitor_voltage_v"] >= j["esc_v_need"], "PASS", "FAIL")
        j["reason"] = np.where(j["status"]=="PASS", "Cap voltage ≥ VBAT max", "Cap voltage < VBAT max")
        out["Compat_cap_esc"] = j[["capacitor_sku","capacitor_name","esc_sku","esc_name","capacitor_voltage_v","esc_vmax","status","reason"]]
    else:
        out["Compat_cap_esc"] = pd.DataFrame(columns=["capacitor_sku","capacitor_name","esc_sku","esc_name","capacitor_voltage_v","esc_vmax","status","reason"])

    # motor ↔ prop (hub type: M5 vs T-MOUNT)
    mo_h = motors[["sku","name","shaft_mm"]].copy()
    mo_h["motor_hub_eff"] = mo_h["shaft_mm"].apply(_motor_hub_from_shaft)
    pr_h = props[["sku","name","prop_hub_eff"]].dropna(subset=["prop_hub_eff"]).copy()
    x = _cartesian(mo_h, pr_h)
    if not x.empty:
        x = x.rename(columns={"sku_x":"motor_sku","name_x":"motor_name","sku_y":"prop_sku","name_y":"prop_name"})
        x["match"] = (x["motor_hub_eff"].notna()) & (x["prop_hub_eff"].notna()) & (x["motor_hub_eff"] == x["prop_hub_eff"])
        x["status"] = np.where(x["match"], "PASS", np.where(x["motor_hub_eff"].isna() | x["prop_hub_eff"].isna(), "WARN", "FAIL"))
        x["reason"] = np.where(x["match"], "Hub type matches", np.where(x["motor_hub_eff"].isna() | x["prop_hub_eff"].isna(), "Insufficient hub/shaft data", "Hub type mismatch"))
        out["Compat_motor_prop_hub"] = x[["motor_sku","motor_name","prop_sku","prop_name","shaft_mm","motor_hub_eff","prop_hub_eff","status","reason"]]
    else:
        out["Compat_motor_prop_hub"] = pd.DataFrame(columns=["motor_sku","motor_name","prop_sku","prop_name","shaft_mm","motor_hub_eff","prop_hub_eff","status","reason"])

    return out