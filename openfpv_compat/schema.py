# -*- coding: utf-8 -*-
"""
Schema & normalization utilities shared by the engine and CLI.

We keep the surface minimal on purpose: just enough typed fields to compute the
compatibility edges we publish in the open-source repo.

All numeric fields are treated as floats in DataFrames; comparison logic handles
NaN gracefully.
"""
from __future__ import annotations

from typing import Dict, List, Set

# Canonical part types we recognize
CANON_TYPES: List[str] = [
    "frame", "motor", "esc", "battery", "fc", "vtx", "camera", "prop",
    "antenna", "rx", "pigtail", "capacitor",
]

# Fields we look for per type (a superset is OK; extra columns are ignored)
TYPE_FIELDS: Dict[str, List[str]] = {
    "frame":   ["frame_max_prop_in", "frame_fc_mount_pattern_eff", "frame_motor_mount_pattern_eff"],
    "motor":   ["shaft_mm", "max_current_a", "cells_min", "cells_max", "motor_mount_pattern_eff"],
    "esc":     ["esc_continuous_current_a", "esc_cells_min", "esc_cells_max", "esc_batt_connector", "esc_mount_pattern_eff"],
    "battery": ["battery_connector", "cell_count"],
    "fc":      ["fc_mount_pattern_eff", "fc_bec_5v_a", "fc_bec_9v_a", "fc_bec_10v_a", "fc_bec_12v_a", "fc_vbat_min_s", "fc_vbat_max_s"],
    "vtx":     ["vtx_system", "vtx_ant_conn", "vtx_5v_current_a", "vtx_9v_current_a", "vtx_10v_current_a", "vtx_12v_current_a", "vtx_input_s_min", "vtx_input_s_max"],
    "camera":  ["camera_system", "camera_input_v_min", "camera_input_v_max", "camera_5v_current_a", "camera_10v_current_a", "camera_12v_current_a"],
    "prop":    ["prop_diameter_in", "prop_pitch_in", "prop_blade_count", "prop_hub"],
    "antenna": ["antenna_conn", "antenna_band_ghz", "antenna_use"],
    "rx":      ["rx_ant_conn", "rx_band_ghz"],
    "pigtail": ["pigtail_connector", "pigtail_awg", "pigtail_length_mm"],
    "capacitor": ["capacitor_voltage_v", "capacitor_uf"],
}

ALL_FIELDS: Set[str] = set().union(*TYPE_FIELDS.values())

# Mount patterns we allow (normalized)
ALLOWED_MOUNTS = {"9x9", "12x12", "16x16", "19x19", "20x20", "25.5x25.5", "30.5x30.5"}

# Canonical RF connectors
RF_CONNECTORS = ["SMA", "RP-SMA", "MMCX", "MCX", "U.FL", "IPEX MHF4", "IPEX MHF1"]

# PASS tokens used by summary helpers
PASS_VALUES = {"PASS", "OK", "TRUE", "YES", "1"}


def normalize_type(t: str) -> str:
    """Return a canonical, singular type (e.g. 'motors' -> 'motor')."""
    if not isinstance(t, str):
        return ""
    s = t.strip().lower()
    if s in ("motors",): s = "motor"
    if s in ("props", "propellers"): s = "prop"
    if s in ("batteries",): s = "battery"
    return s