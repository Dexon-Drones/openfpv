# Rules (what the engine computes)

Each rule emits a table named `Compat_*` with a `status` and `reason`.  
`status`: `PASS`, `FAIL`, or (in a few rules) `WARN` when data is insufficient.

---

## 1) Frame ↔ Prop (size) — `Compat_frame_prop`
**PASS** if `prop_diameter_in ≤ frame_max_prop_in`.

---

## 2) Frame ↔ FC (mount) — `Compat_frame_fc`
**PASS** if mount patterns intersect.

---

## 3) Frame ↔ Motor (arm mount) — `Compat_frame_motor`
**PASS** if mount patterns intersect.

---

## 4) ESC ↔ FC (stack size) — `Compat_esc_fc`
**PASS** if mount patterns intersect.

---

## 5) Battery ↔ ESC (connector & cells) — `Compat_battery_esc`
Connector match = PASS. If `cell_count` and `esc_cells_min/max` known:
- **PASS** if `esc_cells_min ≤ cell_count ≤ esc_cells_max`
- **FAIL** otherwise.

---

## 6) ESC ↔ Motor (basic) — `Compat_esc_motor`
- **FAIL** if cell ranges do not overlap.
- **PASS** if `esc_continuous_current_a ≥ max_current_a` (and/or cells overlap).
- **WARN** when insufficient data.

### 6b) ESC ↔ Motor (strict headroom) — `Compat_esc_motor_headroom`
**PASS** if `esc_continuous_current_a ≥ HEADROOM × max_current_a` and cell ranges overlap.  
Default `HEADROOM = 1.2` (CLI: `--headroom`).

---

## 7) VTX ↔ Camera (ecosystem) — `Compat_vtx_camera`
**PASS** if `vtx_system == camera_system` (case-insensitive).

---

## 8) VTX ↔ Antenna (RF connector) — `Compat_vtx_antenna`
**PASS** on connector match, limited to FPV antennas:  
`antenna_use == 'fpv'` **or** `5.4 ≤ antenna_band_ghz ≤ 6.2`.

---

## 9) FC ↔ VTX (VBAT S-range) — `Compat_fc_vtx_vbat`
**PASS** if `[fc_vbat_min_s, fc_vbat_max_s]` overlaps `[vtx_input_s_min, vtx_input_s_max]`.

---

## 10) FC ↔ VTX (5/9/10/12V rails) — `Compat_fc_vtx_{5v,9v,10v,12v}`
**PASS** if `fc_bec_{V}v_a ≥ vtx_{V}v_current_a`.

---

## 11) FC ↔ Camera (5V current) — `Compat_fc_cam_5v`
**PASS** if `fc_bec_5v_a ≥ camera_5v_current_a`.

---

## 12) FC ↔ Camera (10/12V acceptance) — `Compat_fc_cam_{10v,12v}`
**PASS** if `camera_input_v_min ≤ V ≤ camera_input_v_max`.

---

## 13) RX ↔ Antenna (radio link) — `Compat_rx_antenna`
**PASS** when RF connector matches and coarse band bucket matches (2.4/5.8/0.9).

---

## 14) Pigtail ↔ ESC/Battery (connector) — `Compat_pigtail_esc`, `Compat_battery_pigtail`
**PASS** on connector match (e.g., XT60). Connectors are normalized (e.g., `XT-60` → `XT60`).

---

## 15) Capacitor ↔ ESC (VBAT rating) — `Compat_cap_esc`
Compute `esc_vmax = esc_cells_max × 4.2`, require 5% margin:  
**PASS** if `capacitor_voltage_v ≥ esc_vmax × 1.05`.

---

## 16) Motor ↔ Prop (hub type) — `Compat_motor_prop_hub`
Infer motor hub from `shaft_mm` (≥4.8 → **M5**, ≤2.1 → **T‑MOUNT**).  
Normalize prop hub text to **M5**/**T‑MOUNT**.  
- **PASS** if hubs match  
- **FAIL** if hubs mismatch  
- **WARN** when hub/shaft info is missing
