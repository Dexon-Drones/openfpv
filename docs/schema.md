# Schema

This project ingests a single **parts table** (CSV or JSON) and emits compatibility edges.

## Required columns (all parts)

| Column | Type | Notes |
|---|---|---|
| `sku` | string | Unique ID per part. Any stable string is fine. |
| `type` | string | Canonical singular: `frame`, `motor`, `esc`, `battery`, `fc`, `vtx`, `camera`, `prop`, `antenna`, `rx`, `pigtail`, `capacitor`. Plurals like `motors` are normalized. |
| `name` | string | Human-friendly name. |

### Accepted JSON shapes

- **Array** of part objects: `[ { "sku": "...", "type": "...", ...}, ... ]`
- **Object** with a `parts` array: `{ "parts": [ ... ] }`

If a row has an `attrs` object (nested), it will be flattened into top-level columns.

## Typed fields per `type`

Only add the fields you have; missing values are allowed. The engine gracefully handles `NaN`.

### Frame
- `frame_max_prop_in` *(float)*
- `frame_fc_mount_pattern_eff` *(list or comma string; normalized)*
- `frame_motor_mount_pattern_eff` *(list or comma string; normalized)*

### Motor
- `motor_mount_pattern_eff` *(list or comma string; normalized)*
- `shaft_mm` *(float)*
- `max_current_a` *(float)*
- `cells_min`, `cells_max` *(float)*

### ESC
- `esc_mount_pattern_eff` *(list or comma string; normalized)*
- `esc_continuous_current_a` *(float)*
- `esc_cells_min`, `esc_cells_max` *(float)*
- `esc_batt_connector` *(string; e.g. XT30, XT60)*

### Battery
- `battery_connector` *(string)*
- `cell_count` *(float/int)*

### FC
- `fc_mount_pattern_eff` *(list or comma string; normalized)*
- `fc_bec_5v_a`, `fc_bec_9v_a`, `fc_bec_10v_a`, `fc_bec_12v_a` *(float)*  
- `fc_vbat_min_s`, `fc_vbat_max_s` *(float)*

### VTX
- `vtx_system` *(string; e.g. `analog`, `hdzero`, `walksnail`, `dji`)*  
- `vtx_ant_conn` *(string; e.g. MMCX, U.FL, RP‑SMA)*  
- `vtx_5v_current_a`, `vtx_9v_current_a`, `vtx_10v_current_a`, `vtx_12v_current_a` *(float)*  
- `vtx_input_s_min`, `vtx_input_s_max` *(float)*

### Camera
- `camera_system` *(string; must match `vtx_system` for compat)*  
- `camera_input_v_min`, `camera_input_v_max` *(float)*  
- `camera_5v_current_a`, `camera_10v_current_a`, `camera_12v_current_a` *(float)*

### Prop
- `prop_diameter_in`, `prop_pitch_in` *(float)*
- `prop_blade_count` *(float)*
- `prop_hub` *(string; e.g. M5, T‑mount)

### Antenna
- `antenna_conn` *(string; e.g. MMCX, U.FL, RP‑SMA)*
- `antenna_band_ghz` *(float; e.g. 5.8, 2.4, 0.9)*
- `antenna_use` *(string; `fpv` recommended for video)*

### RX
- `rx_ant_conn` *(string)*
- `rx_band_ghz` *(float; e.g. 2.4, 0.9)*

### Pigtail
- `pigtail_connector` *(string; e.g. XT60)*
- `pigtail_awg` *(float)*, `pigtail_length_mm` *(float)*

### Capacitor
- `capacitor_voltage_v` *(float)*, `capacitor_uf` *(float)*

## Normalization

- **Mount patterns** are normalized to one of:  
  `9x9, 12x12, 16x16, 19x19, 20x20, 25.5x25.5, 30.5x30.5`  
  Aliases like `30x30` → `30.5x30.5`, `25x25` → `25.5x25.5`.
- **RF connectors** are canonicalized where rules need them:  
  `SMA`, `RP-SMA`, `MMCX`, `MCX`, `U.FL`, `IPEX MHF4`, `IPEX MHF1`.

If a field is missing, the corresponding rule either becomes `WARN/UNKNOWN` or that pair is skipped.
