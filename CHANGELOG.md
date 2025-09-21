# Changelog

## 0.1.0 — 2025-09-21
- Initial public release of **openfpv-compat**
- CLI: CSV/JSON in, CSV/JSON/XLSX out (`--merge`, `--pass-only`, `--headroom`, `--print-summary`)
- Rules: frame↔prop, frame↔fc, frame↔motor, esc↔fc, battery↔esc,
  esc↔motor (basic + headroom), vtx↔camera, vtx↔antenna, fc↔vtx (VBAT + rails),
  fc↔camera (5V + 10/12V acceptance), rx↔antenna, pigtail↔esc, battery↔pigtail,
  capacitor↔esc, **motor↔prop hub**.
- Docs: schema + rules, example dataset and demo script.
