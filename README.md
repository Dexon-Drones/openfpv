# openfpv

**An open, transparent compatibility engine for DIY FPV drone parts.**

- 🧮 Reproducible rules → CSV/JSON/XLSX edges you can inspect
- 🧰 Simple input → one CSV/JSON table of parts
- 🧪 Clear results → each edge has `status` and human-readable `reason`
- 🛠️ Hackable → add more rules and fields without breaking the world

---

## Install

### From source (recommended for now)
```bash
python -m pip install -U pip
pip install -e .[xlsx]     # editable dev install; drop [xlsx] if you don't need Excel output
```

Once this is on PyPI you’ll be able to:

```bash
pip install openfpv-compat
```

> Requires Python 3.9+.

---

## Quickstart

Try the demo dataset and write one CSV per pair into `./edges_out`:

```bash
openfpv-compat --in examples/parts.min.json --out edges_out --print-summary
```

Create a single merged CSV:

```bash
openfpv-compat -i examples/parts.min.json -o edges.csv --merge
```

Create an Excel workbook (one sheet per pair) with stricter ESC↔motor headroom:

```bash
openfpv-compat -i examples/parts.min.json -o edges.xlsx --headroom 1.3
```

---

## Data model (short version)

* Each part is a row with at least:

  * `sku` (string), `type` (e.g. `frame`, `motor`, `esc`, …), `name`
* Extra typed fields per part type are described in **[docs/schema.md](docs/schema.md)**
* JSON can be either:

  * an array of part objects, or
  * an object with `"parts": [...]`
* If a row has an `attrs` object, it will be flattened into top-level columns.

---

## What rules are implemented?

See **[docs/rules.md](docs/rules.md)** for the full list. Highlights:

* Frame↔Prop by diameter
* Frame↔FC & Frame↔Motor by mount patterns
* ESC↔FC by stack/size, Battery↔ESC by connector & cells
* ESC↔Motor basic & strict headroom (configurable)
* VTX↔Camera by system, VTX↔Antenna by connector/band
* FC↔VTX by VBAT S-range overlap & 5/9/10/12V BEC rails
* FC↔Camera by 5V current and 10/12V acceptance
* RX↔Antenna by connector & band
* Pigtail↔ESC/Battery by connector
* Capacitor↔ESC by VBAT max

---

## Contributing

* Add new fields/types in `openfpv_compat/schema.py`
* Implement new joins in `openfpv_compat/engine.py`
* Keep rules documented in `docs/rules.md`
* PRs that add *data sources* or *rules with tests* are very welcome.

---

## License

MIT — see [LICENSE](LICENSE).
