# Plan: Reorganize templates/ into thematic subdirectories

## Context

The `templates/` directory has grown to 101 active YAML files in a flat structure, making it hard to navigate. The goal is to group them into a 2-level hierarchy (source → theme within source) while keeping the public API (`retrieve_template("b3-cotahist")`) unchanged. The loader will be updated to search recursively by filename stem.

---

## Target Directory Structure

```
templates/
  b3/
    equities/      ~25 files — cotahist-*, equities-*, listed-*, etfs-*, equity-options, cash-dividends, trades-intraday, otc-trade-*
    futures/       ~23 files — futures-*
    indexes/       ~10 files — indexes-*
    companies/     ~12 files — companies-*, company-*
    curves/        ~6 files  — curves-di1*, curves-dap*, CenariosCurva
    lending/       ~3 files  — lending-*, loan-balance
    raw/           ~18 files — bvbg*, BDIN, FPR, ISIND, ISINS, PUWEB, SupVol, CenariosSpot, economic-indicators-*
  anbima/          ~1 file   — anbima-*
  bcb/             ~4 files  — bcb-*
  cvm/             ~1 file   — cvm-*
  brasa/           ~6 files  — brasa-* (integrated ETL datasets)
  legacy/          9 files   — no change (already exists)
```

**Grouping rules for ambiguous files:**
- Capitalized raw B3 file names (BDIN, FPR, PUWEB, ISIND, ISINS, SupVol, CenariosSpot) → `b3/raw/`
- `PremioOpcaoAcao.yaml` (option premium on equities) → `b3/equities/`
- `CenariosCurva.yaml` (DI1/DAP curve scenarios) → `b3/curves/`
- `b3-bvbg028.yaml`, `b3-bvbg086.yaml`, `b3-bvbg087.yaml` → `b3/raw/`
- `b3-economic-indicators-*.yaml` → `b3/raw/` (raw B3 indicator files)

---

## Changes Required

### 1. Move template files (no content changes)

Move each file to its new subdirectory. Template IDs inside the YAML files stay the same — no edits to YAML content needed.

### 2. Update loader: `brasa/engine/template.py`

**`list_templates()` (line 682)** — change from shallow `iterdir()` to recursive glob:

```python
# Before
return sorted(f.stem for f in templates_dir.iterdir() if f.suffix == ".yaml")

# After
return sorted(f.stem for f in templates_dir.rglob("*.yaml"))
```

**`retrieve_template()` (line 732)** — change from direct path construction to recursive search:

```python
# Before
template_path = templates_dir / f"{template_name}.yaml"
if not template_path.exists():
    ...

# After
matches = list(templates_dir.rglob(f"{template_name}.yaml"))
if not matches:
    available = list_templates()
    ...raise ValueError...
template_path = matches[0]
```

---

## Files to Modify

| File | Change |
|------|--------|
| `brasa/engine/template.py` | Update `list_templates()` and `retrieve_template()` — ~5 lines |
| `templates/*.yaml` (101 files) | Move to subdirectories — no content changes |

---

## What Does NOT Change

- Template IDs inside YAML files
- All public API call sites (`retrieve_template("b3-cotahist")`)
- `legacy/` subdirectory (unchanged)
- `pyproject.toml` — `packages = ["brasa", "templates"]` already covers subdirs

---

## Verification

1. `uv run python -c "from brasa.engine.template import list_templates; print(len(list_templates()))"` → should print 101
2. `uv run python -c "from brasa.engine.template import retrieve_template; retrieve_template('b3-cotahist')"` → no error
3. `uv run pytest` — all tests pass
4. `uv run ruff check . && uv run ruff format --check .`
5. `uv run pre-commit run --all-files`
