# BCB SGS Template Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the legacy `bcb-sgs-data` template with a modern pipeline-based `bcb-sgs` template that supports date ranges via `start`/`end` arguments.

**Architecture:** Update `BCBSGSDownloader` to accept `start`/`end` instead of `refdate`, create a new `bcb-sgs` template with pipeline reader, move the old template to legacy, and refactor the `bcb-data` ETL to read from `input.bcb-sgs` instead of calling python-bcb directly.

**Tech Stack:** python-bcb 0.3.6 (`bcb.sgs`), brasa template/pipeline system, pytest

**Spec:** `docs/superpowers/specs/2026-03-29-bcb-sgs-template-integration-design.md`

---

### File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `brasa/downloaders/downloaders.py:158-170` | Update `BCBSGSDownloader` to use `start`/`end` |
| Create | `templates/bcb/bcb-sgs.yaml` | New pipeline-based SGS template |
| Move   | `templates/bcb/bcb-sgs-data.yaml` → `templates/legacy/bcb-sgs-data.yaml` | Deprecate old template |
| Modify | `templates/bcb/bcb-data.yaml` | Refactor ETL to read from `input.bcb-sgs` |
| Modify | `brasa/etl.py:129-166` | Replace `create_bcb_data` with query-based ETL |
| Modify | `tests/test_templates.py` | Update tests for new template |
| Create | `tests/test_bcb_sgs.py` | Unit and integration tests for new template |

---

### Task 1: Update `BCBSGSDownloader` to accept `start`/`end`

**Files:**
- Modify: `brasa/downloaders/downloaders.py:158-170`
- Test: `tests/test_bcb_sgs.py`

- [ ] **Step 1: Write failing test for the updated downloader**

Create `tests/test_bcb_sgs.py`:

```python
import io
import json
from datetime import date
from unittest.mock import patch

from brasa.downloaders.downloaders import BCBSGSDownloader


def test_bcb_sgs_downloader_with_start_end():
    mock_json = json.dumps([
        {"data": "02/01/2025", "valor": "12,13"},
        {"data": "03/01/2025", "valor": "12,13"},
    ])

    with patch("brasa.downloaders.downloaders.sgs.get_json", return_value=mock_json) as mock_get:
        downloader = BCBSGSDownloader(code=4389, start=date(2025, 1, 2), end=date(2025, 1, 3))
        result = downloader.download()

        mock_get.assert_called_once_with(4389, start=date(2025, 1, 2), end=date(2025, 1, 3))
        assert result is not None
        assert isinstance(result, io.BytesIO)
        data = json.loads(result.read().decode("utf8"))
        assert len(data) == 2
        assert data[0]["data"] == "02/01/2025"


def test_bcb_sgs_downloader_returns_none_on_error():
    with patch("brasa.downloaders.downloaders.sgs.get_json", side_effect=Exception("API error")):
        downloader = BCBSGSDownloader(code=9999, start=date(2025, 1, 1), end=date(2025, 1, 1))
        result = downloader.download()

        assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_bcb_sgs.py -v
```

Expected: FAIL — `BCBSGSDownloader` still expects `refdate` keyword, not `start`/`end`.

- [ ] **Step 3: Update `BCBSGSDownloader`**

In `brasa/downloaders/downloaders.py`, replace lines 158-170:

```python
class BCBSGSDownloader:
    def __init__(self, **kwargs):
        self.args = kwargs

    def download(self) -> IO | None:
        try:
            text = sgs.get_json(
                self.args["code"],
                start=self.args["start"],
                end=self.args["end"],
            )
        except Exception:
            return None
        temp = io.BytesIO(bytes(text, "utf8"))
        return temp
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_bcb_sgs.py -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add brasa/downloaders/downloaders.py tests/test_bcb_sgs.py
git commit -m "feat: update BCBSGSDownloader to accept start/end date range"
```

---

### Task 2: Create `bcb-sgs` template

**Files:**
- Create: `templates/bcb/bcb-sgs.yaml`
- Test: `tests/test_bcb_sgs.py`

- [ ] **Step 1: Write failing test for template loading**

Append to `tests/test_bcb_sgs.py`:

```python
from brasa.engine import MarketDataTemplate, retrieve_template
from brasa.fieldsets import Fieldset


def test_load_bcb_sgs_template():
    tpl = MarketDataTemplate("templates/bcb/bcb-sgs.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
    assert tpl.id == "bcb-sgs"


def test_bcb_sgs_template_fields():
    tpl = MarketDataTemplate("templates/bcb/bcb-sgs.yaml")

    assert isinstance(tpl.fields, Fieldset)
    assert len(tpl.fields) == 3
    assert tpl.fields["refdate"].type_name == "date"
    assert tpl.fields["value"].type_name == "numeric"
    assert tpl.fields["code"].type_name == "integer"


def test_retrieve_bcb_sgs_template():
    tpl = retrieve_template("bcb-sgs")
    assert tpl is not None
    assert isinstance(tpl, MarketDataTemplate)
    assert tpl.id == "bcb-sgs"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_bcb_sgs.py::test_load_bcb_sgs_template tests/test_bcb_sgs.py::test_bcb_sgs_template_fields tests/test_bcb_sgs.py::test_retrieve_bcb_sgs_template -v
```

Expected: FAIL — template file does not exist.

- [ ] **Step 3: Create the template**

Create `templates/bcb/bcb-sgs.yaml`:

```yaml
id: bcb-sgs
description: Séries temporais do SGS (Sistema Gerenciador de Séries) do BCB

downloader:
  function: brasa.downloaders.bcb_sgs_download
  validator: brasa.downloaders.validate_json_empty_file
  format: json
  args:
    code: ~
    start: ~
    end: ~

reader:
  locale: pt
  pipeline:
    - step: read_json
    - step: rename_columns
      columns:
        data: refdate
        valor: value
    - step: add_column
      name: code
      from:
        where: download_args
        key: code
    - step: apply_fields
      errors: coerce

writer:
  layer: input
  partitioning: [refdate, code]

fields:
  - name: refdate
    description: Data de referência
    type: date(format='%d/%m/%Y')
  - name: value
    description: Valor da série
    type: numeric(decimal=',')
  - name: code
    description: Código da série SGS
    type: integer
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_bcb_sgs.py -v
```

Expected: PASS — all 5 tests (2 downloader + 3 template).

- [ ] **Step 5: Commit**

```bash
git add templates/bcb/bcb-sgs.yaml tests/test_bcb_sgs.py
git commit -m "feat: add bcb-sgs template with pipeline reader and date range support"
```

---

### Task 3: Write integration test for full pipeline

**Files:**
- Modify: `tests/test_bcb_sgs.py`

- [ ] **Step 1: Write integration test**

Append to `tests/test_bcb_sgs.py`:

```python
from pathlib import Path

import pytest

from brasa.engine import CacheManager, download_marketdata, process_marketdata


@pytest.mark.integration
def test_bcb_sgs_download_and_process():
    """Integration test: download SGS series 4389 (CDI) for a small date range."""
    download_marketdata("bcb-sgs", code=4389, start=date(2025, 1, 2), end=date(2025, 1, 10))
    process_marketdata("bcb-sgs")

    man = CacheManager()
    ds_path = Path(man.db_path("input/bcb-sgs"))
    assert ds_path.exists(), f"Expected dataset at {ds_path}"


@pytest.mark.integration
def test_bcb_sgs_download_multiple_codes():
    """Integration test: download multiple SGS codes via KwargsIterator."""
    download_marketdata("bcb-sgs", code=[4389, 1178], start=date(2025, 1, 2), end=date(2025, 1, 3))
    process_marketdata("bcb-sgs")

    man = CacheManager()
    ds_path = Path(man.db_path("input/bcb-sgs"))
    assert ds_path.exists(), f"Expected dataset at {ds_path}"
```

- [ ] **Step 2: Run integration test to verify it passes**

```bash
uv run pytest tests/test_bcb_sgs.py::test_bcb_sgs_download_and_process -v
```

Expected: PASS — data downloaded, parsed, and written to parquet.

- [ ] **Step 3: Commit**

```bash
git add tests/test_bcb_sgs.py
git commit -m "test: add integration tests for bcb-sgs template"
```

---

### Task 4: Move `bcb-sgs-data` to legacy

**Files:**
- Move: `templates/bcb/bcb-sgs-data.yaml` → `templates/legacy/bcb-sgs-data.yaml`
- Modify: `tests/test_templates.py`
- Modify: `tests/test_bcb_sgs.py`

- [ ] **Step 1: Move the template file**

```bash
mv templates/bcb/bcb-sgs-data.yaml templates/legacy/bcb-sgs-data.yaml
```

- [ ] **Step 2: Update test references in `tests/test_templates.py`**

Replace all references to `bcb-sgs-data` with the new legacy path or the new `bcb-sgs` template. The tests that load the template by path need updating:

In `tests/test_templates.py`, update `test_load_template` (line 18-22):

```python
def test_load_template():
    tpl = MarketDataTemplate("templates/legacy/bcb-sgs-data.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
```

Update `test_template_load_fields` (line 25-38):

```python
def test_template_load_fields():
    tpl = MarketDataTemplate("templates/legacy/bcb-sgs-data.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
    # Template.fields is now a Fieldset
    assert isinstance(tpl.fields, Fieldset)
    assert len(tpl.fields) == 3
    assert tpl.fields["refdate"].name == "refdate"
    assert tpl.fields["refdate"].description == "Data de referência"
    # Field now has type_name instead of handler
    assert tpl.fields["refdate"].type_name == "date"
    assert tpl.fields["value"].type_name == "numeric"
    assert tpl.fields["code"].type_name == "integer"
```

Update `test_retrieve_temlate` (line 41-45):

```python
def test_retrieve_temlate():
    tpl = retrieve_template("bcb-sgs-data")
    assert tpl is not None
    assert isinstance(tpl, MarketDataTemplate)
    assert tpl.id == "bcb-sgs-data"
```

Note: `retrieve_template` scans all template directories including `templates/legacy/`, so this test should still pass without path changes.

Update `test_save_empty_metadata` (line 58-66) — replace `bcb-sgs-data` with `bcb-sgs`:

```python
def test_save_empty_metadata():
    meta = CacheMetadata("bcb-sgs")
    assert meta.id is not None

    man = CacheManager()
    assert not man.has_meta(meta)
    man.save_meta(meta)
    assert man.has_meta(meta)

    man.remove_meta(meta)
```

The remaining tests referencing `bcb-sgs-data` (lines 70-95) are already `@pytest.mark.skip`, so update their template references to `bcb-sgs` as well but keep them skipped — they test the old single-refdate pattern.

- [ ] **Step 3: Run all tests to verify nothing is broken**

```bash
uv run pytest tests/test_templates.py tests/test_bcb_sgs.py -v
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add templates/bcb/bcb-sgs-data.yaml templates/legacy/bcb-sgs-data.yaml tests/test_templates.py
git commit -m "refactor: move bcb-sgs-data template to legacy, update test references"
```

---

### Task 5: Refactor `bcb-data` ETL to read from `input.bcb-sgs`

**Files:**
- Modify: `brasa/etl.py:129-166`
- Modify: `templates/bcb/bcb-data.yaml`
- Test: `tests/test_bcb_sgs.py`

- [ ] **Step 1: Write failing test for the refactored ETL**

Append to `tests/test_bcb_sgs.py`:

```python
from brasa.engine import process_etl


@pytest.mark.integration
def test_bcb_data_etl_reads_from_input():
    """Integration test: bcb-data ETL reads from input.bcb-sgs instead of calling python-bcb directly."""
    # First, download SGS data for the codes used by bcb-data
    codes = [4389, 1178, 432, 433, 189]
    download_marketdata("bcb-sgs", code=codes, start=date(2025, 1, 2), end=date(2025, 1, 10))
    process_marketdata("bcb-sgs")

    # Then run the ETL
    process_etl("bcb-data")

    man = CacheManager()
    ds_path = Path(man.db_path("staging/bcb-data"))
    assert ds_path.exists(), f"Expected dataset at {ds_path}"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_bcb_sgs.py::test_bcb_data_etl_reads_from_input -v
```

Expected: FAIL — current `create_bcb_data` calls `sgs.get()` directly, not reading from `input.bcb-sgs`.

- [ ] **Step 3: Rewrite `create_bcb_data` in `brasa/etl.py`**

Replace `create_bcb_data` (lines 129-166) with a version that reads from `input.bcb-sgs` parquet instead of calling `sgs.get()` directly:

```python
def create_bcb_data(handler: MarketDataETL):
    code_to_symbol = {
        4389: "CDI",
        1178: "SELIC",
        432: "SETA",
        433: "IPCA",
        189: "IGPM",
    }

    ds = get_dataset("bcb-sgs", layer="input")
    df = ds.to_table().to_pandas()
    df = df[df["code"].isin(code_to_symbol.keys())].copy()
    df["symbol"] = df["code"].map(code_to_symbol)
    df = df[["refdate", "value", "symbol"]]

    fields = [
        pyarrow.field("refdate", pyarrow.timestamp("us")),
        pyarrow.field("value", pyarrow.float64()),
        pyarrow.field("symbol", pyarrow.string()),
    ]
    my_schema = pyarrow.schema(fields)

    write_dataset(df, handler.template_id, schema=my_schema)
```

Note: `get_dataset` and `write_dataset` are already imported in `etl.py` (line 14).

- [ ] **Step 4: Remove `sgs` import from `etl.py`**

In `brasa/etl.py` line 9, change:

```python
from bcb import PTAX, sgs
```

to:

```python
from bcb import PTAX
```

`sgs` was only used by `create_bcb_data`. `PTAX` is still needed by `create_bcb_currency_data`.

- [ ] **Step 5: Run test to verify it passes**

```bash
uv run pytest tests/test_bcb_sgs.py::test_bcb_data_etl_reads_from_input -v
```

Expected: PASS

- [ ] **Step 6: Run full test suite**

```bash
uv run pytest -v
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add brasa/etl.py templates/bcb/bcb-data.yaml tests/test_bcb_sgs.py
git commit -m "refactor: bcb-data ETL reads from input.bcb-sgs instead of calling python-bcb"
```

---

### Task 6: Final validation

**Files:** None (validation only)

- [ ] **Step 1: Run full test suite**

```bash
uv run pytest -v
```

Expected: All tests PASS.

- [ ] **Step 2: Run linting and formatting**

```bash
uv run ruff check . && uv run ruff format --check .
```

Expected: No issues.

- [ ] **Step 3: Run pre-commit hooks**

```bash
uv run pre-commit run --all-files
```

Expected: All hooks pass.

- [ ] **Step 4: Final commit (if any formatting changes)**

```bash
git add -u
git commit -m "style: formatting fixes"
```
