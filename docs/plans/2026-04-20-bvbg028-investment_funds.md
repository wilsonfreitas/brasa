# Investment Funds Dataset (b3-bvbg028) Implementation Plan

**Goal:** Add the `investment_funds` dataset (FICInf records) to b3-bvbg028 template and verify it parses and tests correctly.

**Architecture:** Extend the existing YAML template with one new dataset block following the established pattern (9 common header fields + 3 FIC-specific fields), then append a test case to the parametrized test harness.

**Tech Stack:** YAML (template), Python (test case), pytest (6 parametrized test functions already defined).

---

## Task 1: Add investment_funds dataset to YAML template

**Files:**
- Modify: `templates/b3/raw/b3-bvbg028.yaml`

- [ ] **Step 1: Read the current template to understand structure**

Run: `uv run python -c "from brasa.engine import retrieve_template; tpl = retrieve_template('b3-bvbg028'); print(list(tpl.reader.datasets.keys()))"`

Expected output: `['equities', 'options_on_equities', 'future_contracts', 'exercise_of_equities', 'fixed_income', 'options_on_spot_and_futures']`

- [ ] **Step 2: Add investment_funds dataset block to YAML**

Open `templates/b3/raw/b3-bvbg028.yaml` and locate the last dataset block (after `options_on_spot_and_futures`). Append:

```yaml
  investment_funds:
    tag: FICInf
    fields:
      - name: refdate
        tag: DtAsOf
        type: date(format='%Y%m%d')
      - name: security_id
        tag: SctyId
        type: string
      - name: security_proprietary
        tag: SctyIdSrc
        type: string
      - name: security_market
        tag: MktId
        type: string
      - name: instrument_asset
        tag: InstrmInf/InstrmAsst
        type: string
      - name: instrument_asset_description
        tag: InstrmInf/InstrmAsstDsc
        type: string
      - name: instrument_market
        tag: InstrmInf/MktId
        type: string
      - name: instrument_segment
        tag: InstrmInf/SgmtId
        type: string
      - name: instrument_description
        tag: InstrmInf/InstrmDsc
        type: string
      - name: security_category
        tag: InstrmInf/FICInf/SctyCtgy
        type: string
      - name: fund_name
        tag: InstrmInf/FICInf/FndNm
        type: string
      - name: currency
        tag: InstrmInf/FICInf/Ccy
        type: string
```

- [ ] **Step 3: Verify template loads cleanly**

Run: `uv run python -c "from brasa.engine import retrieve_template; tpl = retrieve_template('b3-bvbg028'); print('investment_funds' in tpl.reader.datasets)"`

Expected output: `True`

- [ ] **Step 4: Commit**

```bash
git add templates/b3/raw/b3-bvbg028.yaml
git commit -m "feat(bvbg028): add investment_funds dataset to YAML template"
```

---

## Task 2: Add DatasetCase to test harness

**Files:**
- Modify: `tests/test_bvbg028_datasets.py`

- [ ] **Step 1: Append DatasetCase entry to DATASETS list**

Open `tests/test_bvbg028_datasets.py` and append this entry to the `DATASETS` list:

```python
    DatasetCase(
        dataset="investment_funds",
        xml_tag="FICInf",
        expected_count=2,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000125482",
            "refdate": "2021-04-23",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "FIC",
            "instrument_asset_description": "FUNDOS DE INVESTIMENTOS FIC",
            "instrument_market": "1",
            "instrument_segment": "5",
            "instrument_description": "FILCB BVMFBOVESPA",
            "security_category": "35",
            "fund_name": "FIC BBVMBOVESPA",
            "currency": "BRL",
        },
    ),
```

- [ ] **Step 2: Commit**

```bash
git add tests/test_bvbg028_datasets.py
git commit -m "test(bvbg028): add investment_funds DatasetCase"
```

---

## Task 3: Run targeted tests

**Files:**
- Test: `tests/test_bvbg028_datasets.py`

- [ ] **Step 1: Run investment_funds-specific tests**

Run: `uv run pytest tests/test_bvbg028_datasets.py -k investment_funds -v`

Expected: All 6 parametrized tests pass (dataset_registered, present_in_output, record_count, required_columns_non_null, dtypes, spot_check_row).

---

## Task 4: Run quality gates

**Files:**
- Full suite: all modified files

- [ ] **Step 1: Run full pytest suite**

Run: `uv run pytest`

Expected: All tests pass, no regressions.

- [ ] **Step 2: Run ruff linting and format check**

Run: `uv run ruff check . && uv run ruff format --check .`

Expected: No issues.

- [ ] **Step 3: Run pre-commit hooks**

Run: `uv run pre-commit run --all-files`

Expected: All hooks pass.
