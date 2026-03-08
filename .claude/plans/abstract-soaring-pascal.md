# Replace `processed_files` dict with `is_processed` bool in CacheMetadata

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the `processed_files: dict[str, str]` attribute in `CacheMetadata` with a simple `is_processed: bool` flag that correctly indicates whether a download was processed, regardless of whether the output is a single parquet file or a Hive-partitioned dataset.

**Architecture:** The current dict stores output-name→path mappings, which breaks for partitioned datasets (where many metadata entries share the same folder) and creates false positives in `check_missing_db()`. Replacing with a bool preserves all semantics: "has this entry been processed?" without the fragile path tracking. SQLite's existing `processed_files` column is reused (stores `'true'`/`'false'` as JSON), with a migration shim for existing records.

**Tech Stack:** Python, SQLite (via sqlite3), PyArrow/Parquet, pytest

---

## Context

`CacheMetadata.processed_files` is a `dict[str, str]` mapping output names (e.g., `"data"`) to file paths. For **single parquet files**, this is accurate. For **partitioned datasets**, it stores the shared root folder (e.g., `db/input/b3-cotahist/`), which is shared by thousands of metadata entries. As a result:

- `check_missing_db()` in the new `doctor.py` checks if those paths exist — but the folder exists because *other* entries wrote to it, not necessarily this one.
- `clean_meta_db_folder()` in `cache.py` calls `is_file()` on what is actually a folder, so it silently does nothing for partitioned datasets.
- The `processed_files` dict is fundamentally not usable as a "this specific entry's output exists" indicator for partitioned data.

**Affected files:**
- `brasa/engine/cache.py` (CacheMetadata class + CacheManager methods)
- `brasa/engine/processing.py` (callers of `add_processed_file`)
- `brasa/engine/dependency_graph.py` (SQL query for unprocessed downloads)
- `brasa/engine/api.py` (8 references)
- `brasa/engine/reporting.py` (TaskResult dataclass + formatting)
- `brasa/engine/doctor.py` (NEW file — `_iter_all_metadata_rows` + `check_missing_db`)
- `tests/test_doctor.py` (NEW file — fixtures using `processed_files` dicts)

---

## Task 1: Update `CacheMetadata` in `cache.py`

**Files:**
- Modify: `brasa/engine/cache.py` (lines 86–182)

**Changes:**

Replace the `_processed_files: dict[str, str]` field with `_is_processed: bool = False`.

```python
# In __init__:
self._is_processed: bool = False
# Remove: self._processed_files: dict[str, str] = {}
```

Replace the `processed_files` property and its setter with `is_processed`:

```python
@property
def is_processed(self) -> bool:
    """Whether this download has been successfully processed."""
    return self._is_processed

@is_processed.setter
def is_processed(self, value: bool) -> None:
    self._is_processed = value
```

Replace `add_processed_file` and `remove_processed_file` with:

```python
def mark_as_processed(self) -> None:
    """Mark this download as successfully processed."""
    self._is_processed = True

def mark_as_unprocessed(self) -> None:
    """Mark this download as not yet processed."""
    self._is_processed = False
```

Update `to_dict()` — keep the `"processed_files"` key (same SQLite column), store as JSON bool:

```python
def to_dict(self) -> dict:
    return {
        ...
        "processed_files": self._is_processed,  # stored as JSON true/false
        ...
    }
```

Update `from_dict()` with a migration shim to handle existing records:

```python
def from_dict(self, kwargs) -> None:
    for k, v in kwargs.items():
        if k == "processed_files":
            # Migration: old format was a dict; new format is a bool
            if isinstance(v, dict):
                self._is_processed = bool(v)  # non-empty dict → True
            elif isinstance(v, bool):
                self._is_processed = v
            else:
                self._is_processed = bool(v)  # handles int 0/1, str "0"/"1"
        else:
            setattr(self, k, v)
```

**Step 1:** Make the edits to `brasa/engine/cache.py` as described above.

**Step 2:** Run the tests (they will fail in multiple places — that's expected):
```bash
uv run pytest tests/test_doctor.py -x --no-integration 2>&1 | head -40
```

---

## Task 2: Add SQL migration for existing `processed_files` records

**Files:**
- Modify: `brasa/engine/cache.py` (`CacheManager.init()` and new `_migrate_processed_files()`)

**Problem:** Existing SQLite rows have `processed_files` stored as JSON dicts (e.g., `'{}'`, `'{"data": "db/..."}'`). The Python `from_dict()` shim handles reading them correctly, but old rows in the DB are never rewritten. An explicit idempotent SQL migration is needed — following the same pattern as `_migrate_download_trials()`.

**Changes:**

Add `_migrate_processed_files()` (idempotent — safe to run multiple times):

```python
def _migrate_processed_files(self) -> None:
    """Migrate processed_files column from dict JSON to bool JSON (idempotent).

    Old format: '{}' (unprocessed) or '{"data": "path/..."}' (processed)
    New format: 'false' or 'true'
    """
    db_conn = sqlite3.connect(database=self.cache_path(self.meta_db_filename))
    c = db_conn.cursor()

    # Already-migrated rows have 'true' or 'false' — skip them.
    # Convert empty/null dict → 'false'
    c.execute(
        "UPDATE cache_metadata SET processed_files = 'false' "
        "WHERE processed_files IN ('{}', '', 'null') OR processed_files IS NULL"
    )
    # Convert any remaining JSON object (old dict format) → 'true'
    c.execute(
        "UPDATE cache_metadata SET processed_files = 'true' "
        "WHERE processed_files NOT IN ('{}', '', 'null', 'false', 'true') "
        "  AND processed_files IS NOT NULL"
    )

    db_conn.commit()
    db_conn.close()
```

Wire it into `init()` alongside `_migrate_download_trials()`:

```python
def init(self) -> None:
    ...
    if not Path(self.cache_path(self.meta_db_filename)).exists():
        self.create_meta_db()
    else:
        self._migrate_download_trials()
        self._migrate_processed_files()   # ← add this line
    ...
```

**Step 1:** Apply edits to `brasa/engine/cache.py`.

**Step 2:** Verify the migration is idempotent by running:
```bash
uv run pytest tests/ -x --no-integration -q 2>&1 | head -20
```

---

## Task 3: Update remaining `CacheManager` methods in `cache.py`

**Files:**
- Modify: `brasa/engine/cache.py` (CacheManager class)

**Changes:**

**`save_meta()` serialization** (around line 446): The `json.dumps(meta.processed_files, ...)` call now serializes a bool. JSON dumps handles bool natively (`True` → `'true'`, `False` → `'false'`). Just remove the `default=json_convert_from_object` if it was only needed for dict serialization. Verify the call still works:
```python
json.dumps(meta.is_processed)  # → 'true' or 'false'
```

**`clean_meta_db_folder()`** (line 486): The old code iterated over `processed_files.values()` to delete individual files. For partitioned data this was already broken (folder paths, not files). Update to a no-op comment explaining why:
```python
def clean_meta_db_folder(self, meta: CacheMetadata) -> None:
    """Clean the database folder for a cache entry.

    Note: With partitioned datasets, output paths are shared across
    entries and cannot be cleaned per-entry. Use check_orphan_db()
    in doctor to find orphaned db folders instead.
    """
    pass
```

**`load_marketdata()`** (line 641): The path-based loading via `processed_files` is gone. Update to use `is_processed` to guard the warning:
```python
def load_marketdata(
    self, meta: CacheMetadata, reprocess: bool = False
) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
    """Load processed market data from cache."""
    if reprocess:
        self.read_marketdata(meta)
    if not meta.is_processed:
        warn("No processed files", stacklevel=2)
        return None
    warn(
        "load_marketdata() cannot load by path for partitioned datasets. "
        "Use get_marketdata() instead.",
        stacklevel=2,
    )
    return None
```

**`get_templates_with_unprocessed_downloads()` SQL** (line 829): Add `'false'` to the unprocessed set:
```python
"  AND (processed_files IN ('{}', '', 'null', 'false') "
"       OR processed_files IS NULL) "
```

**Step 1:** Apply edits to `brasa/engine/cache.py` as described.

**Step 2:** Run:
```bash
uv run pytest tests/ -x --no-integration -q 2>&1 | head -40
```

---

## Task 4: Update `processing.py`

**Files:**
- Modify: `brasa/engine/processing.py` (lines 54–127)

**Changes:**

The functions `save_parquet_file` and `save_partitioned_parquet_file` both accept a `processed_files_name: str` parameter and call `meta.add_processed_file(processed_files_name, fname/folder)`. Replace these with `meta.mark_as_processed()`.

Remove `processed_files_name` from both function signatures (it's no longer needed):

```python
def save_parquet_file(
    meta: CacheMetadata, folder: str, df: pd.DataFrame
) -> None:
    man = CacheManager()
    fname_part = get_fname_part(meta, df)
    fname = str(Path(folder) / man.parquet_file_name(fname_part))
    df.to_parquet(man.cache_path(fname))
    meta.mark_as_processed()


def save_partitioned_parquet_file(
    meta: CacheMetadata,
    folder: str,
    df: pd.DataFrame,
    partition_cols: list[str],
    schema: pa.Schema = None,
    layer: str | None = None,
    dataset_name: str | None = None,
    source_template: str | None = None,
) -> None:
    ...
    meta.mark_as_processed()
```

Search for all callers of `save_parquet_file` and `save_partitioned_parquet_file` in `brasa/engine/` and update them to remove the `processed_files_name` argument (typically `"data"` or output name). These calls are in the pipeline processing code (likely `brasa/engine/processing.py` further down or `brasa/engine/pipelines/`).

**Step 1:** Apply edits to `brasa/engine/processing.py`.

**Step 2:** Find all callers:
```bash
uv run grep -rn "save_parquet_file\|save_partitioned_parquet_file" brasa/ --include="*.py"
```

**Step 3:** Update each caller to drop the `processed_files_name` argument.

**Step 4:** Run:
```bash
uv run pytest tests/ -x --no-integration -q 2>&1 | head -40
```

---

## Task 5: Update `api.py`

**Files:**
- Modify: `brasa/engine/api.py`

**Changes:**

Find each of the 8 usages of `processed_files` and update:

| Old | New |
|-----|-----|
| `meta.processed_files = {}` | `meta.is_processed = False` |
| `len(meta.processed_files) == 0` | `not meta.is_processed` |
| `processed_files=meta.processed_files` | `is_processed=meta.is_processed` |
| `reprocess or len(meta.processed_files) == 0` | `reprocess or not meta.is_processed` |

**Step 1:** Apply all edits to `brasa/engine/api.py`.

**Step 2:** Run:
```bash
uv run pytest tests/ -x --no-integration -q 2>&1 | head -40
```

---

## Task 6: Update `reporting.py`

**Files:**
- Modify: `brasa/engine/reporting.py`

**Changes:**

In the `TaskResult` dataclass (around line 241), replace:
```python
processed_files: dict[str, str] = field(default_factory=dict)
```
with:
```python
is_processed: bool = False
```

Update `to_dict()` (line 258):
```python
"is_processed": self.is_processed,
# Remove: "processed_files": self.processed_files,
```

Update the report formatting (around line 572):
```python
# Old: if result.processed_files: text.append(f"{len(result.processed_files)} files\n")
# New:
if result.is_processed:
    text.append("processed\n")
```

Update all factory functions (`create_task_result_success`, `create_task_result_from_exception`, `create_task_result_skipped`) — change parameter from `processed_files: dict[str, str] | None = None` to `is_processed: bool = False`, and update the body accordingly.

**Step 1:** Apply edits to `brasa/engine/reporting.py`.

**Step 2:** Run:
```bash
uv run pytest tests/ -x --no-integration -q 2>&1 | head -40
```

---

## Task 7: Update `doctor.py`

**Files:**
- Modify: `brasa/engine/doctor.py` (NEW untracked file)

**Changes:**

**`_iter_all_metadata_rows()`** (line 113): The `processed_files` column now holds `'true'`/`'false'` (or old dict JSON for migration). Parse it as bool:

```python
# Old:
"processed_files": json.loads(row[4] or "{}"),
# New:
"is_processed": _parse_is_processed(row[4]),
```

Add helper at module level:
```python
def _parse_is_processed(raw: str | None) -> bool:
    """Parse the processed_files column as a boolean (handles migration)."""
    if not raw:
        return False
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return False
    if isinstance(parsed, bool):
        return parsed
    if isinstance(parsed, dict):
        return bool(parsed)  # migration: non-empty dict → True
    return bool(parsed)
```

**`check_missing_db()`** (line 358): Rewrite to detect metadata entries marked as processed (`is_processed = True`) but whose template's db folder is missing or empty, instead of checking stored paths:

```python
def check_missing_db() -> list[Issue]:
    """Find templates with processed downloads but missing db folders.

    Returns:
        List of issues found.
    """
    from .cache import CacheManager
    from .catalog import DatasetCatalog

    man = CacheManager()
    catalog = DatasetCatalog()
    rows = _iter_all_metadata_rows()

    # Find templates that have at least one processed entry
    processed_templates = {
        row["template"]
        for row in rows
        if row["is_processed"] and not row["is_invalid_download"]
    }

    missing: list[str] = []
    for template_id in processed_templates:
        # Check if any parquet files exist for this template
        entry = catalog.get(template_id)
        if entry is None:
            # Not in catalog — check db folder directly
            db_folder = Path(man.db_path(""))
            # Try to find the folder by template name convention
            found = any(db_folder.rglob(f"{template_id}/*.parquet")) if db_folder.exists() else False
            if not found:
                missing.append(template_id)
        # If in catalog, the folder is tracked and checked by check_orphan_db/check_empty_parquet

    if not missing:
        return []

    return [
        Issue(
            category="DB / Parquet",
            code="missing-db",
            severity="error",
            description=(
                f"{len(missing)} template(s) have processed downloads "
                "but no parquet files found (re-process required)"
            ),
            details=missing,
            fixable=False,
        )
    ]
```

> **Note:** Review the actual `DatasetCatalog` API before implementing — adjust to use the correct methods for checking if a dataset has data. The key logic is: "metadata says processed=True but no parquet files exist on disk."

**Step 1:** Apply edits to `brasa/engine/doctor.py`.

**Step 2:** Run:
```bash
uv run pytest tests/test_doctor.py -x --no-integration -v 2>&1 | head -60
```

---

## Task 8: Update `tests/test_doctor.py`

**Files:**
- Modify: `tests/test_doctor.py` (NEW untracked file)

**Changes:**

Find all test fixtures that create or set `processed_files` on `CacheMetadata` or in database rows. Replace with `is_processed = True` / `is_processed = False` or `meta.mark_as_processed()`.

Common patterns to update:
```python
# Old pattern:
meta.processed_files = {"data": "db/input/template/20240101.parquet"}
# New pattern:
meta.mark_as_processed()

# Old pattern in SQL fixtures:
INSERT INTO cache_metadata (..., processed_files, ...) VALUES (..., '{"data": "some/path"}', ...)
# New pattern:
INSERT INTO cache_metadata (..., processed_files, ...) VALUES (..., 'true', ...)

# Old pattern checking for processed:
assert len(row["processed_files"]) > 0
# New pattern:
assert row["is_processed"] is True
```

**Step 1:** Read `tests/test_doctor.py` fully, identify all `processed_files` references.

**Step 2:** Apply edits.

**Step 3:** Run the full test suite:
```bash
uv run pytest tests/test_doctor.py --no-integration -v
```

---

## Task 9: Verify and fix remaining test failures

**Step 1:** Run the full test suite:
```bash
uv run pytest --no-integration -q
```

**Step 2:** Fix any remaining failures. Common sources:
- Tests that check `meta.processed_files` — update to `meta.is_processed`
- Tests that call `save_parquet_file` or `save_partitioned_parquet_file` with the old signature (with `processed_files_name` argument)
- Tests for `reporting` that check `result.processed_files`

**Step 3:** Run ruff:
```bash
uv run ruff check . && uv run ruff format --check .
```
Fix any issues with `uv run ruff check . --fix && uv run ruff format .`

**Step 4:** Run pre-commit:
```bash
uv run pre-commit run --all-files
```

---

## Verification

End-to-end check — confirm the change is correct:

```bash
# 1. All tests pass
uv run pytest --no-integration -q

# 2. Ruff clean
uv run ruff check . && uv run ruff format --check .

# 3. Pre-commit clean
uv run pre-commit run --all-files

# 4. Smoke-check doctor command (requires cache data)
# uv run python -m brasa doctor --category db --category meta
```

For the doctor `check_missing_db`, manually verify:
1. With a clean cache (no entries): no issues reported.
2. With a metadata row where `is_processed = True` and the template folder exists: no issue.
3. With a metadata row where `is_processed = True` and the template folder is missing: error reported.
4. With `is_processed = False`: not flagged (not processed yet is not an error).
