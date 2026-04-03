# WIL-34: DownloadArgs Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix a `sqlite3.IntegrityError: UNIQUE constraint failed` bug caused by `download_args` containing `"YYYY-MM-DD"` date strings that hash differently after JSON roundtrip than the `"YYYY-MM-DDTHH:MM:SS"` form stored by other templates.

**Architecture:** Introduce a `DownloadArgs` class in `brasa/util.py` that normalizes all date/datetime values to `"YYYY-MM-DDTHH:MM:SS"` strings at construction time. Replace all `dict` usage for `download_args` in `CacheMetadata` and the surrounding serialization/deserialization code with this new type.

**Tech Stack:** Python 3.10+, existing brasa codebase (no new dependencies).

---

## File Map

| File | Change |
|------|--------|
| `brasa/util.py` | Add `_normalize`, `_to_object`, `DownloadArgs` class; simplify `generate_checksum_for_template` |
| `brasa/engine/cache.py` | Update `CacheMetadata.download_args` type; update `save_meta` serialization; update `_load_meta_dict_by_id` deserialization; update `from_dict` special-case |
| `brasa/engine/download.py` | Wrap `kwargs` in `DownloadArgs` at line 137 |
| `brasa/engine/api.py` | Wrap `kwargs` in `DownloadArgs` at lines 113 and 302 |
| `brasa/engine/processing.py` | Use `.get_object("refdate")` instead of `["refdate"]` for `.strftime()` |
| `brasa/readers/helpers.py` | Use `.get_object("refdate")` at line 447 |
| `tests/test_download_args.py` | New test file for `DownloadArgs` |
| `tests/test_cache.py` | Add roundtrip and hash-stability tests |

---

## Task 1: Add `DownloadArgs` to `brasa/util.py`

**Files:**
- Modify: `brasa/util.py`
- Create: `tests/test_download_args.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_download_args.py`:

```python
"""Tests for DownloadArgs normalization and serialization."""
from datetime import date, datetime

import pytest

from brasa.util import DownloadArgs, generate_checksum_for_template


class TestNormalize:
    def test_datetime_is_formatted_as_iso_string(self):
        args = DownloadArgs({"refdate": datetime(2024, 1, 8)})
        assert args["refdate"] == "2024-01-08T00:00:00"

    def test_date_is_formatted_as_iso_string(self):
        args = DownloadArgs({"refdate": date(2024, 1, 8)})
        assert args["refdate"] == "2024-01-08T00:00:00"

    def test_bare_date_string_is_upcasted(self):
        args = DownloadArgs({"refdate": "2000-01-01"})
        assert args["refdate"] == "2000-01-01T00:00:00"

    def test_full_datetime_string_is_unchanged(self):
        args = DownloadArgs({"refdate": "2024-01-08T00:00:00"})
        assert args["refdate"] == "2024-01-08T00:00:00"

    def test_other_string_is_unchanged(self):
        args = DownloadArgs({"code": "today"})
        assert args["code"] == "today"

    def test_integer_is_unchanged(self):
        args = DownloadArgs({"series_id": 4398})
        assert args["series_id"] == 4398

    def test_empty_dict(self):
        args = DownloadArgs({})
        assert list(args.keys()) == []


class TestGetObject:
    def test_date_string_returns_datetime(self):
        args = DownloadArgs({"refdate": "2024-01-08T00:00:00"})
        obj = args.get_object("refdate")
        assert isinstance(obj, datetime)
        assert obj == datetime(2024, 1, 8)

    def test_non_date_string_returns_as_is(self):
        args = DownloadArgs({"code": "today"})
        assert args.get_object("code") == "today"

    def test_integer_returns_as_is(self):
        args = DownloadArgs({"series_id": 4398})
        assert args.get_object("series_id") == 4398


class TestSerialization:
    def test_to_json_roundtrip(self):
        args = DownloadArgs({"refdate": datetime(2024, 1, 8), "code": "abc"})
        restored = DownloadArgs.from_json(args.to_json())
        assert restored["refdate"] == "2024-01-08T00:00:00"
        assert restored["code"] == "abc"

    def test_from_json_does_not_reconvert_datetime_strings(self):
        args = DownloadArgs({"refdate": "2024-01-08T00:00:00"})
        restored = DownloadArgs.from_json(args.to_json())
        # Must still be a string, NOT a datetime object
        assert isinstance(restored["refdate"], str)
        assert restored["refdate"] == "2024-01-08T00:00:00"

    def test_to_dict(self):
        args = DownloadArgs({"refdate": datetime(2024, 1, 8), "x": 1})
        d = args.to_dict()
        assert d == {"refdate": "2024-01-08T00:00:00", "x": 1}
        assert isinstance(d, dict)


class TestDictInterface:
    def test_contains(self):
        args = DownloadArgs({"a": 1})
        assert "a" in args
        assert "b" not in args

    def test_get_with_default(self):
        args = DownloadArgs({"a": 1})
        assert args.get("a") == 1
        assert args.get("b", 99) == 99

    def test_items(self):
        args = DownloadArgs({"a": 1, "b": 2})
        assert set(args.items()) == {("a", 1), ("b", 2)}

    def test_iter(self):
        args = DownloadArgs({"a": 1, "b": 2})
        assert set(args) == {"a", "b"}


class TestHashStability:
    """The core requirement: same date in any form → same hash."""

    def test_all_date_forms_produce_same_hash(self):
        template = "bcb-sgs"
        h1 = generate_checksum_for_template(template, DownloadArgs({"refdate": "2000-01-01"}))
        h2 = generate_checksum_for_template(template, DownloadArgs({"refdate": "2000-01-01T00:00:00"}))
        h3 = generate_checksum_for_template(template, DownloadArgs({"refdate": date(2000, 1, 1)}))
        h4 = generate_checksum_for_template(template, DownloadArgs({"refdate": datetime(2000, 1, 1)}))
        assert h1 == h2 == h3 == h4
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_download_args.py -v
```

Expected: `ImportError` or `AttributeError` — `DownloadArgs` does not exist yet.

- [ ] **Step 3: Implement `DownloadArgs` in `brasa/util.py`**

Add after the existing imports (keep all existing code unchanged), inserting before `generate_checksum_for_template`:

```python
import json
import re
```

Add `json` and `re` to the existing import block at the top of `util.py` (they are not yet imported).

Then add the following block **before** `generate_checksum_for_template`:

```python
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def _normalize_download_arg(value: Any) -> Any:
    """Normalize a download arg value to its canonical form.

    Canonical form for date-like values is YYYY-MM-DDTHH:MM:SS.
    Other values are returned unchanged.
    """
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day).strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(value, str) and _DATE_RE.match(value):
        return f"{value}T00:00:00"
    return value


def _to_download_arg_object(value: Any) -> Any:
    """Reconstruct a rich Python object from a canonical download arg value."""
    if isinstance(value, str) and _DATETIME_RE.match(value):
        return datetime.fromisoformat(value)
    return value


class DownloadArgs:
    """Canonical, serialization-stable container for download arguments.

    Values are stored in canonical form: date/datetime always as
    "YYYY-MM-DDTHH:MM:SS" strings; other primitives unchanged.
    Rich objects are reconstructed on demand via get_object().
    """

    def __init__(self, data: dict[str, Any]) -> None:
        self._data: dict[str, Any] = {k: _normalize_download_arg(v) for k, v in data.items()}

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def items(self):
        return self._data.items()

    def keys(self):
        return self._data.keys()

    def __iter__(self):
        return iter(self._data)

    def get_object(self, key: str) -> Any:
        """Return the value as a rich Python type (datetime for date strings, etc.)."""
        return _to_download_arg_object(self._data[key])

    def to_json(self) -> str:
        """Serialize to JSON — always stable, no custom encoder needed."""
        return json.dumps(self._data)

    @classmethod
    def from_json(cls, s: str) -> "DownloadArgs":
        """Deserialize from JSON — plain load, values stay as canonical strings."""
        obj = cls.__new__(cls)
        obj._data = json.loads(s)
        return obj

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dict copy (for **unpacking into downloaders)."""
        return dict(self._data)
```

- [ ] **Step 4: Update `generate_checksum_for_template` to accept `DownloadArgs`**

Replace the existing function:

```python
def generate_checksum_for_template(
    template: str, args: "DownloadArgs", extra_key: str = ""
) -> str:
    """Generates a hash for a template and its arguments.

    The hash is used to identify a template and its arguments.
    Values in args are already canonical — no normalization needed.
    """
    t = tuple(sorted(args.items(), key=lambda x: x[0]))
    obj: Any = (template, t)
    if extra_key:
        obj = (template, t, extra_key)
    return hashlib.md5(pickle.dumps(obj)).hexdigest()
```

Note: Remove the old `normalized = {...}` dict comprehension — `DownloadArgs` handles this.

- [ ] **Step 5: Add `json` and `re` imports to `util.py`**

The file currently imports: `hashlib`, `itertools`, `logging`, `pickle`, `warnings`, `zipfile`, `date`, `datetime`, `Path`, `IO`, `Any`. Add `json` and `re` to the import block.

- [ ] **Step 6: Run tests to confirm they pass**

```bash
uv run pytest tests/test_download_args.py -v
```

Expected: all tests PASS.

- [ ] **Step 7: Run full test suite to check for regressions**

```bash
uv run pytest --no-integration -v
```

Expected: all existing tests PASS (nothing uses `generate_checksum_for_template` with `DownloadArgs` yet — that comes in Task 2, but the old dict-based path no longer exists in the function, so existing callers that pass plain dicts will break. Verify this and proceed to Task 2 immediately).

- [ ] **Step 8: Commit**

```bash
git add brasa/util.py tests/test_download_args.py
git commit -m "feat(WIL-34): add DownloadArgs class with canonical date normalization"
```

---

## Task 2: Update `CacheMetadata` and `CacheManager` serialization

**Files:**
- Modify: `brasa/engine/cache.py`
- Modify: `tests/test_cache.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_cache.py`:

```python
from datetime import datetime

from brasa.util import DownloadArgs


class TestDownloadArgsSerialization:
    """Verify save_meta/load_meta roundtrip preserves canonical form."""

    def test_save_and_load_meta_preserves_download_args(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = DownloadArgs({"refdate": "2000-01-01"})
        meta.download_checksum = "abc123"
        temp_cache.save_meta(meta)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = DownloadArgs({"refdate": "2000-01-01"})
        temp_cache.load_meta(meta2)

        assert isinstance(meta2.download_args, DownloadArgs)
        assert meta2.download_args["refdate"] == "2000-01-01T00:00:00"

    def test_same_id_before_and_after_roundtrip(self, temp_cache):
        meta = CacheMetadata("test-template")
        meta.download_args = DownloadArgs({"refdate": "2000-01-01"})
        meta.download_checksum = "abc123"
        original_id = meta.id

        temp_cache.save_meta(meta)

        meta2 = CacheMetadata("test-template")
        meta2.download_args = DownloadArgs({"refdate": "2000-01-01"})
        temp_cache.load_meta(meta2)

        assert meta2.id == original_id

    def test_bare_date_and_datetime_string_produce_same_id(self, temp_cache):
        meta1 = CacheMetadata("test-template")
        meta1.download_args = DownloadArgs({"refdate": "2000-01-01"})

        meta2 = CacheMetadata("test-template")
        meta2.download_args = DownloadArgs({"refdate": "2000-01-01T00:00:00"})

        assert meta1.id == meta2.id

    def test_no_integrity_error_on_process_after_download(self, temp_cache):
        """Reproduce WIL-34: save with bare date, reload, save again — no UNIQUE error."""
        meta = CacheMetadata("bcb-sgs")
        meta.download_args = DownloadArgs({"refdate": "2000-01-01"})
        meta.download_checksum = "deadbeef"
        temp_cache.save_meta(meta)

        # Simulate process time: reload from DB
        meta2 = CacheMetadata("bcb-sgs")
        meta2.download_args = DownloadArgs({"refdate": "2000-01-01"})
        temp_cache.load_meta(meta2)

        # Mark as processed and save again — must NOT raise IntegrityError
        meta2.mark_as_processed()
        temp_cache.save_meta(meta2)  # should not raise

        # Verify it updated (not inserted a duplicate)
        with temp_cache.meta_db_connection as conn:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM cache_metadata WHERE template = 'bcb-sgs'")
            count = c.fetchone()[0]
        assert count == 1
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_cache.py::TestDownloadArgsSerialization -v
```

Expected: errors because `CacheMetadata.download_args` is still typed as `dict` and serialization still uses `json_convert_from_object`.

- [ ] **Step 3: Update `CacheMetadata` in `brasa/engine/cache.py`**

In `CacheMetadata.__init__`, change:

```python
self.download_args: dict[str, Any] = {}
```

to:

```python
self.download_args: DownloadArgs = DownloadArgs({})
```

Add import at top of `cache.py`:

```python
from brasa.util import DownloadArgs, generate_checksum_for_template
```

(The existing import `from brasa.util import generate_checksum_for_template` is already there — extend it.)

- [ ] **Step 4: Update `from_dict` in `CacheMetadata`**

In `CacheMetadata.from_dict`, the loop currently does `setattr(self, k, v)` for all keys. The `download_args` key receives a plain dict from the old code path. We will update the serialization next so it returns a `DownloadArgs`, but add a safety guard:

```python
    def from_dict(self, kwargs) -> None:
        """Load metadata from a dictionary."""
        for k, v in kwargs.items():
            if k == "processed_files":
                if isinstance(v, dict):
                    self._is_processed = bool(v)
                elif isinstance(v, bool):
                    self._is_processed = v
                else:
                    self._is_processed = bool(v)
            elif k == "download_args":
                if isinstance(v, DownloadArgs):
                    self.download_args = v
                elif isinstance(v, dict):
                    self.download_args = DownloadArgs(v)
                else:
                    self.download_args = DownloadArgs({})
            else:
                setattr(self, k, v)
```

- [ ] **Step 5: Update `_load_meta_dict_by_id` to use `DownloadArgs.from_json`**

In `CacheManager._load_meta_dict_by_id`, change:

```python
"download_args": json.loads(
    meta_row[4], object_hook=json_convert_to_object
),
```

to:

```python
"download_args": DownloadArgs.from_json(meta_row[4]),
```

- [ ] **Step 6: Update `save_meta` to use `DownloadArgs.to_json()`**

In `CacheManager.save_meta`, there are two places where `download_args` is serialized. Both use:

```python
json.dumps(meta.download_args, default=json_convert_from_object),
```

Change both to:

```python
meta.download_args.to_json(),
```

(This applies to both the UPDATE branch and the INSERT branch of `save_meta`.)

- [ ] **Step 7: Update `download_marketdata` unpack call in `cache.py`**

At line 738, `_download_marketdata(meta, **meta.download_args)` unpacks the dict interface. With `DownloadArgs`, `**` unpacking requires `keys()` and `__getitem__`, which are both implemented. Verify this still works — no change needed if `DownloadArgs.__iter__` returns keys and `__getitem__` returns values. Test it by running the tests.

Also update `process_without_checks` at line 877:

```python
_download_marketdata(meta, **meta.download_args.to_dict())
```

Change to use `.to_dict()` to be explicit:

```python
_download_marketdata(meta, **meta.download_args.to_dict())
```

(Check if this is already using `**meta.download_args` — if so change to `.to_dict()`.)

- [ ] **Step 8: Run the new tests**

```bash
uv run pytest tests/test_cache.py -v
```

Expected: all PASS.

- [ ] **Step 9: Run full suite**

```bash
uv run pytest --no-integration -v
```

Expected: all PASS.

- [ ] **Step 10: Commit**

```bash
git add brasa/engine/cache.py tests/test_cache.py
git commit -m "feat(WIL-34): update CacheMetadata and CacheManager to use DownloadArgs"
```

---

## Task 3: Update assignment sites in `download.py` and `api.py`

**Files:**
- Modify: `brasa/engine/download.py`
- Modify: `brasa/engine/api.py`

- [ ] **Step 1: Update `download.py` line 137**

In `_download_marketdata`, change:

```python
meta.download_args = kwargs
```

to:

```python
meta.download_args = DownloadArgs(kwargs)
```

Add import at top of `download.py`:

```python
from brasa.util import DownloadArgs
```

Also update the `**meta.download_args` unpack on line 139 (the downloader call):

```python
fp, response, retry_info = template.downloader.download(
    on_attempt_failure=on_attempt_failure, **meta.download_args.to_dict()
)
```

- [ ] **Step 2: Update `api.py` line 113**

In `get_marketdata`, change:

```python
meta.download_args = kwargs
```

to:

```python
meta.download_args = DownloadArgs(kwargs)
```

- [ ] **Step 3: Update `api.py` line 302**

In `download_marketdata`, change:

```python
meta.download_args = args
```

to:

```python
meta.download_args = DownloadArgs(args)
```

Add import at top of `api.py`:

```python
from brasa.util import DownloadArgs
```

- [ ] **Step 4: Run full suite**

```bash
uv run pytest --no-integration -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add brasa/engine/download.py brasa/engine/api.py
git commit -m "feat(WIL-34): wrap download_args in DownloadArgs at all assignment sites"
```

---

## Task 4: Update rich-object access in `processing.py` and `readers/helpers.py`

**Files:**
- Modify: `brasa/engine/processing.py`
- Modify: `brasa/readers/helpers.py`

- [ ] **Step 1: Update `processing.py` line 32**

In `get_fname_part`, change:

```python
fname_part = meta.download_args["refdate"].strftime(fmt)
```

to:

```python
fname_part = meta.download_args.get_object("refdate").strftime(fmt)
```

Also update any other `.strftime()` calls on `meta.download_args[...]` in the same function (lines 34, 36, 44, 46). Each pattern like `meta.download_args['issuingCompany']` that is just used as a string can stay as `meta.download_args["issuingCompany"]` (dict-interface). Only the `.strftime()` calls need `.get_object()`.

Specifically, update all four `df['refdate'].iloc[0].strftime(fmt)` lines — those read from the DataFrame, not from `download_args`, so they are fine as-is.

The one change needed is:

```python
# line 32: before
fname_part = meta.download_args["refdate"].strftime(fmt)
# after
fname_part = meta.download_args.get_object("refdate").strftime(fmt)
```

- [ ] **Step 2: Update `readers/helpers.py` line 447**

Change:

```python
df["refdate"] = pd.to_datetime(meta.download_args["refdate"])
```

to:

```python
df["refdate"] = pd.to_datetime(meta.download_args.get_object("refdate"))
```

(`pd.to_datetime` accepts both `datetime` objects and ISO strings, but using `get_object` makes the intent explicit and consistent.)

- [ ] **Step 3: Run full suite**

```bash
uv run pytest --no-integration -v
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add brasa/engine/processing.py brasa/readers/helpers.py
git commit -m "feat(WIL-34): use get_object() for rich date access in processing and helpers"
```

---

## Task 5: Final verification and cleanup

**Files:**
- No new files; verify all checks pass.

- [ ] **Step 1: Search for any remaining raw dict assignment to `download_args`**

```bash
grep -n "download_args\s*=" brasa/engine/cache.py brasa/engine/download.py brasa/engine/api.py
```

Verify that every assignment site now wraps with `DownloadArgs(...)` or assigns a `DownloadArgs` instance. The only exception is `CacheMetadata.__init__` (`= DownloadArgs({})`) and `from_dict` (which already handles the wrapping).

- [ ] **Step 2: Search for remaining `.strftime()` calls on raw `download_args` subscript**

```bash
grep -n 'download_args\[.*\]\.strftime' brasa/
```

Expected: zero results.

- [ ] **Step 3: Run the full test suite**

```bash
uv run pytest --no-integration -v
```

Expected: all PASS.

- [ ] **Step 4: Run ruff**

```bash
uv run ruff check . && uv run ruff format --check .
```

Fix any issues, then re-run to confirm clean.

- [ ] **Step 5: Run pre-commit**

```bash
uv run pre-commit run --all-files
```

Expected: all PASS.

- [ ] **Step 6: Commit any fixups**

```bash
git add -u
git commit -m "fix(WIL-34): ruff and pre-commit cleanup"
```

---

## Notes on Migration of Existing DB Rows

The Linear issue references WIL-35 as a sub-issue for scanning existing `cache_metadata` rows that have bare `"YYYY-MM-DD"` download_args. After this fix:

- **New downloads** will always write `"YYYY-MM-DDTHH:MM:SS"`.
- **Existing rows** with the old bare-date format will get a **different hash** when loaded via `DownloadArgs.from_json()` since `from_json` does NOT re-normalize (values are already canonical strings from the DB). For bcb-sgs rows with `"2000-01-01"` stored, `DownloadArgs.from_json(...)` will keep `"2000-01-01"` as-is (it is a valid JSON string, not re-normalized). This means the `id` will still mismatch!

**Critical fix for `from_json`:** `from_json` must also normalize, because existing DB rows may contain bare date strings. Change `from_json` to normalize on load:

```python
@classmethod
def from_json(cls, s: str) -> "DownloadArgs":
    """Deserialize from JSON — normalizes values to canonical form."""
    obj = cls.__new__(cls)
    raw = json.loads(s)
    obj._data = {k: _normalize_download_arg(v) for k, v in raw.items()}
    return obj
```

This ensures that existing rows with `"2000-01-01"` will be normalized to `"2000-01-01T00:00:00"` on load, producing the same hash as a freshly-created `DownloadArgs({"refdate": "2000-01-01"})`.

Update `test_from_json_does_not_reconvert_datetime_strings` accordingly — it should still hold because `"2024-01-08T00:00:00"` matches `_DATETIME_RE` but `_normalize_download_arg` only acts on `_DATE_RE` strings, so datetime strings pass through unchanged.

Add an additional test for the migration case:

```python
def test_from_json_normalizes_bare_date_strings(self):
    """Existing DB rows with bare dates are normalized on load."""
    import json
    raw_json = json.dumps({"refdate": "2000-01-01"})
    restored = DownloadArgs.from_json(raw_json)
    assert restored["refdate"] == "2000-01-01T00:00:00"
```

Add this test to `TestSerialization` in `tests/test_download_args.py` in **Task 1 Step 1** before implementing.
