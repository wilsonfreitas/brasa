# Fix: date vs datetime type mismatch breaks meta.id round-trip

## Context

`process_marketdata("b3-bvbg028")` crashes with `sqlite3.IntegrityError: UNIQUE constraint failed: cache_metadata.download_checksum`. The root cause is a `date` vs `datetime` type mismatch in the JSON serialization/deserialization of `download_args`.

**How it happens:**
1. `bizdays` Calendar methods (`following`, `seq`, etc.) return `date` objects
2. `DateRange` passes these `date` objects into download args as `refdate`
3. `json_convert_from_object` serializes `date(2025, 3, 12)` as `"2025-03-12"` (no time component)
4. `json_convert_to_object` deserializes `"2025-03-12"` back as `datetime(2025, 3, 12)` — always returns `datetime`
5. `meta.id` (computed from `pickle.dumps(download_args)`) changes because `pickle.dumps(date(...)) != pickle.dumps(datetime(...))`
6. `save_meta` looks up the wrong ID → tries INSERT → UNIQUE constraint on `download_checksum` fails
7. The exception handler also calls `save_meta` → same failure → unhandled crash

**Secondary issue:** The error handler in `process_single` (`api.py:476-482`) calls `save_meta` which can fail with the same error, causing an unhandled exception.

## Fix

### Step 1: Normalize dates in `json_convert_from_object` (primary fix)

**File:** `brasa/engine/core.py`

Ensure `date` objects are serialized with the time component so they round-trip as `datetime`:

```python
def json_convert_from_object(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return datetime(obj.year, obj.month, obj.day).isoformat()
    raise TypeError(...)
```

This makes `date(2025, 3, 12)` serialize as `"2025-03-12T00:00:00"` instead of `"2025-03-12"`, so `json_convert_to_object` returns `datetime(2025, 3, 12, 0, 0)`.

**But this alone doesn't fix it** — we also need `meta.id` to compute the same hash whether the input was `date` or `datetime`.

### Step 2: Normalize dates in `generate_checksum_for_template`

**File:** `brasa/util.py`

Convert any `date` values in `args` to `datetime` before hashing, so the ID is stable regardless of input type:

```python
def generate_checksum_for_template(template, args, extra_key=""):
    normalized = {}
    for k, v in args.items():
        if isinstance(v, date) and not isinstance(v, datetime):
            v = datetime(v.year, v.month, v.day)
        normalized[k] = v
    t = tuple(sorted(normalized.items(), key=lambda x: x[0]))
    ...
```

### Step 3: Protect `save_meta` error handler

**File:** `brasa/engine/api.py` (line ~476-482)

Wrap the error-handler's `save_meta` call in a try/except so it doesn't mask the original error:

```python
except Exception as ex:
    meta.processing_errors = str(ex)
    with db_lock:
        try:
            cache.save_meta(meta)
        except Exception:
            pass  # Don't mask original error
    ...
```

### Step 4: Fix the existing broken entry in the DB

**File:** One-time data fix (manual or script)

The entry with `id=a9ab470f...` / `refdate=2025-03-12` has a stale ID that won't match after the fix. Options:
- Delete and re-download: `sqlite3 meta.db "DELETE FROM cache_metadata WHERE id = 'a9ab470f55be084e296e6b022b354d5c';"` + remove its raw folder
- Or update its `download_args` to `{"refdate": "2025-03-12T00:00:00"}` and update its `id` to the new hash

## Files to modify

| File | Change |
|---|---|
| `brasa/engine/core.py` | `json_convert_from_object`: convert `date` → `datetime` before `.isoformat()` |
| `brasa/util.py` | `generate_checksum_for_template`: normalize `date` → `datetime` in args |
| `brasa/engine/api.py` | Wrap error-handler `save_meta` in try/except |

## Verification

1. Unit test: verify `generate_checksum_for_template` returns same hash for `date` and `datetime` inputs
2. Unit test: verify `json_convert_from_object(date(...))` produces ISO string with time component
3. Unit test: verify `json_convert_to_object` round-trips correctly for both formats
4. Fix the broken DB entry, then run `uv run python cli-ei.py` — should process without error
5. `uv run pytest`
6. `uv run ruff check . && uv run ruff format --check .`
7. `uv run pre-commit run --all-files`
