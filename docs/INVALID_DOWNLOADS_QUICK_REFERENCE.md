# Quick Reference: Invalid Download Status

## Problem Solved
When `download_marketdata()` encounters a file that fails validation, it now:
1. ✅ Marks the metadata as invalid (persisted in cache)
2. ✅ Prevents duplicate download attempts
3. ✅ Stores the reason for failure
4. ✅ Allows forced re-download with `reprocess=True`

## Key Changes

| File | Change | Impact |
|------|--------|--------|
| `brasa/engine/exceptions.py` | Added `InvalidContentException` | New exception type for validation failures |
| `brasa/engine/cache.py` | Added `is_invalid_download`, `invalid_download_reason` fields | Track invalid status in metadata |
| `sql/create-meta-db.sql` | Added 2 new columns to `cache_metadata` table | Persist invalid status to database |
| `brasa/engine/download.py` | Wrap validation in try/except, raise `InvalidContentException` | Catch validation errors |
| `brasa/engine/api.py` | Updated `_should_download()` to skip invalid entries | Prevent re-downloads of invalid cache |
| `tests/test_invalid_downloads.py` | New comprehensive test suite | 10 tests covering all scenarios |

## Behavior Summary

### First Download Attempt (Invalid File)
```
download_marketdata(template, arg1=value)
  → validation fails
  → InvalidContentException raised
  → metadata.is_invalid_download = True
  → metadata saved to cache
```

### Second Download Attempt (Same Arguments)
```
download_marketdata(template, arg1=value)
  → _should_download() returns False
  → Download skipped (no error, no attempt)
```

### Force Re-Download
```
download_marketdata(template, arg1=value, reprocess=True)
  → _should_download() returns True
  → Download attempted
  → On success: invalid flags cleared
```

## Testing

Run the test suite:
```bash
poetry run pytest tests/test_invalid_downloads.py -v
```

Expected: **10/10 tests pass**

## Backward Compatibility

✅ No breaking changes
✅ Works with existing code
✅ Optional feature (works transparently)
✅ Database migration automatic

## Implementation Details

### InvalidContentException Flow
```
downloader.validate() → Exception
    ↓
except Exception as e:
    raise InvalidContentException(message) from e
    ↓
except InvalidContentException:
    meta.is_invalid_download = True
    meta.invalid_download_reason = message
    save_meta(meta)
```

### _should_download() Logic
```python
if reprocess:
    remove_meta()
    return True

if has_meta(meta):
    load_meta(meta)
    if is_invalid_download:
        return False  # Skip

if not has_successful_trial(meta):
    return True

return False
```

## Example: Real-World Usage

```python
import brasa

# This fails with invalid content
try:
    brasa.download_marketdata(
        "b3-company-info",
        issuingCompany="CGOS"
    )
except Exception as e:
    print(f"Download failed: {e}")
    # Invalid status is now cached

# Retry without force - skipped automatically
try:
    brasa.download_marketdata(
        "b3-company-info",
        issuingCompany="CGOS"
    )
except Exception as e:
    print(f"This won't execute - download was skipped")
    # _should_download() returned False

# If the issue is fixed externally, force a retry
try:
    brasa.download_marketdata(
        "b3-company-info",
        issuingCompany="CGOS",
        reprocess=True
    )
    print("Success! Invalid flag cleared on successful download")
except Exception as e:
    print(f"Still failing: {e}")
```

## Files Modified

1. [brasa/engine/exceptions.py](brasa/engine/exceptions.py) - Added `InvalidContentException`
2. [brasa/engine/cache.py](brasa/engine/cache.py) - Metadata fields + persistence logic
3. [sql/create-meta-db.sql](sql/create-meta-db.sql) - Database schema extension
4. [brasa/engine/download.py](brasa/engine/download.py) - Validation error handling
5. [brasa/engine/api.py](brasa/engine/api.py) - Skip logic for invalid entries
6. [tests/test_invalid_downloads.py](tests/test_invalid_downloads.py) - Comprehensive tests

## Questions?

Refer to [IMPLEMENTATION_INVALID_DOWNLOADS.md](IMPLEMENTATION_INVALID_DOWNLOADS.md) for detailed documentation.
