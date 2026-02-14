# Invalid Download Status Implementation Summary

## Overview

Added comprehensive support for tracking and managing invalid downloads in the cache metadata. This prevents duplicate download attempts for files that fail validation and provides a clear way to force re-download when needed.

## Changes Made

### 1. New Exception Type
**File:** [brasa/engine/exceptions.py](brasa/engine/exceptions.py)

Added `InvalidContentException` to distinguish download validation failures from transport/network failures:
```python
class InvalidContentException(Exception):
    """Raised when downloaded content is invalid or fails validation."""
    pass
```

### 2. Extended Cache Metadata
**File:** [brasa/engine/cache.py](brasa/engine/cache.py)

Added two new fields to `CacheMetadata`:
- `is_invalid_download: bool` - Flag indicating if content is marked invalid
- `invalid_download_reason: str` - Detailed error message explaining why content is invalid

Updated `to_dict()` to include both fields for serialization.

### 3. Database Schema
**File:** [sql/create-meta-db.sql](sql/create-meta-db.sql)

Extended `cache_metadata` table with two new columns:
```sql
is_invalid_download TEXT
invalid_download_reason TEXT
```

### 4. Metadata Persistence
**File:** [brasa/engine/cache.py](brasa/engine/cache.py)

Updated two key methods:

**`_load_meta_dict_by_id()`**: Added backward-compatible loading of invalid status fields:
```python
"is_invalid_download": meta_row[10] == "1" if len(meta_row) > 10 else False,
"invalid_download_reason": meta_row[11] if len(meta_row) > 11 else "",
```

**`save_meta()`**: Extended to persist invalid status to database (converts bool to "1"/"0" for SQLite TEXT column).

### 5. Download Validation
**File:** [brasa/engine/download.py](brasa/engine/download.py)

Wrapped `template.downloader.validate()` to catch validation exceptions and raise `InvalidContentException`:
```python
try:
    template.downloader.validate(man.cache_path(fname))
except Exception as e:
    raise InvalidContentException(
        f"Downloaded content validation failed for {fname}: {str(e)}"
    ) from e
```

### 6. Error Handling in Download Pipeline
**File:** [brasa/engine/cache.py](brasa/engine/cache.py)

Updated `download_marketdata()` to catch `InvalidContentException`:
```python
except InvalidContentException as e:
    # Mark metadata as invalid but save it for future reference
    meta.is_invalid_download = True
    meta.invalid_download_reason = str(e)
    self.save_trial(meta, False)
    self.clean_meta_raw_folder(meta)
    raise e
```

This ensures:
- Invalid downloads are persisted in cache metadata
- Raw download folder is cleaned up
- Failed trial is recorded
- Exception is re-raised for caller handling

### 7. Smart Download Decision Logic
**File:** [brasa/engine/api.py](brasa/engine/api.py)

Updated `_should_download()` to check invalid status:
```python
if cache.has_meta(meta):
    cache.load_meta(meta)
    # If metadata is marked as invalid, skip unless forced with reprocess
    if meta.is_invalid_download:
        return False
```

Behavior:
- **Normal case**: Invalid cached entries are skipped (function returns `False`)
- **Forced case**: Setting `reprocess=True` removes the invalid entry, allowing re-download
- **Backward compatibility**: Non-invalid entries follow original logic

## How It Works

### Scenario 1: Invalid File on First Download
```
download_marketdata("b3-company-info", issuingCompany="CGOS")
    ↓
Template downloader.validate() detects invalid payload
    ↓
InvalidContentException raised
    ↓
Caught in cache.download_marketdata()
    ↓
is_invalid_download = True
invalid_download_reason = "Error details..."
    ↓
Metadata saved to SQLite
    ↓
Raw files cleaned up
```

### Scenario 2: Subsequent Download Attempt (Without Force)
```
download_marketdata("b3-company-info", issuingCompany="CGOS")
    ↓
_should_download() loads cached metadata
    ↓
Detects is_invalid_download == True
    ↓
Returns False (skip download)
    ↓
No duplicate download attempted ✓
```

### Scenario 3: Forced Re-Download
```
download_marketdata("b3-company-info", issuingCompany="CGOS", reprocess=True)
    ↓
_should_download() sees reprocess=True
    ↓
Removes invalid cache entry
    ↓
Returns True (download)
    ↓
Fresh download attempted (clears invalid status on success)
```

## Testing

Added comprehensive test suite: [tests/test_invalid_downloads.py](tests/test_invalid_downloads.py)

**Test Coverage:**
- Metadata fields exist and initialize correctly
- `to_dict()` includes invalid fields
- `InvalidContentException` functionality
- Saving and loading invalid status from cache
- `_should_download()` skips invalid entries
- `_should_download()` forces re-download with `reprocess=True`
- Normal download flow unchanged (backward compatibility)
- `from_dict()` loads invalid status correctly
- Invalid reason is preserved across save/load cycles
- Invalid status cleared on successful re-download

**All 10 tests pass ✓**

## Backward Compatibility

✓ Existing code continues to work without changes
✓ Database migration handled gracefully (NULL values default to False/"")
✓ No breaking changes to function signatures
✓ Uses existing `reprocess` parameter (no new API)

## Benefits

1. **Avoids duplicate retries** - Invalid content is skipped automatically
2. **Preserves error context** - Reason stored for debugging
3. **Optional recovery path** - `reprocess=True` allows users to force retry
4. **Transparent to callers** - Works within existing download flow
5. **Database persistence** - Survives application restarts

## Usage Example

```python
import brasa

# First attempt - detects invalid content
brasa.download_marketdata("b3-company-info", issuingCompany="CGOS")
# → InvalidContentException raised
# → Metadata marked as invalid
# → Cached for future reference

# Second attempt - skips download
brasa.download_marketdata("b3-company-info", issuingCompany="CGOS")
# → Skipped (returns False from _should_download)
# → No download attempted

# Force re-download when issue is fixed
brasa.download_marketdata("b3-company-info", issuingCompany="CGOS", reprocess=True)
# → Invalid cache entry removed
# → Fresh download attempted
# → If successful, invalid status cleared
```
