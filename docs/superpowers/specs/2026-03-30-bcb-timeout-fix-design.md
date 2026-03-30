# Design: Fix BCB `sgs.get_json()` Timeout

**Date:** 2026-03-30
**Status:** Approved

## Problem Statement

The `BCBSGSDownloader` calls `sgs.get_json()` from the `python-bcb` library to fetch time series data from the BCB's SGS (Sistema Gerenciador de Séries). Occasionally, these calls timeout on slow connections or when the BCB API is slow to respond. Currently, any exception—including timeout errors—is silently caught and returns `None`, making it difficult to distinguish between timeouts and other failures.

## Solution Overview

Set the BCB HTTP client timeout globally to 60 seconds at module import time in `brasa/downloaders/downloaders.py`. This ensures all calls to `sgs.get_json()` have sufficient time to complete before timing out.

## Architecture

### Change Location
`brasa/downloaders/downloaders.py`

### Implementation
After the existing imports (specifically after `from bcb import sgs`), add:

```python
from bcb.http import _CLIENT
_CLIENT.timeout = 60.0
```

This configures the module-level HTTP client singleton used internally by the `python-bcb` library.

### Scope
- **Applies to:** All calls to `sgs.get_json()` in the current Python process
- **Set at:** Module import time (before any downloader classes are instantiated)
- **Lifetime:** For the entire lifetime of the Python process

### Backwards Compatibility
- No API changes
- No new dependencies
- Existing code paths remain unchanged
- Exception handling in `BCBSGSDownloader.download()` remains the same (catch all exceptions, return `None`)

## Why This Approach

1. **Simplicity:** Two lines of code, no function signatures to change
2. **Proven:** Directly based on user-tested solution
3. **Global effect:** Applies to all BCB operations without repetition
4. **Maintenance:** Single configuration point; future BCB timeouts or client configs can extend this approach

## Testing Impact

- **Unit tests:** Continue to pass (mock `sgs.get_json()`, don't execute it)
- **Integration tests:** Benefit from longer timeout when actually calling the BCB API
- **New tests:** None needed; change is transparent to callers

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| HTTP client modification affects other code | We only set timeout; other properties unchanged. Only affects BCB library operations. |
| 60 seconds too long/short | Chosen value is conservative; can be adjusted if needed in future |
| Module import side effects | Acceptable trade-off; downloaders module is always imported by brasa during initialization |

## Future Extensions

If additional external client timeouts need configuration (e.g., ANBIMA, B3), extend this pattern:
- Keep all client initialization in `downloaders.py` or move to dedicated `brasa/config.py`
- Document all timeout values as constants at module top

## Success Criteria

✓ `BCBSGSDownloader` calls to `sgs.get_json()` complete without timeout on slow connections
✓ Existing tests pass
✓ No new test failures
✓ No breaking API changes
