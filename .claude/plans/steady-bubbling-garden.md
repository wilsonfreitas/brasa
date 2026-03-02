# Plan: Raise InvalidContentException in B3PagedURLEncodedDownloader

## Context

`B3PagedURLEncodedDownloader.download()` returns `None` when the B3 API responds with an empty
`results` list (no data available for the given parameters). In the current pipeline:

1. The `None` propagates to `template.MarketDataDownloader.download()`, which raises
   `DownloadException("Download returned null file pointer.")`.
2. `cache.CacheManager.download_marketdata()` catches `DownloadException` and records the
   trial as **FAILED** (status code `"F"`).

The correct semantic is **INVALID** (status code `"I"`), which prevents future re-download
attempts for the same parameters. This requires the downloader to raise `InvalidContentException`
instead of returning `None`, and the template layer must let non-retriable exceptions propagate
without wrapping them in `DownloadException`.

---

## Affected Files

| File | Change |
|---|---|
| `brasa/downloaders/downloaders.py` | Raise `InvalidContentException` instead of `return None` |
| `brasa/engine/template.py` | Propagate non-retriable exceptions as-is (RTRY-003) |
| `tests/test_downloads.py` | Update comment about null file pointer for `b3-cash-dividends` |
| `tests/test_download_retry.py` | Add test for `InvalidContentException` propagating without wrapping |

A new test file `tests/test_b3_paged_downloader.py` will hold focused unit tests for
`B3PagedURLEncodedDownloader`.

---

## Implementation Steps

### 1. `brasa/downloaders/downloaders.py`

Add `InvalidContentException` import and raise it when results are empty:

```python
from brasa.engine.exceptions import DownloadException, InvalidContentException
```

In `B3PagedURLEncodedDownloader.download()`, replace:
```python
if len(results) == 0:
    return None
```
with:
```python
if len(results) == 0:
    raise InvalidContentException("No results returned for the given query parameters")
```

### 2. `brasa/engine/template.py` — `MarketDataDownloader.download()`

The docstring already says `InvalidContentException`, `CorruptedContentException`, and
`DuplicatedFolderException` *"propagate immediately (RTRY-003)"*, but the exception-handling
block wraps everything in `DownloadException`. Fix this by re-raising non-retriable exceptions
before the wrapping logic:

In the `download()` method, after the `try/except` block that sets `caught_err`, add an early
re-raise for the non-retriable family:

```python
# RTRY-003: non-retriable exceptions propagate immediately, without wrapping
from .exceptions import (
    CorruptedContentException,
    DuplicatedFolderException,
    InvalidContentException,
)
if isinstance(caught_err, (InvalidContentException, CorruptedContentException, DuplicatedFolderException)):
    raise caught_err
```

This block goes at the top of the `if caught_err is not None:` block, before `wrapped = ...`.

### 3. `tests/test_b3_paged_downloader.py` (new file)

Unit tests for `B3PagedURLEncodedDownloader` using mocked `requests.get`:

- **`test_empty_results_raises_invalid_content_exception`** — mock the first page to return
  `{"page": {"totalPages": 1}, "results": []}` and assert that `download()` raises
  `InvalidContentException`.
- **`test_single_page_with_results_returns_file`** — mock a response with results and assert
  that a valid `BytesIO` is returned.
- **`test_multi_page_results_are_combined`** — mock two pages and assert the results are merged.

### 4. `tests/test_download_retry.py`

Add a test to `TestIsRetriableFailure` (or a new class) verifying that when
`InvalidContentException` is raised by a download function, it propagates correctly through
`MarketDataDownloader.download()` without being swallowed:

```python
def test_invalid_content_exception_propagates_through_template():
    """InvalidContentException raised in download function must propagate as-is."""
    downloader = _make_downloader()
    with patch.object(downloader, "download_function", side_effect=InvalidContentException("no data")):
        with pytest.raises(InvalidContentException):
            downloader.download()
```

### 5. `tests/test_downloads.py`

Update the comment at lines 76-79 to reflect the new behaviour:
```python
# b3-cash-dividends with no matching records now raises InvalidContentException (not null fp)
```

---

## Verification

```bash
# Run all unit tests (no network required)
uv run pytest tests/test_b3_paged_downloader.py tests/test_download_retry.py -v

# Full test suite
uv run pytest

# Lint + format
uv run ruff check . && uv run ruff format --check .

# Pre-commit
uv run pre-commit run --all-files
```

Expected outcomes:
- New tests in `test_b3_paged_downloader.py` pass.
- New propagation test in `test_download_retry.py` passes.
- Existing tests remain green.
- No lint/format errors.
