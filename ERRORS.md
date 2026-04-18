# ERRORS.md — Error Analysis and Troubleshooting Guide

This document catalogs errors encountered during execution of the brasa CLI and provides troubleshooting guidance.

---

## Errors Observed in `daily-b3.yaml` Download Run

### 1. DownloadException: HTTP 500 on b3-bvbg028

**Status:** FAILED
**Template:** `b3-bvbg028`
**Date:** refdate=2026-03-27
**Duration:** 103.11s
**Error Code:** `status_code = 500`
**URL:** `https://www.b3.com.br/pesquisapregao/download?filelist=IN260327.zip`

#### What This Means

The B3 server returned a **HTTP 500 Internal Server Error** while trying to download the BVBG028 file (Indicadores) for 2026-03-27. This is a **server-side error** (not a problem with your request).

#### Why It Happened

- B3 server was temporarily unavailable or experiencing issues
- The specific file for this date may not exist or be inaccessible on the server
- Server issue is transient and may resolve on retry

#### How to Troubleshoot

1. **Check B3 website availability:**
   - Visit https://www.b3.com.br manually
   - Try downloading the file manually for the same date
   - If manual download also fails with 500, the issue is on B3's side

2. **Retry the download:**
   - The CLI automatically retries failed downloads
   - If retry fails again, wait a few hours and try again (B3 may be under maintenance)
   - HTTP 500 is typically transient

3. **Check the error details:**
   - Template: `b3-bvbg028`
   - Date: 2026-03-27 (note: this is a future date — is this correct?)
   - Verify the date parameter makes sense for your use case

4. **For repeated 500 errors on this template:**
   - Check B3's status page
   - Try a different date range to isolate the issue
   - The template URL may be outdated if B3 changed their API

#### Error Context

```
State:
  Downloaded files: (none)          ← No file was downloaded before error
  Processed:        (not processed) ← File was never processed
```

This indicates the download failed **before** any files were saved, so no partial data was persisted.

---

## Common Error Patterns in brasa

### Pattern 1: DownloadException (HTTP Errors)

**Format:**
```
DownloadException: status_code = NNN url = https://...
```

**Meaning:**
- `status_code = 400` — Bad request (check your arguments/parameters)
- `status_code = 401/403` — Authentication or permission issue
- `status_code = 404` — File not found on server
- `status_code = 500/502/503` — Server error (transient, usually retries help)
- `status_code = 429` — Rate limited (too many requests; wait and retry)

**What to do:**
1. Check the URL is valid
2. For 404: the file may not exist for this date
3. For 500+: wait and retry (server is busy)
4. For 429: increase `retry_delay` in template or wait before retrying

---

### Pattern 2: InvalidContentException

**Meaning:**
- Downloaded file content is invalid
- File is empty, truncated, or wrong format
- Will NOT be retried (marked as permanent failure)

**Examples:**
- Empty ZIP file
- Corrupted CSV/JSON
- Wrong file format (e.g., HTML error page instead of data)

**What to do:**
1. Check the URL returns correct data manually
2. Verify the template's validation rules are correct
3. Check B3's website for data availability on that date

---

### Pattern 3: CorruptedContentException

**Meaning:**
- File appears corrupted but may succeed on retry
- Examples: truncated files, encoding errors, incomplete archives
- WILL be retried on next run

**What to do:**
1. Retry the download (automatic on next run)
2. Check network stability during the download (was it interrupted?)
3. If persistent, the file may be genuinely corrupted at source

---

### Pattern 4: DuplicatedFolderException

**Meaning:**
- The download folder for this date already exists
- The system won't overwrite it

**What to do:**
1. Delete the existing folder in `.brasa-cache/` or your `BRASA_DATA_PATH`
2. Re-run the download
3. Or skip this date if you already have the data

---

### Pattern 5: Missing Dependency

**Meaning:**
- A template depends on an upstream dataset that hasn't been downloaded
- Template is skipped

**What to do:**
1. Run the upstream template first
2. Or ensure dependencies are listed correctly in the template YAML
3. Check dependency_graph with: `brasa depgraph b3-bvbg028`

---

## Interpreting the CLI Output

### Status Legend

```
. (passed)    — Template downloaded successfully
F (failed)    — Template failed to download
E (error)     — Unexpected error during processing
S (skipped)   — Template was skipped (dependency missing, etc.)
D (duplicated)— Data already exists, no re-download
I (invalid)   — Invalid configuration
C (corrupted) — File is corrupted (will retry later)
```

### Example Output

```
Download b3-bvbg087 ..... [5/5] (0.8s)
```

This means:
- Template: `b3-bvbg087`
- Status: 5 dots = all 5 date parameters passed
- Time: completed in 0.8 seconds

```
Download b3-bvbg028 ....F [5/5] (177.5s)
```

This means:
- Template: `b3-bvbg028`
- Status: 4 dots + 1 F = 4 dates passed, 1 date failed
- Time: 177.5 seconds total

---

## Unclear Error Messages

### Issue: "NoneType: None" Traceback

**Observed in:**
```
Traceback:
NoneType: None
```

**Problem:**
- This is a **useless traceback** — doesn't show the actual error location
- Happens when the exception message itself is None
- Makes debugging very difficult

**What's likely happening:**
- The actual error is hidden earlier in the chain
- Look at the `Message:` field instead:
  ```
  Message: status_code = 500 url = https://www.b3.com.br/...
  ```

**How to fix:**
- The error message should show `status_code = 500` (HTTP error)
- Not the traceback line

---

### Issue: Unhelpful Error Messages

The CLI can sometimes show unclear error messages:

1. **"Market data download failed: null file pointer"**
   - The downloader returned no file
   - The HTTP request succeeded but returned an empty response
   - Usually means the file doesn't exist for this date

2. **"Market data download failed: empty zip file"**
   - Downloaded a ZIP but it's empty (no files inside)
   - Usually means B3 has no data for this date

3. **"Content validation failed"**
   - File exists but doesn't match expected format/size/structure
   - Check the template's validation rules in YAML

4. **"Downloaded content validation failed for [filename]: ..."**
   - Look at the bracketed filename and the error message
   - Could be encoding issue, missing columns, wrong format

---

## How Errors Are Categorized

### Retriable Errors (Will Retry)
- HTTP 500/502/503 (server errors)
- HTTP 429 (rate limited)
- `CorruptedContentException` (truncated files, etc.)
- Connection timeouts

### Non-Retriable Errors (Won't Retry)
- HTTP 400/404 (bad request, not found)
- HTTP 401/403 (authentication)
- `InvalidContentException` (empty files, wrong format)
- `DuplicatedFolderException` (folder exists)

### Configuration Errors (Will Fail)
- Template not found
- Invalid template YAML
- Missing dependencies
- Invalid date range

---

## Debugging Strategies

### 1. Check Template Configuration

```bash
brasa show-template b3-bvbg028
```

This shows:
- Download URL pattern
- Retry configuration
- Validation rules
- Expected output format

### 2. Check Dependency Graph

```bash
brasa depgraph --template b3-bvbg028
```

Shows what this template depends on (if anything).

### 3. Check Cache Status

```bash
brasa doctor
```

Shows:
- Cache folder location
- Database integrity
- Orphan files
- Date gaps in data

### 4. Manual Download Test

Try downloading the URL manually to see if B3 responds correctly:

```bash
curl -I "https://www.b3.com.br/pesquisapregao/download?filelist=IN260327.zip"
```

This shows HTTP status without downloading the full file.

### 5. Check Existing Data

Look at what's already in cache:

```bash
ls -lh ~/.brasa-cache/raw/  # or your BRASA_DATA_PATH
```

---

## Common Reasons for Failures by Template

### b3-bvbg028 (Indicators)
- **Likely fails:** Weekends, holidays (market closed)
- **HTTP 500:** B3 server issue (transient)
- **404:** Data not published for this date

### b3-bvbg086/087 (Options)
- **Likely fails:** Fridays only (expires weekly)
- **404:** No options available for this date

### b3-cotahist-daily (Stock Prices)
- **Likely fails:** Weekends, holidays
- **Usually most reliable** (core market data)

### b3-economic-indicators-fwf (Economic Data)
- **Likely fails:** Month-end only
- **Check calendar:** Uses fiscal calendar, not calendar dates

### BCB/ANBIMA templates
- **Network issues:** External APIs may be slow
- **Rate limiting:** Too many parallel requests
- **Data delays:** May not be published immediately

---

## Solutions by Error Type

| Error | Root Cause | Solution |
|-------|-----------|----------|
| `status_code = 500` | B3 server error | Wait 1-2 hours, retry |
| `status_code = 404` | File not found | Date may be wrong or no data for that date |
| `status_code = 429` | Rate limited | Increase `retry_delay`, reduce `max_workers` |
| Empty ZIP | No data for date | This is expected for holidays/weekends |
| Download timeout | Network slow | Increase `download_timeout` in template |
| Duplicate folder | Cache exists | Delete old cache, re-download |
| Validation failed | Format changed | Update template, report to maintainers |
| Memory error | Large file | Reduce date range, increase RAM |
| "No such file" | Template missing | Download the template first |

---

## Recommended Next Steps

1. **For the current b3-bvbg028 failure:**
   - Check if 2026-03-27 is a weekday (market open day)
   - Wait a few hours and retry (HTTP 500 is usually transient)
   - Try a different date to see if it's date-specific or template-wide

2. **For future downloads:**
   - Use `--no-integration` flag to skip slow/fragile API calls
   - Stagger downloads (don't request all dates in one batch)
   - Monitor error reports and skip known-failing dates

3. **For template issues:**
   - Report template failures in QUESTIONS.md with specifics
   - Test template with manual URL download first
   - Check B3's website for data availability

---

**Last Updated:** 2026-03-28
**Command:** `uv run python -m brasa.cli download --plan daily-b3.yaml`
**Partial Run Captured** (command interrupted after ~3 minutes of ~30 minute total runtime)
