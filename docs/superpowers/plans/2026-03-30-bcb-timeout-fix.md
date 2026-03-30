# BCB Timeout Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set the BCB HTTP client timeout to 60 seconds globally so `sgs.get_json()` calls don't fail on slow connections.

**Architecture:** Import the `bcb.http._CLIENT` singleton in `brasa/downloaders/downloaders.py` and set its `timeout` property to 60.0 at module load time. This applies to all subsequent calls to `sgs.get_json()` without code changes to existing downloader logic.

**Tech Stack:** Python, python-bcb library (existing dependency), no new dependencies

---

### Task 1: Add BCB Timeout Configuration

**Files:**
- Modify: `brasa/downloaders/downloaders.py:1-15` (top of file, after imports)

**Context:** The BCB library uses a module-level HTTP client singleton (`bcb.http._CLIENT`). By setting its `timeout` property to 60.0, all HTTP operations performed by the library for the rest of the Python process will wait up to 60 seconds before timing out.

- [ ] **Step 1: Open the downloaders file and locate the BCB import**

Run: `head -20 brasa/downloaders/downloaders.py`

You should see imports including `from bcb import sgs` around line 12.

- [ ] **Step 2: Add the timeout configuration**

After the line `from bcb import sgs` (currently line 12), add these two lines:

```python
from bcb.http import _CLIENT

_CLIENT.timeout = 60.0
```

The file should now look like:
```python
import binascii
import io
import json
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import IO

import bizdays
import pytz
import requests
from bcb import sgs
from bcb.http import _CLIENT

_CLIENT.timeout = 60.0

from brasa.engine.exceptions import DownloadException, InvalidContentException
```

- [ ] **Step 3: Run all tests to verify nothing breaks**

Run: `uv run pytest`

Expected: All tests pass (or same pass/skip/fail as before the change).

- [ ] **Step 4: Run Ruff to check code style**

Run: `uv run ruff check . && uv run ruff format --check .`

Expected: No issues, or only pre-existing issues.

- [ ] **Step 5: Run pre-commit hooks**

Run: `uv run pre-commit run --all-files`

Expected: All hooks pass.

- [ ] **Step 6: Commit the change**

```bash
git add brasa/downloaders/downloaders.py
git commit -m "fix: set BCB HTTP client timeout to 60 seconds

Configure bcb.http._CLIENT with a 60-second timeout to prevent
timeout errors on slow connections or when the BCB API is slow.
This applies globally to all sgs.get_json() calls.

Co-Authored-By: Claude Haiku 4.5 <noreply@anthropic.com>"
```

---

## Self-Review Against Spec

✓ **Problem addressed:** Timeout on `sgs.get_json()` calls — fixed by setting global HTTP client timeout

✓ **Solution matches spec:** Sets `_CLIENT.timeout = 60.0` at module level, exactly as designed

✓ **Architecture:** Global scope confirmed — applied at module import time before any downloader classes instantiate

✓ **Backwards compatibility:** No API changes, no new exceptions, no changes to exception handling logic

✓ **Testing:** Existing tests cover `BCBSGSDownloader`; no new tests needed

✓ **Completeness:** Single file change, single task, with full test/lint/pre-commit validation
