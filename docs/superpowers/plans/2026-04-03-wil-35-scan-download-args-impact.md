# WIL-35: Scan Script for DownloadArgs Normalization Impact Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write a read-only scan script that analyzes existing `cache_metadata` rows to count how many have non-canonical date values and would get a new hash after DownloadArgs normalization.

**Architecture:** A standalone Python script that connects to the SQLite metadata database, iterates through all `cache_metadata` rows, and for each row: (1) loads the stored `download_args` JSON, (2) detects non-canonical date strings (bare `YYYY-MM-DD`), (3) computes both the stored id and the new normalized id, and (4) aggregates results by template with a summary table.

**Tech Stack:** Python 3.10+, sqlite3, brasa's `DownloadArgs` and `generate_checksum_for_template` from WIL-34.

---

## File Map

| File | Change |
|------|--------|
| `scripts/scan_download_args.py` | Create new script |

---

## Task 1: Implement scan script

**Files:**
- Create: `scripts/scan_download_args.py`

- [ ] **Step 1: Create the script skeleton with imports and main function**

Create `scripts/scan_download_args.py` with:

```python
"""Scan cache_metadata rows to assess DownloadArgs normalization impact.

Identifies rows with non-canonical date values (YYYY-MM-DD instead of
YYYY-MM-DDTHH:MM:SS) and computes which rows would get a new id after
normalization.

Usage:
    uv run python scripts/scan_download_args.py

No database modifications are made — this is a read-only scan.
"""

import json
import os
import re
import sqlite3
from collections import defaultdict
from pathlib import Path

from brasa.util import DownloadArgs, generate_checksum_for_template


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def has_non_canonical_dates(download_args_dict: dict) -> bool:
    """Check if any value in download_args is a non-canonical date string."""
    for value in download_args_dict.values():
        if isinstance(value, str) and _DATE_RE.match(value):
            return True
    return False


def scan_cache_metadata() -> None:
    """Scan all cache_metadata rows and report normalization impact."""
    cache_path = os.environ.get("BRASA_DATA_PATH", str(Path.cwd() / ".brasa-cache"))
    db_path = Path(cache_path) / "meta" / "meta.db"

    if not db_path.exists():
        print(f"Error: Cache metadata database not found at {db_path}")
        return

    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    # Query all rows from cache_metadata
    c.execute("SELECT id, template, download_args, extra_key FROM cache_metadata")
    rows = c.fetchall()
    conn.close()

    if not rows:
        print("No rows found in cache_metadata.")
        return

    # Aggregate results by template
    results = defaultdict(lambda: {"total": 0, "non_canonical": 0, "id_would_change": 0})

    total_rows = 0
    total_non_canonical = 0
    total_id_change = 0

    for stored_id, template, download_args_json, extra_key in rows:
        total_rows += 1

        # Parse download_args from JSON (plain load, no object_hook)
        try:
            download_args_dict = json.loads(download_args_json)
        except (json.JSONDecodeError, TypeError):
            print(f"Warning: Could not parse download_args for id {stored_id}")
            continue

        # Check for non-canonical dates
        has_non_canonical = has_non_canonical_dates(download_args_dict)
        if has_non_canonical:
            total_non_canonical += 1
            results[template]["non_canonical"] += 1

        # Compute new id with DownloadArgs normalization
        normalized_args = DownloadArgs(download_args_dict)
        new_id = generate_checksum_for_template(template, normalized_args, extra_key)

        if stored_id != new_id:
            total_id_change += 1
            results[template]["id_would_change"] += 1

        results[template]["total"] += 1

    # Print results
    print("\nScanning cache_metadata...\n")
    print(
        f"{'Template':<40} {'Total':>8} {'Non-canonical':>18} {'Id would change':>16}"
    )
    print("-" * 85)

    for template in sorted(results.keys()):
        data = results[template]
        print(
            f"{template:<40} {data['total']:>8} {data['non_canonical']:>18} {data['id_would_change']:>16}"
        )

    print("-" * 85)
    print(f"\nTotal rows:             {total_rows}")
    print(f"Non-canonical dates:    {total_non_canonical}  ({100*total_non_canonical/total_rows:.1f}%)")
    print(f"Id would change:        {total_id_change}  ({100*total_id_change/total_rows:.1f}%)")


if __name__ == "__main__":
    scan_cache_metadata()
```

- [ ] **Step 2: Run the script to verify it works**

```bash
uv run python scripts/scan_download_args.py
```

Expected output: A table showing template names, total rows, non-canonical date counts, and id-change counts. Even if cache_metadata is empty or doesn't exist, the script should handle it gracefully.

- [ ] **Step 3: Test with a sample scenario**

If the cache is empty, that's fine — the script will report "No rows found" or "0 total rows". If there is data, verify the output makes sense (non-canonical dates should be <= total, id changes should be >= non-canonical dates since normalization also affects multi-valued args).

- [ ] **Step 4: Verify script can be run standalone**

```bash
cd /home/wilson/dev/python/brasa
uv run python scripts/scan_download_args.py
```

Expected: Script runs without errors and prints a summary (even if empty).

- [ ] **Step 5: Commit**

```bash
git add scripts/scan_download_args.py
git commit -m "feat(WIL-35): add cache_metadata scan script for DownloadArgs impact analysis"
```

---

## Notes

- **Read-only:** The script only reads from the database and never modifies it.
- **Works standalone:** Can be run at any time with `uv run python scripts/scan_download_args.py`.
- **No tests:** This is a utility/diagnostic script, not a testable component. Manual verification is sufficient.
- **Environment variable:** Respects `BRASA_DATA_PATH` like all other brasa code.
- **Error handling:** Gracefully handles missing database or malformed JSON rows with warnings.
