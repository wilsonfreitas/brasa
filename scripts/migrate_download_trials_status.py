#!/usr/bin/env python
"""Migration script for download_trials status columns.

Adds missing status columns (status_code, status_name, reason, http_status)
to existing download_trials tables and backfills legacy rows:
    downloaded=1 -> status_code='.', status_name='PASSED'
    downloaded=0 -> status_code='F', status_name='FAILED'

Usage:
    poetry run python scripts/migrate_download_trials_status.py [CACHE_PATH]

If CACHE_PATH is omitted, uses $BRASA_DATA_PATH or ./.brasa-cache.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path


def migrate_download_trials(db_path: str) -> dict:
    """Migrate a single SQLite metadata database.

    Args:
        db_path: Path to the meta.db file.

    Returns:
        Dict with keys: added_columns, backfilled_passed, backfilled_failed.
    """
    result = {"added_columns": [], "backfilled_passed": 0, "backfilled_failed": 0}

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Check existing columns
    c.execute("PRAGMA table_info(download_trials)")
    existing = {row[1] for row in c.fetchall()}

    new_columns = {
        "status_code": "TEXT",
        "status_name": "TEXT",
        "reason": "TEXT",
        "http_status": "INTEGER",
    }

    for col_name, col_type in new_columns.items():
        if col_name not in existing:
            c.execute(f"ALTER TABLE download_trials ADD COLUMN {col_name} {col_type}")
            result["added_columns"].append(col_name)

    # Backfill: downloaded=1 -> PASSED
    c.execute(
        "UPDATE download_trials SET status_code = '.', status_name = 'PASSED' "
        "WHERE status_code IS NULL AND downloaded = '1'"
    )
    result["backfilled_passed"] = c.rowcount

    # Backfill: downloaded=0 -> FAILED
    c.execute(
        "UPDATE download_trials SET status_code = 'F', status_name = 'FAILED' "
        "WHERE status_code IS NULL AND downloaded = '0'"
    )
    result["backfilled_failed"] = c.rowcount

    conn.commit()
    conn.close()
    return result


def main() -> None:
    """Entry point for migration script."""
    cache_root = os.environ.get("BRASA_DATA_PATH")

    if not cache_root:
        raise ValueError(
            "BRASA_DATA_PATH environment variable not set. Please set it to the cache root directory."
        )

    db_path = str(Path(cache_root) / "meta" / "meta.db")

    if not Path(db_path).exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    print(f"Migrating: {db_path}")
    result = migrate_download_trials(db_path)

    if result["added_columns"]:
        print(f"  Added columns: {', '.join(result['added_columns'])}")
    else:
        print("  All columns already present (idempotent).")

    total = result["backfilled_passed"] + result["backfilled_failed"]
    if total:
        print(
            f"  Backfilled {total} rows "
            f"({result['backfilled_passed']} passed, "
            f"{result['backfilled_failed']} failed)"
        )
    else:
        print("  No legacy rows to backfill.")

    print("Migration complete.")


if __name__ == "__main__":
    main()
