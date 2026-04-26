#!/usr/bin/env python
"""Migration script: wipe orphan download_trials rows from meta.db.

Removes download_trials rows whose cache_id has no matching entry in
cache_metadata. These accumulate from past remove_meta/clean_meta_db calls
that predated the atomic-delete fix.

Idempotent: safe to run multiple times; subsequent runs find nothing to delete.

Usage:
    uv run python scripts/migrate_meta_db_fk.py [CACHE_PATH]

If CACHE_PATH is omitted, uses $BRASA_DATA_PATH or ./.brasa-cache.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path


def migrate(db_path: str) -> dict:
    """Wipe orphan download_trials rows from a single meta.db.

    Args:
        db_path: Path to the meta.db file.

    Returns:
        Dict with key: orphans_removed (int).
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "DELETE FROM download_trials "
            "WHERE cache_id NOT IN (SELECT id FROM cache_metadata)"
        )
        orphans_removed = cur.rowcount or 0
        conn.commit()
    finally:
        conn.close()

    return {"orphans_removed": orphans_removed}


def main() -> None:
    """Entry point for migration script."""
    if len(sys.argv) > 1:
        cache_root = sys.argv[1]
    else:
        cache_root = os.environ.get("BRASA_DATA_PATH", str(Path.cwd() / ".brasa-cache"))

    db_path = str(Path(cache_root) / "meta" / "meta.db")
    if not Path(db_path).exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Migrating: {db_path}")
    result = migrate(db_path)
    print(f"  Orphan trial rows removed: {result['orphans_removed']}")
    print("Migration complete.")


if __name__ == "__main__":
    main()
