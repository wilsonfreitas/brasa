#!/usr/bin/env python
"""Migration script: add FK + CASCADE between download_trials and cache_metadata.

Steps:
1. Wipe pre-existing orphan download_trials rows.
2. Recreate download_trials with FOREIGN KEY(cache_id)
   REFERENCES cache_metadata(id) ON DELETE CASCADE.
3. Validate via PRAGMA foreign_key_check.

Idempotent: detects an existing FK via PRAGMA foreign_key_list and skips
the recreate step on subsequent runs.

Usage:
    uv run python scripts/migrate_meta_db_fk.py [CACHE_PATH]

If CACHE_PATH is omitted, uses $BRASA_DATA_PATH or ./.brasa-cache.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

_DOWNLOAD_TRIALS_NEW_SQL = """
CREATE TABLE download_trials_new (
    cache_id TEXT,
    timestamp TEXT,
    downloaded TEXT,
    status_code TEXT,
    status_name TEXT,
    reason TEXT,
    http_status INTEGER,
    FOREIGN KEY(cache_id) REFERENCES cache_metadata(id) ON DELETE CASCADE
)
"""


def _has_fk(conn: sqlite3.Connection) -> bool:
    rows = conn.execute("PRAGMA foreign_key_list(download_trials)").fetchall()
    return any(r[2] == "cache_metadata" and r[3] == "cache_id" for r in rows)


def migrate(db_path: str) -> dict:
    """Run the FK migration on a single meta.db.

    Args:
        db_path: Path to the meta.db file.

    Returns:
        Dict with keys: orphans_removed (int), fk_added (bool).
    """
    result = {"orphans_removed": 0, "fk_added": False}

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = OFF")

        # 1. Wipe orphans (always — cheap, safe).
        cur = conn.execute(
            "DELETE FROM download_trials "
            "WHERE cache_id NOT IN (SELECT id FROM cache_metadata)"
        )
        result["orphans_removed"] = cur.rowcount or 0

        # 2. Recreate with FK if not already present.
        if not _has_fk(conn):
            conn.execute(_DOWNLOAD_TRIALS_NEW_SQL)
            conn.execute(
                "INSERT INTO download_trials_new SELECT * FROM download_trials"
            )
            conn.execute("DROP TABLE download_trials")
            conn.execute("ALTER TABLE download_trials_new RENAME TO download_trials")
            result["fk_added"] = True

        # 3. Validate.
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise RuntimeError(f"foreign_key_check violations: {violations}")

        conn.commit()
    finally:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.close()

    return result


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
    if result["fk_added"]:
        print("  Added FK + CASCADE to download_trials.")
    else:
        print("  FK already present (idempotent).")
    print("Migration complete.")


if __name__ == "__main__":
    main()
