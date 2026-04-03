"""Migrate cache_metadata IDs to match DownloadArgs normalization.

After WIL-34 introduced DownloadArgs, the hash computation for
cache_metadata.id changed: old code pickled datetime objects while new
code pickles canonical strings.  This script recomputes all IDs and
updates both cache_metadata.id and download_trials.cache_id.

Usage:
    uv run python scripts/migrate_cache_ids.py            # dry-run (default)
    uv run python scripts/migrate_cache_ids.py --apply    # execute migration

No changes are made without --apply.
"""

import argparse
import json
import os
import shutil
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from brasa.util import DownloadArgs, generate_checksum_for_template


def _resolve_db_path() -> Path:
    """Resolve the path to the SQLite metadata database."""
    cache_path = os.environ.get("BRASA_DATA_PATH", str(Path.cwd() / ".brasa-cache"))
    return Path(cache_path) / "meta" / "meta.db"


def _compute_migration_map(
    conn: sqlite3.Connection,
) -> tuple[dict[str, str], dict[str, dict[str, int]]]:
    """Compute old_id -> new_id mapping for all rows that need migration.

    Returns:
        A tuple of (migration_map, per_template_stats).
        migration_map: {old_id: new_id} for rows where id would change.
        per_template_stats: {template: {"total": N, "migrated": N}}.
    """
    c = conn.cursor()
    c.execute("SELECT id, template, download_args, extra_key FROM cache_metadata")
    rows = c.fetchall()

    migration_map: dict[str, str] = {}
    stats: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "migrated": 0})

    for stored_id, template, download_args_json, extra_key in rows:
        stats[template]["total"] += 1

        try:
            download_args_dict = json.loads(download_args_json)
        except (json.JSONDecodeError, TypeError):
            continue

        normalized_args = DownloadArgs(download_args_dict)
        new_id = generate_checksum_for_template(template, normalized_args, extra_key)

        if stored_id != new_id:
            migration_map[stored_id] = new_id
            stats[template]["migrated"] += 1

    return migration_map, dict(stats)


def _check_collisions(migration_map: dict[str, str]) -> dict[str, list[str]]:
    """Check if any two old IDs map to the same new ID.

    Returns:
        Dict of {new_id: [old_id1, old_id2, ...]} for collisions.
        Empty dict means no collisions.
    """
    reverse: dict[str, list[str]] = defaultdict(list)
    for old_id, new_id in migration_map.items():
        reverse[new_id].append(old_id)
    return {new_id: old_ids for new_id, old_ids in reverse.items() if len(old_ids) > 1}


def _apply_migration(conn: sqlite3.Connection, migration_map: dict[str, str]) -> None:
    """Execute the migration within a single transaction.

    Updates cache_metadata.id and download_trials.cache_id for all
    entries in the migration map.
    """
    c = conn.cursor()
    for old_id, new_id in migration_map.items():
        c.execute("UPDATE cache_metadata SET id = ? WHERE id = ?", (new_id, old_id))
        c.execute(
            "UPDATE download_trials SET cache_id = ? WHERE cache_id = ?",
            (new_id, old_id),
        )
    conn.commit()


def _print_summary(
    stats: dict[str, dict[str, int]],
    migration_map: dict[str, str],
    collisions: dict[str, list[str]],
    applied: bool,
    backup_path: str | None,
) -> None:
    """Print the migration summary table."""
    total_rows = sum(s["total"] for s in stats.values())
    total_migrated = len(migration_map)

    print("\nMigrate cache_metadata IDs (DownloadArgs normalization)\n")
    print(f"{'Template':<40} {'Total':>8} {'Migrated':>10}")
    print("-" * 60)

    for template in sorted(stats.keys()):
        data = stats[template]
        if data["migrated"] > 0:
            print(f"{template:<40} {data['total']:>8} {data['migrated']:>10}")

    print("-" * 60)
    print(f"\nTotal rows:      {total_rows}")
    if total_rows > 0:
        print(
            f"To migrate:      {total_migrated}  "
            f"({100 * total_migrated / total_rows:.1f}%)"
        )
    else:
        print(f"To migrate:      {total_migrated}")
    print(f"Collisions:      {len(collisions)}")

    if collisions:
        print("\nCOLLISIONS DETECTED — migration aborted:")
        for new_id, old_ids in collisions.items():
            print(f"  {new_id} <- {old_ids}")

    if not applied:
        print("\n[DRY RUN] No changes made. Pass --apply to execute migration.")
    elif backup_path:
        print(f"\n[APPLIED] Migration complete. Backup at {backup_path}")


def migrate_cache_ids(apply: bool = False) -> None:
    """Main migration function."""
    db_path = _resolve_db_path()

    if not db_path.exists():
        print(f"Error: Cache metadata database not found at {db_path}")
        return

    conn = sqlite3.connect(str(db_path))
    try:
        migration_map, stats = _compute_migration_map(conn)

        if not migration_map:
            print("Nothing to migrate. All IDs are up to date.")
            conn.close()
            return

        collisions = _check_collisions(migration_map)

        backup_path = None
        if apply and not collisions:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            backup_path = str(db_path) + f".bak-{timestamp}"
            shutil.copy2(str(db_path), backup_path)
            _apply_migration(conn, migration_map)

        _print_summary(stats, migration_map, collisions, apply, backup_path)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate cache_metadata IDs to match DownloadArgs normalization."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute the migration. Without this flag, only a dry-run is performed.",
    )
    args = parser.parse_args()
    migrate_cache_ids(apply=args.apply)
