"""Migrate b3-listed-*-etfs cache metadata to the consolidated b3-listed-funds template.

Before:
  cache_metadata rows with template IN (
    'b3-listed-stock-etfs',
    'b3-listed-cripto-etfs',
    'b3-listed-fixed-income-etfs',
    'b3-listed-reits'
  )
  raw files under raw/<old_template>/<download_checksum>/...
  input parquet under db/input/<old_template>/...
  staging parquet under db/staging/b3-listed-funds/...

After:
  cache_metadata rows rewritten with:
    template        = 'b3-listed-funds'
    id              = md5(('b3-listed-funds', sorted_args_with_typeFund, extra_key))
    download_args   = JSON including the injected typeFund
    downloaded_files= paths rewritten under raw/b3-listed-funds/...
    processed_files = 'false' (forces re-process of input dataset)
  raw files moved to raw/b3-listed-funds/<download_checksum>/...
  old input + staging parquet trees removed.

Safety:
  - meta.db backed up before any change.
  - raw/<old_tpl> trees tar-gzipped into meta/ before move.
  - pre-flight: aborts if any new_id would collide with an unrelated existing row.
  - idempotent: partial prior runs are resumed; a clean final state is a no-op
    (still creates a fresh backup).
  - post-flight: verifies zero old-template rows remain and row counts match.

Usage:
    uv run python scripts/migrate_listed_etfs.py [--dry-run]

Honors BRASA_DATA_PATH.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path

from brasa.util import DownloadArgs, generate_checksum_for_template

OLD_TEMPLATES = (
    "b3-listed-stock-etfs",
    "b3-listed-cripto-etfs",
    "b3-listed-fixed-income-etfs",
    "b3-listed-reits",
)
NEW_TEMPLATE = "b3-listed-funds"

TEMPLATE_TO_TYPE_FUND = {
    "b3-listed-stock-etfs": "ETF",
    "b3-listed-cripto-etfs": "ETF-CRIPTO",
    "b3-listed-fixed-income-etfs": "ETF-RF",
    "b3-listed-reits": "ETF-FII",
}


def _cache_root() -> Path:
    return Path(os.environ.get("BRASA_DATA_PATH", str(Path.cwd() / ".brasa-cache")))


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def _backup_meta_db(meta_db: Path, ts: str) -> Path:
    backup = meta_db.with_name(f"{meta_db.name}.bak-{ts}-migrate-listed-etfs")
    shutil.copy2(meta_db, backup)
    return backup


def _backup_raw_dirs(cache_root: Path, ts: str) -> Path | None:
    """Tar-gzip the old raw/<tpl> trees before we move anything. Returns archive path, or None if nothing to back up."""
    to_back = [
        cache_root / "raw" / tpl
        for tpl in OLD_TEMPLATES
        if (cache_root / "raw" / tpl).exists()
    ]
    if not to_back:
        return None
    meta_dir = cache_root / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    archive = meta_dir / f"raw-old-etfs.bak-{ts}.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for p in to_back:
            tar.add(p, arcname=str(p.relative_to(cache_root)))
    return archive


def _rewrite_downloaded_files(files: list[str]) -> list[str]:
    out = []
    for f in files:
        rewritten = f
        for tpl in OLD_TEMPLATES:
            rewritten = rewritten.replace(f"raw/{tpl}/", f"raw/{NEW_TEMPLATE}/")
        out.append(rewritten)
    return out


def _move_raw(cache_root: Path, old_tpl: str, checksum: str) -> str:
    """Move raw/<old_tpl>/<checksum> -> raw/<NEW>/<checksum>.

    Returns one of: 'moved', 'already-at-dst', 'missing', 'duplicate-dropped'.
    """
    src = cache_root / "raw" / old_tpl / checksum
    dst = cache_root / "raw" / NEW_TEMPLATE / checksum
    if dst.exists() and src.exists():
        shutil.rmtree(src)
        return "duplicate-dropped"
    if dst.exists():
        return "already-at-dst"
    if not src.exists():
        return "missing"
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
    return "moved"


def _delete_tree(path: Path) -> bool:
    if path.exists():
        shutil.rmtree(path)
        return True
    return False


def main(argv: list[str] | None = None) -> int:  # noqa: PLR0911, PLR0912, PLR0915
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan and validate only; make no changes.",
    )
    args_ns = parser.parse_args(argv)
    dry_run = args_ns.dry_run

    cache_root = _cache_root()
    meta_db = cache_root / "meta" / "meta.db"
    if not meta_db.exists():
        print(f"meta.db not found at {meta_db}; nothing to migrate.", file=sys.stderr)
        return 1

    ts = _timestamp()

    # ---- Phase 1: read + plan (always, even in dry-run) ----
    with sqlite3.connect(meta_db) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        placeholders = ",".join("?" for _ in OLD_TEMPLATES)
        c.execute(
            "SELECT id, download_checksum, download_args, template, "
            "downloaded_files, extra_key FROM cache_metadata "
            "WHERE template IN (" + placeholders + ")",
            OLD_TEMPLATES,
        )
        old_rows = [dict(r) for r in c.fetchall()]

        # Existing new-template rows (for collision detection and idempotency).
        c.execute(
            "SELECT id FROM cache_metadata WHERE template = ?",
            (NEW_TEMPLATE,),
        )
        existing_new_ids = {r["id"] for r in c.fetchall()}

    # Build plan.
    plan = []  # list of dicts with everything needed for execute phase
    seen_new_ids: dict[str, str] = {}  # new_id -> old_id (detect intra-batch dup)
    collisions: list[str] = []

    for row in old_rows:
        old_tpl = row["template"]
        args_dict = dict(DownloadArgs.from_json(row["download_args"]).items())
        args_dict["typeFund"] = TEMPLATE_TO_TYPE_FUND[old_tpl]
        new_args = DownloadArgs(args_dict)
        new_id = generate_checksum_for_template(
            NEW_TEMPLATE, new_args, row["extra_key"] or ""
        )
        new_args_json = new_args.to_json()

        files = json.loads(row["downloaded_files"]) if row["downloaded_files"] else []
        new_files = _rewrite_downloaded_files(files)

        # Collision checks.
        if new_id in seen_new_ids and seen_new_ids[new_id] != row["id"]:
            collisions.append(
                f"intra-batch: old_ids {seen_new_ids[new_id]} and {row['id']} "
                f"both map to new_id {new_id}"
            )
        elif new_id in existing_new_ids and new_id != row["id"]:
            # Either a prior partial run (ok: delete old row, keep existing new),
            # or an unrelated pre-existing row (abort).
            # We can't distinguish here; flag it and let user inspect.
            collisions.append(
                f"new_id {new_id} (from old_id {row['id']}) already exists under "
                f"template={NEW_TEMPLATE}; manual inspection required"
            )
        seen_new_ids[new_id] = row["id"]

        plan.append(
            {
                "old_id": row["id"],
                "new_id": new_id,
                "checksum": row["download_checksum"],
                "old_tpl": old_tpl,
                "new_args_json": new_args_json,
                "new_files_json": json.dumps(new_files),
            }
        )

    # Report plan.
    print(f"cache root:  {cache_root}")
    print(f"meta.db:     {meta_db}")
    print(f"rows to migrate: {len(plan)}")
    if collisions:
        print("COLLISIONS DETECTED — aborting:", file=sys.stderr)
        for c_msg in collisions:
            print(f"  - {c_msg}", file=sys.stderr)
        return 2

    if dry_run:
        for p in plan[:10]:
            print(
                f"  {p['old_tpl']:<32} {p['old_id']} -> {p['new_id']}  "
                f"checksum={p['checksum']}"
            )
        if len(plan) > 10:
            print(f"  ... and {len(plan) - 10} more")
        print("DRY RUN — no changes made.")
        return 0

    if not plan:
        print("no old-template rows found; creating backup and exiting (no-op).")
        _backup_meta_db(meta_db, ts)
        return 0

    # ---- Phase 2: backups ----
    db_backup = _backup_meta_db(meta_db, ts)
    print(f"meta.db backup: {db_backup}")
    raw_backup = _backup_raw_dirs(cache_root, ts)
    if raw_backup:
        print(f"raw backup:     {raw_backup}")

    # ---- Phase 3: move raw files ----
    move_stats = {"moved": 0, "already-at-dst": 0, "missing": 0, "duplicate-dropped": 0}
    for p in plan:
        result = _move_raw(cache_root, p["old_tpl"], p["checksum"])
        move_stats[result] += 1
    print(f"raw moves: {move_stats}")

    # ---- Phase 4: update DB rows (transactional) ----
    rewritten_rows = 0
    deleted_as_dup = 0
    with sqlite3.connect(meta_db) as conn:
        c = conn.cursor()
        try:
            for p in plan:
                # Idempotency: if new_id already exists and is different from old_id,
                # a prior run already migrated this row — just drop the stale old row.
                c.execute(
                    "SELECT 1 FROM cache_metadata WHERE id = ? AND template = ?",
                    (p["new_id"], NEW_TEMPLATE),
                )
                if c.fetchone() and p["new_id"] != p["old_id"]:
                    c.execute("DELETE FROM cache_metadata WHERE id = ?", (p["old_id"],))
                    deleted_as_dup += 1
                    continue

                c.execute(
                    "UPDATE cache_metadata SET "
                    "id = ?, template = ?, download_args = ?, "
                    "downloaded_files = ?, processed_files = 'false' "
                    "WHERE id = ?",
                    (
                        p["new_id"],
                        NEW_TEMPLATE,
                        p["new_args_json"],
                        p["new_files_json"],
                        p["old_id"],
                    ),
                )
                rewritten_rows += 1
            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
            print(f"DB update failed; rolled back: {e}", file=sys.stderr)
            print(
                "NOTE: raw files may already be under the new location. "
                f"Restore from {db_backup} and {raw_backup} if needed.",
                file=sys.stderr,
            )
            return 3

    # ---- Phase 5: delete old parquet trees ----
    deleted_parquet = 0
    for tpl in OLD_TEMPLATES:
        if _delete_tree(cache_root / "db" / "input" / tpl):
            deleted_parquet += 1
    if _delete_tree(cache_root / "db" / "staging" / NEW_TEMPLATE):
        deleted_parquet += 1

    # Remove now-empty old raw/<tpl> parent dirs.
    empty_raw_dirs = 0
    for tpl in OLD_TEMPLATES:
        p = cache_root / "raw" / tpl
        if p.exists() and not any(p.iterdir()):
            p.rmdir()
            empty_raw_dirs += 1

    # ---- Phase 6: post-flight verification ----
    with sqlite3.connect(meta_db) as conn:
        c = conn.cursor()
        c.execute(
            "SELECT COUNT(*) FROM cache_metadata WHERE template IN ("
            + placeholders
            + ")",
            OLD_TEMPLATES,
        )
        remaining_old = c.fetchone()[0]
        c.execute(
            "SELECT COUNT(*) FROM cache_metadata WHERE template = ?", (NEW_TEMPLATE,)
        )
        total_new = c.fetchone()[0]

    print(
        f"migrated rows: {rewritten_rows}; "
        f"dropped-as-duplicate rows: {deleted_as_dup}; "
        f"parquet trees removed: {deleted_parquet}; "
        f"empty old raw dirs removed: {empty_raw_dirs}"
    )
    print(f"post-flight: old-template rows remaining = {remaining_old}")
    print(f"post-flight: {NEW_TEMPLATE} rows total    = {total_new}")

    if remaining_old != 0:
        print("VERIFICATION FAILED: old-template rows still present.", file=sys.stderr)
        return 4
    expected_new = len(existing_new_ids) + rewritten_rows
    if total_new != expected_new:
        print(
            f"VERIFICATION FAILED: expected {expected_new} {NEW_TEMPLATE} rows, "
            f"got {total_new}.",
            file=sys.stderr,
        )
        return 5
    return 0


if __name__ == "__main__":
    sys.exit(main())
