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
  cache_metadata rows rewritten with template = 'b3-listed-funds',
  new id = md5(('b3-listed-funds', sorted_args, extra_key)),
  processed_files = 'false' so next process_marketdata regenerates
    the input parquet dataset.
  raw files moved to raw/b3-listed-funds/<download_checksum>/...
  old input + staging parquet directories removed.
  meta.db backed up to meta.db.bak-<UTC_TS>-migrate-listed-etfs.

Idempotent: re-running after completion finds zero old rows and logs
a no-op (while still creating a fresh backup).

Usage:
    uv run python scripts/migrate_listed_etfs.py

Honors BRASA_DATA_PATH.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
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


def _cache_root() -> Path:
    return Path(os.environ.get("BRASA_DATA_PATH", str(Path.cwd() / ".brasa-cache")))


def _backup_meta_db(meta_db: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    backup = meta_db.with_name(f"{meta_db.name}.bak-{ts}-migrate-listed-etfs")
    shutil.copy2(meta_db, backup)
    return backup


def _rewrite_downloaded_files(files: list[str]) -> list[str]:
    out = []
    for f in files:
        rewritten = f
        for tpl in OLD_TEMPLATES:
            rewritten = rewritten.replace(f"raw/{tpl}/", f"raw/{NEW_TEMPLATE}/")
            rewritten = rewritten.replace(f"{tpl}/", f"{NEW_TEMPLATE}/")
        out.append(rewritten)
    return out


def _move_raw(cache_root: Path, old_tpl: str, checksum: str) -> bool:
    src = cache_root / "raw" / old_tpl / checksum
    dst = cache_root / "raw" / NEW_TEMPLATE / checksum
    if dst.exists():
        if src.exists():
            shutil.rmtree(src)
        return False
    if not src.exists():
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
    return True


def _delete_tree(path: Path) -> bool:
    if path.exists():
        shutil.rmtree(path)
        return True
    return False


def main() -> int:
    cache_root = _cache_root()
    meta_db = cache_root / "meta" / "meta.db"
    if not meta_db.exists():
        print(
            f"meta.db not found at {meta_db}; nothing to migrate.",
            file=sys.stderr,
        )
        return 1

    backup = _backup_meta_db(meta_db)
    print(f"backup: {backup}")

    moved_raw_dirs = 0
    rewritten_rows = 0

    with sqlite3.connect(meta_db) as conn:
        c = conn.cursor()
        placeholders = ",".join("?" for _ in OLD_TEMPLATES)
        c.execute(
            "SELECT id, download_checksum, download_args, template, downloaded_files, "
            "extra_key FROM cache_metadata WHERE template IN (" + placeholders + ")",
            OLD_TEMPLATES,
        )
        rows = c.fetchall()

        for (
            old_id,
            checksum,
            args_json,
            old_tpl,
            files_json,
            extra_key,
        ) in rows:
            args = DownloadArgs.from_json(args_json)
            new_id = generate_checksum_for_template(NEW_TEMPLATE, args, extra_key or "")

            if _move_raw(cache_root, old_tpl, checksum):
                moved_raw_dirs += 1

            files = json.loads(files_json) if files_json else []
            new_files = _rewrite_downloaded_files(files)

            c.execute(
                "UPDATE cache_metadata "
                "SET id = ?, template = ?, downloaded_files = ?, processed_files = 'false' "
                "WHERE id = ?",
                (new_id, NEW_TEMPLATE, json.dumps(new_files), old_id),
            )
            rewritten_rows += 1

        conn.commit()

    deleted_parquet = 0
    for tpl in OLD_TEMPLATES:
        if _delete_tree(cache_root / "db" / "input" / tpl):
            deleted_parquet += 1

    if _delete_tree(cache_root / "db" / "staging" / "b3-listed-funds"):
        deleted_parquet += 1

    empty_raw_dirs = 0
    for tpl in OLD_TEMPLATES:
        p = cache_root / "raw" / tpl
        if p.exists() and not any(p.iterdir()):
            p.rmdir()
            empty_raw_dirs += 1

    print(
        f"migrated rows: {rewritten_rows}; "
        f"raw dirs moved: {moved_raw_dirs}; "
        f"parquet trees removed: {deleted_parquet}; "
        f"empty old raw dirs removed: {empty_raw_dirs}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
