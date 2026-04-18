"""Tests for scripts/migrate_listed_etfs.py.

Builds a fixture meta.db with rows under the four old ETF template ids,
stubs matching raw directories and parquet directories, runs the
migration script in-process, and asserts:

* meta.db backup is created
* rows rewritten: template + id + processed_files
* raw directories moved to raw/b3-listed-funds/<checksum>/
* old input parquet trees removed
* old staging parquet tree removed
* re-run is a no-op (rows and raw already migrated)
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from brasa.util import DownloadArgs, generate_checksum_for_template

OLD_TEMPLATES = [
    "b3-listed-stock-etfs",
    "b3-listed-cripto-etfs",
    "b3-listed-fixed-income-etfs",
    "b3-listed-reits",
]


def _old_type_for(template: str) -> str:
    return {
        "b3-listed-stock-etfs": "ETF",
        "b3-listed-cripto-etfs": "ETF-CRIPTO",
        "b3-listed-fixed-income-etfs": "ETF-RF",
        "b3-listed-reits": "ETF-FII",
    }[template]


def _insert_row(conn, template: str, refdate: str, checksum: str) -> tuple[str, str]:
    """Insert a fixture row and return (old_id, new_id_expected)."""
    args = {
        "language": "pt-br",
        "typeFund": _old_type_for(template),
        "pageNumber": 1,
        "pageSize": 999,
    }
    da = DownloadArgs(args)
    old_id = generate_checksum_for_template(template, da, refdate)
    new_id = generate_checksum_for_template("b3-listed-funds", da, refdate)
    conn.execute(
        "INSERT INTO cache_metadata VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            old_id,
            checksum,
            "2025-01-02T00:00:00",
            json.dumps({}),
            da.to_json(),
            template,
            json.dumps([f"raw/{template}/{checksum}/data.json"]),
            "true",
            refdate,
            "",
            "0",
            "",
        ),
    )
    conn.commit()
    return old_id, new_id


@pytest.fixture
def fake_cache(tmp_path, monkeypatch):
    """Build a minimal cache tree with meta.db, raw stubs, parquet stubs."""
    monkeypatch.setenv("BRASA_DATA_PATH", str(tmp_path))
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()
    meta_db = meta_dir / "meta.db"
    sql_path = Path(__file__).parent.parent / "sql" / "create-meta-db.sql"
    with sqlite3.connect(meta_db) as conn:
        conn.executescript(sql_path.read_text())

    raw_root = tmp_path / "raw"
    db_root = tmp_path / "db"
    rows = {}
    for i, tpl in enumerate(OLD_TEMPLATES):
        checksum = f"chk{i:02d}"
        refdate = "2025-01-02"
        with sqlite3.connect(meta_db) as conn:
            old_id, new_id = _insert_row(conn, tpl, refdate, checksum)
        rows[tpl] = (old_id, new_id, checksum)

        raw_dir = raw_root / tpl / checksum
        raw_dir.mkdir(parents=True)
        (raw_dir / "data.json").write_text("[]")

        parquet_dir = db_root / "input" / tpl / f"refdate={refdate}"
        parquet_dir.mkdir(parents=True)
        (parquet_dir / "part-0.parquet").write_bytes(b"stub")

    staging_dir = db_root / "staging" / "b3-listed-funds" / "refdate=2025-01-02"
    staging_dir.mkdir(parents=True)
    (staging_dir / "part-0.parquet").write_bytes(b"stub")

    return {"cache": tmp_path, "meta_db": meta_db, "rows": rows}


def _run_script(cache_path: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "scripts/migrate_listed_etfs.py"],
        env={**__import__("os").environ, "BRASA_DATA_PATH": str(cache_path)},
        capture_output=True,
        text=True,
        check=False,
    )


def test_migration_happy_path(fake_cache):
    cache = fake_cache["cache"]
    meta_db = fake_cache["meta_db"]
    rows = fake_cache["rows"]

    proc = _run_script(cache)
    assert proc.returncode == 0, proc.stderr

    backups = list((cache / "meta").glob("meta.db.bak-*-migrate-listed-etfs"))
    assert len(backups) == 1, f"expected one backup, got {backups}"

    with sqlite3.connect(meta_db) as conn:
        c = conn.cursor()
        c.execute(
            "SELECT COUNT(*) FROM cache_metadata WHERE template IN (?, ?, ?, ?)",
            tuple(OLD_TEMPLATES),
        )
        assert c.fetchone()[0] == 0, "old-template rows remain"

        c.execute(
            "SELECT id, template, processed_files FROM cache_metadata "
            "WHERE template = 'b3-listed-funds'"
        )
        migrated = c.fetchall()
        assert len(migrated) == len(OLD_TEMPLATES)
        for row_id, tpl, processed in migrated:
            assert tpl == "b3-listed-funds"
            assert processed == "false"
            assert row_id in {new for (_, new, _) in rows.values()}

    for tpl, (_, _, checksum) in rows.items():
        assert not (cache / "raw" / tpl / checksum).exists(), f"{tpl} raw not moved"
        assert (cache / "raw" / "b3-listed-funds" / checksum / "data.json").exists()

    for tpl in OLD_TEMPLATES:
        assert not (cache / "db" / "input" / tpl).exists(), f"{tpl} parquet not removed"
    assert not (cache / "db" / "staging" / "b3-listed-funds").exists()


def test_migration_is_idempotent(fake_cache):
    import time

    cache = fake_cache["cache"]

    first = _run_script(cache)
    assert first.returncode == 0, first.stderr

    time.sleep(1.1)  # Ensure different second for backup timestamp

    second = _run_script(cache)
    assert second.returncode == 0, second.stderr

    backups = sorted((cache / "meta").glob("meta.db.bak-*-migrate-listed-etfs"))
    assert len(backups) == 2, "expected a new backup on every run"

    with sqlite3.connect(fake_cache["meta_db"]) as conn:
        c = conn.cursor()
        c.execute(
            "SELECT COUNT(*) FROM cache_metadata WHERE template = 'b3-listed-funds'"
        )
        assert c.fetchone()[0] == len(OLD_TEMPLATES)
