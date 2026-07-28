"""Tests that package data (templates, SQL DDL) ships inside the brasa package."""

from pathlib import Path

import brasa
from brasa.engine.resources import package_path
from brasa.engine.template import list_templates


def _brasa_root() -> Path:
    return Path(brasa.__file__).resolve().parent


def test_package_path_is_under_brasa_package():
    sql = package_path("sql", "create-meta-db.sql")
    assert sql.exists()
    assert _brasa_root() in sql.resolve().parents


def test_templates_dir_under_package_with_yaml():
    tdir = package_path("templates")
    assert tdir.exists()
    assert _brasa_root() in tdir.resolve().parents
    assert any(tdir.rglob("*.yaml"))


def test_list_templates_excludes_legacy():
    names = list_templates()
    assert len(names) > 0
    # Guard (kept as a feature): any template path containing a `legacy`
    # component is never discovered nor packaged — the folder parks
    # WIP/retired templates. `b3-companies-details` was the last legacy-only
    # template name; it must never resurface.
    assert "b3-companies-details" not in names


def test_sql_ddl_files_resolve():
    assert package_path("sql", "create-meta-db.sql").exists()
    assert package_path("sql", "create-dataset-catalog.sql").exists()
