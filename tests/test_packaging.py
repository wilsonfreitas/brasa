"""Tests that SQL DDL ships inside the brasa package, and templates do not."""

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


def test_package_ships_no_templates():
    # Since 0.5.0 templates come only from template folders (`brasa init`).
    assert not package_path("templates").exists()
    assert not any(_brasa_root().rglob("*.yaml"))


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


def test_package_version_is_pep440_dev_version():
    from packaging.version import Version

    from brasa.engine.resources import package_version

    v = Version(package_version())
    assert v.base_version == "0.5.0"
    assert v.is_devrelease
