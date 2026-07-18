"""Tests for user-defined template discovery via BRASA_TEMPLATE_PATH."""

from pathlib import Path

import pytest

from brasa.engine.template import (
    clear_template_cache,
    list_template_sources,
    list_templates,
    retrieve_template,
)

_ETL = """id: {name}
description: test user template
etl:
  pipeline:
    - step: sql_query
      datasets:
        - input.b3-bvbg086
      query: SELECT 1
"""


def _write_template(directory: Path, name: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.yaml"
    path.write_text(_ETL.format(name=name))
    return path


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_template_cache()
    yield
    clear_template_cache()


def test_user_template_discovered_and_loads(tmp_path, monkeypatch):
    _write_template(tmp_path, "my-user-tpl")
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", str(tmp_path))
    clear_template_cache()
    assert "my-user-tpl" in list_templates()
    tmpl = retrieve_template("my-user-tpl")
    assert tmpl.id == "my-user-tpl"


def test_user_template_shadows_bundled(tmp_path, monkeypatch):
    # 'b3-futures' is a bundled template; a user file with the same name wins.
    _write_template(tmp_path, "b3-futures")
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", str(tmp_path))
    clear_template_cache()
    tmpl = retrieve_template("b3-futures")
    assert tmpl.description == "test user template"  # the user file, not bundled
    entry = next(e for e in list_template_sources() if e.name == "b3-futures")
    assert entry.shadows is True
    assert entry.source.resolve() == tmp_path.resolve()


def test_nonexistent_root_skipped(tmp_path, monkeypatch):
    _write_template(tmp_path, "my-user-tpl")
    missing = tmp_path / "does-not-exist"
    import os

    monkeypatch.setenv(
        "BRASA_TEMPLATE_PATH", os.pathsep.join([str(missing), str(tmp_path)])
    )
    clear_template_cache()
    assert "my-user-tpl" in list_templates()  # valid root still resolves


def test_user_etl_enters_dependency_graph(tmp_path, monkeypatch):
    _write_template(tmp_path, "my-user-etl")
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", str(tmp_path))
    clear_template_cache()
    from brasa.engine.dependency_graph import TemplateDependencyGraph

    graph = TemplateDependencyGraph()
    assert "my-user-etl" in graph.templates


def test_unset_env_is_bundled_only(tmp_path, monkeypatch):
    monkeypatch.delenv("BRASA_TEMPLATE_PATH", raising=False)
    clear_template_cache()
    names = list_templates()
    assert "b3-futures" in names  # bundled still present
    assert "my-user-tpl" not in names  # nothing user-defined leaks in
    sources = list_template_sources()
    assert all(e.shadows is False for e in sources)  # no shadows without user roots
