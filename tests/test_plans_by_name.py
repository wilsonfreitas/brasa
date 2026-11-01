"""Download plans by name from template folders, and list-plans (WIL-202)."""

import os
from pathlib import Path

import pytest

from brasa.engine.download_plan import load_plan
from brasa.engine.exceptions import TemplateFolderError
from brasa.engine.template import clear_template_cache, list_plan_sources

_PLAN = """\
name: {name}
description: {description}
tasks:
  - template: b3-bvbg086
"""


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("BRASA_TEMPLATE_PATH", raising=False)
    clear_template_cache()
    yield
    clear_template_cache()


def _plan(root: Path, stem: str, name: str | None = None, description="a plan"):
    plans = root / "plans"
    plans.mkdir(parents=True, exist_ok=True)
    path = plans / f"{stem}.yaml"
    path.write_text(_PLAN.format(name=name or stem, description=description))
    return path


def _env(monkeypatch, *roots: Path) -> None:
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", os.pathsep.join(map(str, roots)))


def test_plan_found_by_name(tmp_path, monkeypatch):
    _plan(tmp_path / "ws", "daily")
    _env(monkeypatch, tmp_path / "ws")
    plan = load_plan("daily")
    assert plan.name == "daily"
    assert [t.template for t in plan.tasks] == ["b3-bvbg086"]


def test_first_folder_wins_for_plans(tmp_path, monkeypatch):
    _plan(tmp_path / "mine", "daily", description="my recipe")
    _plan(tmp_path / "official", "daily", description="official recipe")
    _env(monkeypatch, tmp_path / "mine", tmp_path / "official")
    assert load_plan("daily").description == "my recipe"
    entry = next(e for e in list_plan_sources() if e.name == "daily")
    assert entry.source.resolve() == (tmp_path / "mine").resolve()
    assert entry.shadows is True


def test_path_still_works(tmp_path):
    path = tmp_path / "anywhere.yaml"
    path.write_text(_PLAN.format(name="whatever", description="x"))
    assert load_plan(str(path)).name == "whatever"


def test_yaml_suffix_means_path(tmp_path, monkeypatch):
    _plan(tmp_path / "ws", "daily")
    _env(monkeypatch, tmp_path / "ws")
    monkeypatch.chdir(tmp_path)  # no ./daily.yaml here
    with pytest.raises(FileNotFoundError):
        load_plan("daily.yaml")


def test_name_mismatch_is_error(tmp_path, monkeypatch):
    path = _plan(tmp_path / "ws", "daily", name="other")
    _env(monkeypatch, tmp_path / "ws")
    with pytest.raises(ValueError, match="must match the file name 'daily'") as exc:
        load_plan("daily")
    assert str(path) in str(exc.value)


def test_unknown_plan_name_lists_available(tmp_path, monkeypatch):
    _plan(tmp_path / "ws", "daily")
    _env(monkeypatch, tmp_path / "ws")
    with pytest.raises(ValueError, match="Download plan 'nope' not found") as exc:
        load_plan("nope")
    assert "daily" in str(exc.value)


def test_plan_folder_version_mismatch_is_error(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    _plan(root, "daily")
    (root / "brasa.toml").write_text('requires-brasa = ">=99"\n')
    _env(monkeypatch, root)
    with pytest.raises(TemplateFolderError, match="requires brasa >=99"):
        load_plan("daily")
