"""Template folders: layout, folder list, brasa.toml version check (WIL-198)."""

import json
import logging
import os
from pathlib import Path

import pytest

from brasa.engine.exceptions import TemplateFolderError
from brasa.engine.resources import package_version
from brasa.engine.template import (
    TemplateFolder,
    clear_template_cache,
    get_template_folders,
    list_template_sources,
    list_templates,
)

_ETL = """id: {name}
description: test template
etl:
  pipeline:
    - step: sql_query
      datasets:
        - input.b3-bvbg086
      query: SELECT 1
"""


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("BRASA_TEMPLATE_PATH", raising=False)
    clear_template_cache()
    yield
    clear_template_cache()


def _template(path: Path) -> Path:
    """Write a minimal ETL template whose id is the file stem."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_ETL.format(name=path.stem))
    return path


def _manifest(root: Path, text: str) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "brasa.toml").write_text(text)


def _config(tmp_path: Path, template_path: str) -> None:
    # conftest points config_file_path() at tmp_path / "config.toml"
    (tmp_path / "config.toml").write_text(
        f"template_path = {json.dumps(template_path)}\n"
    )


def _env(monkeypatch, *roots: Path) -> None:
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", os.pathsep.join(map(str, roots)))


def _folder(root: Path) -> TemplateFolder:
    return next(f for f in get_template_folders() if f.root.resolve() == root.resolve())


def test_yaml_outside_templates_dir_is_ignored(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    _template(root / "templates" / "a.yaml")
    _template(root / "plans" / "daily.yaml")
    _template(root / ".github" / "workflows" / "ci.yaml")
    _template(root / "x.yaml")
    _env(monkeypatch, root)
    names = list_templates()
    assert "a" in names
    assert not {"daily", "ci", "x"} & set(names)


def test_flat_folder_warns_and_loads_nothing(tmp_path, monkeypatch, caplog):
    root = tmp_path / "old"
    _template(root / "old.yaml")
    _env(monkeypatch, root)
    with caplog.at_level(logging.WARNING, logger="brasa.engine.template"):
        names = list_templates()
    assert "old" not in names
    assert "has no templates/ subfolder" in caplog.text
    assert "move them into" in caplog.text


def test_folder_with_only_plans_does_not_warn(tmp_path, monkeypatch, caplog):
    root = tmp_path / "ws"
    (root / "plans").mkdir(parents=True)
    (root / "plans" / "p.yaml").write_text("name: p\ntasks: []\n")
    _env(monkeypatch, root)
    with caplog.at_level(logging.WARNING, logger="brasa.engine.template"):
        list_templates()
    assert "has no templates/" not in caplog.text


def test_duplicate_name_in_one_folder_is_error(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    first = _template(root / "templates" / "a" / "x.yaml")
    second = _template(root / "templates" / "b" / "x.yaml")
    _env(monkeypatch, root)
    with pytest.raises(TemplateFolderError, match="Duplicate template name 'x'") as exc:
        list_templates()
    assert str(first) in str(exc.value)
    assert str(second) in str(exc.value)


def test_env_folder_wins_over_config_folder(tmp_path, monkeypatch):
    env_root, cfg_root = tmp_path / "env", tmp_path / "cfg"
    _template(env_root / "templates" / "dup.yaml")
    _template(cfg_root / "templates" / "dup.yaml")
    _env(monkeypatch, env_root)
    _config(tmp_path, str(cfg_root))
    entry = next(e for e in list_template_sources() if e.name == "dup")
    assert entry.source.resolve() == env_root.resolve()
    assert entry.shadows is True


def test_config_template_path_is_read(tmp_path):
    root = tmp_path / "cfg"
    _template(root / "templates" / "from-config.yaml")
    _config(tmp_path, str(root))
    assert "from-config" in list_templates()


def test_config_template_path_expands_user(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    _template(tmp_path / "ws" / "templates" / "tilde-tpl.yaml")
    _config(tmp_path, "~/ws")
    assert "tilde-tpl" in list_templates()


def test_same_folder_in_env_and_config_loaded_once(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    _template(root / "templates" / "once.yaml")
    _env(monkeypatch, root)
    _config(tmp_path, str(root))
    assert list_templates().count("once") == 1
    roots = [f.root.resolve() for f in get_template_folders()]
    assert roots.count(root.resolve()) == 1


def test_legacy_in_folder_path_is_not_skipped(tmp_path, monkeypatch):
    root = tmp_path / "legacy" / "ws"
    _template(root / "templates" / "kept.yaml")
    _template(root / "templates" / "legacy" / "y.yaml")
    _env(monkeypatch, root)
    names = list_templates()
    assert "kept" in names
    assert "y" not in names


def test_manifest_absent_means_no_check(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    _template(root / "templates" / "t1.yaml")
    _env(monkeypatch, root)
    folder = _folder(root)
    assert folder.error is None
    assert folder.requires is None


def test_manifest_ok(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    _template(root / "templates" / "t1.yaml")
    _manifest(root, 'name = "my-ws"\nrequires-brasa = ">=0.0"\n')
    _env(monkeypatch, root)
    folder = _folder(root)
    assert folder.error is None
    assert folder.name == "my-ws"
    entry = next(e for e in list_template_sources() if e.name == "t1")
    assert entry.source_name == "my-ws"


def test_manifest_mismatch_is_error(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    _template(root / "templates" / "t1.yaml")
    _manifest(root, 'requires-brasa = ">=99"\n')
    _env(monkeypatch, root)
    folder = _folder(root)  # listing folders never raises
    assert "requires brasa >=99" in folder.error
    assert f"(installed: {package_version()})" in folder.error
    assert "pip install -U" in folder.error
    with pytest.raises(TemplateFolderError, match="requires brasa >=99"):
        list_templates()


def test_manifest_missing_key_is_error(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    _manifest(root, 'name = "x"\n')
    _env(monkeypatch, root)
    assert "brasa.toml: missing 'requires-brasa'" in _folder(root).error


def test_manifest_bad_specifier_is_error(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    _manifest(root, 'requires-brasa = "banana"\n')
    _env(monkeypatch, root)
    assert "invalid 'requires-brasa' value 'banana'" in _folder(root).error


def test_invalid_toml_manifest_is_error(tmp_path, monkeypatch):
    root = tmp_path / "ws"
    _manifest(root, "requires-brasa = \n")
    _env(monkeypatch, root)
    assert "brasa.toml: invalid TOML (" in _folder(root).error


def test_dev_version_satisfies_dev_lower_bound(tmp_path, monkeypatch):
    monkeypatch.setattr("brasa.engine.template.package_version", lambda: "0.5.0.dev0")
    root = tmp_path / "ws"
    _env(monkeypatch, root)
    _manifest(root, 'requires-brasa = ">=0.5.0.dev0,<0.6"\n')
    assert _folder(root).error is None
    # PEP 440: a dev release sorts below its final release.
    _manifest(root, 'requires-brasa = ">=0.5,<0.6"\n')
    assert "requires brasa >=0.5,<0.6" in _folder(root).error
