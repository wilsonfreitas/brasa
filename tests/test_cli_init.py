"""Tests for the `brasa init` command handler."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from brasa.cli import _cmd_init, parser
from tests.test_template_git import make_origin, push_change


@pytest.fixture()
def isolated_config(tmp_path, monkeypatch):
    cfg_file = tmp_path / "cfg" / "config.toml"
    monkeypatch.setattr("brasa.engine.config.config_file_path", lambda: cfg_file)
    monkeypatch.setattr(
        "brasa.engine.config.default_data_path",
        lambda: str(tmp_path / "default-home"),
    )
    monkeypatch.delenv("BRASA_DATA_PATH", raising=False)
    monkeypatch.delenv("BRASA_TEMPLATE_PATH", raising=False)
    # `brasa init` clones the official templates repo: keep it offline.
    origin = make_origin(tmp_path / "official")
    monkeypatch.setattr(
        "brasa.engine.template_git.OFFICIAL_TEMPLATES_URL", f"file://{origin}"
    )
    return cfg_file


def make_args(data_path=None, yes=False, templates=None):
    return SimpleNamespace(data_path=data_path, yes=yes, templates=templates)


def read_config(cfg_file):
    from brasa.engine.config import tomllib

    return tomllib.loads(cfg_file.read_text())


def test_init_with_data_path_flag(isolated_config, tmp_path, capsys):
    target = tmp_path / "custom-home"
    _cmd_init(make_args(data_path=str(target)))
    assert target.is_dir()
    assert read_config(isolated_config)["data_path"] == str(target.resolve())
    out = capsys.readouterr().out
    assert "Brasa home ready at" in out


def test_init_yes_accepts_default(isolated_config, tmp_path, capsys):
    _cmd_init(make_args(yes=True))
    default = tmp_path / "default-home"
    assert default.is_dir()
    assert read_config(isolated_config)["data_path"] == str(default.resolve())


def test_init_non_interactive_accepts_default(isolated_config, tmp_path, monkeypatch):
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    _cmd_init(make_args())
    assert (tmp_path / "default-home").is_dir()


def test_init_interactive_empty_input_accepts_default(
    isolated_config, tmp_path, monkeypatch
):
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt="": "")
    _cmd_init(make_args())
    assert (tmp_path / "default-home").is_dir()


def test_init_interactive_custom_path(isolated_config, tmp_path, monkeypatch):
    custom = tmp_path / "typed-home"
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda prompt="": str(custom))
    _cmd_init(make_args())
    assert custom.is_dir()
    assert read_config(isolated_config)["data_path"] == str(custom.resolve())


def test_init_reconfigure_suggests_persisted_value(
    isolated_config, tmp_path, monkeypatch
):
    first = tmp_path / "first-home"
    _cmd_init(make_args(data_path=str(first)))
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    _cmd_init(make_args())  # no flags: suggested default is the persisted value
    assert read_config(isolated_config)["data_path"] == str(first.resolve())


def test_init_warns_about_env_override(isolated_config, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("BRASA_DATA_PATH", "/env/override")
    _cmd_init(make_args(yes=True))
    assert "takes precedence" in capsys.readouterr().out


def test_setup_command_removed():
    with pytest.raises(SystemExit):
        parser.parse_args(["setup"])


# ---------------------------------------------------------------------------
# Template folder step (WIL-199)
# ---------------------------------------------------------------------------


def test_init_clones_official_repo_by_default(isolated_config, tmp_path, capsys):
    target = tmp_path / "home"
    _cmd_init(make_args(data_path=str(target)))
    clone = (target / "templates").resolve()
    assert (clone / ".git").is_dir()
    assert (clone / "templates" / "t1.yaml").is_file()
    assert read_config(isolated_config)["template_path"] == str(clone)
    assert "Templates: cloned file://" in capsys.readouterr().out


def test_init_with_templates_url(isolated_config, tmp_path):
    fork = make_origin(tmp_path / "fork")
    target = tmp_path / "home"
    _cmd_init(make_args(data_path=str(target), templates=f"file://{fork}"))
    clone = (target / "templates").resolve()
    assert read_config(isolated_config)["template_path"] == str(clone)


def test_init_with_templates_path(isolated_config, tmp_path):
    mine = tmp_path / "mine"
    mine.mkdir()
    _cmd_init(make_args(data_path=str(tmp_path / "home"), templates=str(mine)))
    assert read_config(isolated_config)["template_path"] == str(mine.resolve())
    assert not (tmp_path / "home" / "templates").exists()


def test_init_missing_path_errors(isolated_config, tmp_path, capsys):
    with pytest.raises(SystemExit) as exc:
        _cmd_init(
            make_args(data_path=str(tmp_path / "home"), templates=str(tmp_path / "x"))
        )
    assert exc.value.code == 1
    assert "Template folder not found" in capsys.readouterr().err
    assert "template_path" not in read_config(isolated_config)


def test_init_keeps_existing_template_path(isolated_config, tmp_path, capsys):
    mine = tmp_path / "mine"
    mine.mkdir()
    _cmd_init(make_args(data_path=str(tmp_path / "home"), templates=str(mine)))
    capsys.readouterr()
    _cmd_init(make_args(data_path=str(tmp_path / "home")))  # no --templates
    assert read_config(isolated_config)["template_path"] == str(mine.resolve())
    assert not (tmp_path / "home" / "templates").exists()
    assert f"Template folders kept: {mine.resolve()}" in capsys.readouterr().out


def test_init_replaces_template_path_with_flag(isolated_config, tmp_path, capsys):
    old, new = tmp_path / "old", tmp_path / "new"
    old.mkdir()
    new.mkdir()
    _cmd_init(make_args(data_path=str(tmp_path / "home"), templates=str(old)))
    _cmd_init(make_args(data_path=str(tmp_path / "home"), templates=str(new)))
    assert read_config(isolated_config)["template_path"] == str(new.resolve())
    assert f"{old.resolve()} -> {new.resolve()}" in capsys.readouterr().out


def test_update_templates_cli_reports(isolated_config, tmp_path, capsys):
    from brasa import cli
    from brasa.engine import template_git

    origin = template_git.OFFICIAL_TEMPLATES_URL.removeprefix("file://")
    _cmd_init(make_args(data_path=str(tmp_path / "home")))
    push_change(tmp_path, Path(origin), "id: t1\ndescription: new\n")
    capsys.readouterr()
    cli.main(["update-templates"])  # returns normally (exit 0)
    out = capsys.readouterr().out
    assert "updated " in out and " -> " in out


def test_info_shows_origin_and_commit(isolated_config, tmp_path, capsys):
    from brasa import cli
    from brasa.engine import template_git

    _cmd_init(make_args(data_path=str(tmp_path / "home")))
    capsys.readouterr()
    cli.main(["info"])
    out = capsys.readouterr().out
    assert f"origin {template_git.OFFICIAL_TEMPLATES_URL}" in out
    assert "commit " in out
