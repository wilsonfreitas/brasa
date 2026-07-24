"""Tests for the `brasa init` command handler."""

import sys
from types import SimpleNamespace

import pytest

from brasa.cli import _cmd_init, parser


@pytest.fixture()
def isolated_config(tmp_path, monkeypatch):
    cfg_file = tmp_path / "cfg" / "config.toml"
    monkeypatch.setattr("brasa.engine.config.config_file_path", lambda: cfg_file)
    monkeypatch.setattr(
        "brasa.engine.config.default_data_path",
        lambda: str(tmp_path / "default-home"),
    )
    monkeypatch.delenv("BRASA_DATA_PATH", raising=False)
    return cfg_file


def make_args(data_path=None, yes=False):
    return SimpleNamespace(data_path=data_path, yes=yes)


def read_config(cfg_file):
    import tomllib

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
