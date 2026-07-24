"""Tests for brasa.engine.config data-path resolution."""

from pathlib import Path

import pytest

from brasa.engine.config import (
    default_data_path,
    load_config,
    resolve_data_path,
    save_data_path,
)
from brasa.engine.exceptions import BrasaNotConfiguredError


@pytest.fixture()
def isolated_config(tmp_path, monkeypatch):
    """Point the config file at a temp location and clear the env var."""
    cfg = tmp_path / "config.toml"
    monkeypatch.setattr("brasa.engine.config.config_file_path", lambda: cfg)
    monkeypatch.delenv("BRASA_DATA_PATH", raising=False)
    return cfg


def test_env_var_takes_precedence(isolated_config, monkeypatch):
    save_data_path("/from/config")
    monkeypatch.setenv("BRASA_DATA_PATH", "/from/env")
    assert resolve_data_path() == "/from/env"


def test_config_file_used_when_no_env(isolated_config):
    save_data_path("/from/config")
    assert resolve_data_path() == "/from/config"


def test_unconfigured_raises(isolated_config):
    with pytest.raises(BrasaNotConfiguredError):
        resolve_data_path()


def test_save_and_load_roundtrip(isolated_config):
    save_data_path("/some/path with spaces")
    assert load_config() == {"data_path": "/some/path with spaces"}


def test_load_config_missing_file_returns_empty(isolated_config):
    assert load_config() == {}


def test_default_data_path_is_absolute():
    assert Path(default_data_path()).is_absolute()
