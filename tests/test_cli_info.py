from importlib import metadata

import pytest

from brasa import cli


def test_version_flag(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["--version"])
    assert exc.value.code == 0
    out = capsys.readouterr().out.strip()
    assert out == f"brasa {metadata.version('brasa-marketdata')}"


@pytest.fixture
def isolated_config(tmp_path, monkeypatch):
    """Point config_file_path at a tmp file and clear BRASA_DATA_PATH."""
    cfg_file = tmp_path / "config.toml"
    monkeypatch.setattr("brasa.engine.config.config_file_path", lambda: cfg_file)
    monkeypatch.delenv("BRASA_DATA_PATH", raising=False)
    return cfg_file


def test_info_configured_via_env_var(isolated_config, tmp_path, monkeypatch, capsys):
    data = tmp_path / "data"
    data.mkdir()
    monkeypatch.setenv("BRASA_DATA_PATH", str(data))
    cli.main(["info"])
    out = capsys.readouterr().out
    assert "BRASA_DATA_PATH environment variable" in out
    assert "(exists)" in out


def test_info_configured_via_config_file(isolated_config, tmp_path, capsys):
    data = tmp_path / "data"
    data.mkdir()
    isolated_config.write_text(f'data_path = "{data}"\n')
    cli.main(["info"])
    out = capsys.readouterr().out
    assert "config file" in out
    assert "(exists)" in out


def test_info_not_configured(isolated_config, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["info"])
    assert exc.value.code == 1
    assert "not configured" in capsys.readouterr().out


def test_info_missing_data_path(isolated_config, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("BRASA_DATA_PATH", str(tmp_path / "nope"))
    with pytest.raises(SystemExit) as exc:
        cli.main(["info"])
    assert exc.value.code == 1
    assert "(MISSING)" in capsys.readouterr().out


def test_info_invalid_config_toml(isolated_config, capsys):
    isolated_config.write_text("data_path = [unclosed\n")
    with pytest.raises(SystemExit) as exc:
        cli.main(["info"])
    assert exc.value.code == 1
    assert "INVALID TOML" in capsys.readouterr().out


def test_info_env_var_overrides_config_file(
    isolated_config, tmp_path, monkeypatch, capsys
):
    data = tmp_path / "data"
    data.mkdir()
    isolated_config.write_text('data_path = "/other/path"\n')
    monkeypatch.setenv("BRASA_DATA_PATH", str(data))
    cli.main(["info"])
    assert "overridden by BRASA_DATA_PATH" in capsys.readouterr().out
