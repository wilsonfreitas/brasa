"""End-to-end CLI bootstrap behavior in a clean environment (no config)."""

import os
import subprocess
import sys

import pytest


def run_cli(argv, cwd, home):
    """Run the brasa CLI in a subprocess with no brasa configuration.

    Strips BRASA_DATA_PATH and points HOME/XDG dirs at *home* so neither
    the env var nor a real user config file can leak in.
    """
    env = {k: v for k, v in os.environ.items() if k != "BRASA_DATA_PATH"}
    env["HOME"] = str(home)
    env["XDG_CONFIG_HOME"] = str(home / ".config")
    env["XDG_DATA_HOME"] = str(home / ".local" / "share")
    code = (
        f"import sys; sys.argv = {['brasa', *argv]!r}; "
        "from brasa.cli import main; main()"
    )
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


@pytest.fixture()
def clean_dirs(tmp_path):
    cwd = tmp_path / "cwd"
    home = tmp_path / "home"
    cwd.mkdir()
    home.mkdir()
    return cwd, home


def test_stateful_command_fails_with_actionable_message(clean_dirs):
    cwd, home = clean_dirs
    result = run_cli(["list-datasets"], cwd, home)
    assert result.returncode == 1
    assert "brasa is not configured yet" in result.stderr
    assert "brasa init" in result.stderr
    assert "Traceback" not in result.stderr


def test_stateful_command_creates_no_brasa_cache(clean_dirs):
    cwd, home = clean_dirs
    run_cli(["list-datasets"], cwd, home)
    assert not (cwd / ".brasa-cache").exists()
