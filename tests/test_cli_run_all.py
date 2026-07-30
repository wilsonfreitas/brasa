"""Integration tests for the `brasa run-all` CLI command (in-process)."""

from __future__ import annotations

import pytest

from brasa import cli


def _run_cli(argv, capsys):
    try:
        cli.main(argv)
        code = 0
    except SystemExit as e:
        code = int(e.code or 0)
    return code, capsys.readouterr().out


def test_run_all_help(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["run-all", "--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "--dry-run" in out
    assert "-v" in out


def test_run_all_dry_run_empty_cache(capsys):
    """Dry-run on an empty cache: downloads blocked, no failures -> exit 0."""
    code, out = _run_cli(["run-all", "--dry-run"], capsys)
    assert code == 0
    assert "DRY RUN" in out or "Everything is up to date." in out


def test_run_all_empty_cache_exits_zero(capsys):
    """Real run on an empty cache: all downloads blocked (not failed) -> exit 0."""
    code, _ = _run_cli(["run-all"], capsys)
    assert code == 0
