"""Integration tests for the `brasa run-all` CLI command."""

from __future__ import annotations

import subprocess
import sys


def test_run_all_help():
    result = subprocess.run(
        [sys.executable, "-m", "brasa.cli", "run-all", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--dry-run" in result.stdout
    assert "-v" in result.stdout


def test_run_all_dry_run_empty_cache():
    """Dry-run on an empty cache: downloads blocked, no failures -> exit 0."""
    result = subprocess.run(
        [sys.executable, "-m", "brasa.cli", "run-all", "--dry-run"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "DRY RUN" in result.stdout or "Everything is up to date." in result.stdout


def test_run_all_empty_cache_exits_zero():
    """Real run on an empty cache: all downloads blocked (not failed) -> exit 0."""
    result = subprocess.run(
        [sys.executable, "-m", "brasa.cli", "run-all"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
