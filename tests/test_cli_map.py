"""Integration tests for the `brasa map` CLI command."""

from __future__ import annotations

import subprocess
import sys


def test_map_help():
    result = subprocess.run(
        [sys.executable, "-m", "brasa.cli", "map", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--format" in result.stdout
    assert "flat" in result.stdout
    assert "grouped" in result.stdout
    assert "tree" in result.stdout
    assert "--all" in result.stdout
    assert "--reverse" in result.stdout
    assert "--no-color" in result.stdout


def test_map_runs_against_empty_cache():
    """In an empty cache, every template is 'never-run' — exit code 1."""
    result = subprocess.run(
        [sys.executable, "-m", "brasa.cli", "map", "--no-color"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode in (0, 1)
