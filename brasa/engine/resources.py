"""Helpers for package data bundled inside brasa and the installed version."""

from importlib import metadata
from importlib.resources import files
from pathlib import Path

# The PyPI distribution name (it was "brasa-marketdata" until 0.4.0).
DISTRIBUTION = "brasa-core"


def package_path(*parts: str) -> Path:
    """Resolve a path to a data file bundled in the brasa package.

    Args:
        *parts: Path components under ``brasa/files`` — e.g.
            ``"sql", "create-meta-db.sql"``.

    Returns:
        A concrete filesystem ``Path`` to the requested resource. Works for
        wheel, editable, and source-tree installs.
    """
    base = Path(str(files("brasa")))
    return base.joinpath("files", *parts)


def package_version() -> str:
    """Return the installed brasa version.

    Returns:
        The version of the installed distribution, e.g. ``"0.5.0.dev0"``.
    """
    return metadata.version(DISTRIBUTION)
