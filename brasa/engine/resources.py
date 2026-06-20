"""Helpers for resolving data files bundled inside the brasa package."""

from importlib.resources import files
from pathlib import Path


def package_path(*parts: str) -> Path:
    """Resolve a path to a data file bundled in the brasa package.

    Args:
        *parts: Path components under ``brasa/files`` — e.g. ``"templates"`` or
            ``"sql", "create-meta-db.sql"``.

    Returns:
        A concrete filesystem ``Path`` to the requested resource. Works for
        wheel, editable, and source-tree installs.
    """
    base = Path(str(files("brasa")))
    return base.joinpath("files", *parts)
