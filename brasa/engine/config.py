"""User-level configuration for brasa: data-path resolution and persistence."""

import json
import os
import sys
from pathlib import Path

import platformdirs

from .exceptions import BrasaNotConfiguredError

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

_APP_NAME = "brasa"


def default_data_path() -> str:
    """Return the platform default data directory for brasa.

    Returns:
        Absolute path, e.g. ``~/.local/share/brasa`` on Linux.
    """
    return platformdirs.user_data_dir(_APP_NAME)


def config_file_path() -> Path:
    """Return the path of the user config file.

    Returns:
        Path to ``config.toml`` inside the platform config dir,
        e.g. ``~/.config/brasa/config.toml`` on Linux.
    """
    return platformdirs.user_config_path(_APP_NAME) / "config.toml"


def load_config() -> dict:
    """Load the user config file.

    Returns:
        Parsed TOML as a dict; empty dict when the file does not exist.
    """
    path = config_file_path()
    if not path.exists():
        return {}
    with path.open("rb") as f:
        return tomllib.load(f)


def save_data_path(data_path: str) -> None:
    """Persist *data_path* into the user config file.

    Args:
        data_path: Absolute path of the brasa data directory.
    """
    path = config_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    # json.dumps produces a valid TOML basic string (escapes \ and ")
    path.write_text(f"data_path = {json.dumps(str(data_path))}\n", encoding="utf-8")


def resolve_data_path() -> str:
    """Resolve the brasa data path.

    Precedence: ``BRASA_DATA_PATH`` env var, then ``data_path`` in the
    user config file.

    Returns:
        The resolved data path.

    Raises:
        BrasaNotConfiguredError: If neither source provides a path.
    """
    env = os.environ.get("BRASA_DATA_PATH")
    if env:
        return env
    data_path = load_config().get("data_path")
    if data_path:
        return str(data_path)
    raise BrasaNotConfiguredError(
        "brasa is not configured: run `brasa init` or set BRASA_DATA_PATH"
    )
