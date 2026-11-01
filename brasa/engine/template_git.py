"""Git operations for template folders: clone at ``brasa init``, update later.

git always runs with an argument list (never a shell string), and a URL is
passed after ``--`` so a value starting with ``-`` can't become an option.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from .exceptions import TemplateFolderError

OFFICIAL_TEMPLATES_URL = "https://github.com/wilsonfreitas/brasa-templates"
_URL_PREFIXES = ("https://", "http://", "ssh://", "git@", "file://")


def is_url(value: str) -> bool:
    """Return True when ``value`` is a git URL rather than a local path.

    Args:
        value: What the user passed to ``brasa init --templates``.

    Returns:
        True for ``https://``, ``http://``, ``ssh://``, ``git@`` and
        ``file://`` values and anything ending in ``.git``.
    """
    return value.startswith(_URL_PREFIXES) or value.endswith(".git")


def _git(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    """Run git with an argument list; never raises for a non-zero exit."""
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=False
    )


def _require_git() -> None:
    """Raise a clear error when git is not installed."""
    if shutil.which("git") is None:
        raise TemplateFolderError(
            "git is required to clone brasa-templates. Install git, or download "
            "the repo yourself and run `brasa init --templates <path>`."
        )


def _is_git_root(folder: Path) -> bool:
    """True when ``folder`` is the top level of a git work tree.

    A folder that merely sits inside some other repository is not treated as
    a clone, so updating never pulls an unrelated parent repository.
    """
    out = _git("-C", str(folder), "rev-parse", "--show-toplevel")
    return out.returncode == 0 and Path(out.stdout.strip()).resolve() == (
        folder.resolve()
    )


def _short_head(folder: Path) -> str:
    """Return the short commit hash of ``folder``'s HEAD."""
    return _git("-C", str(folder), "rev-parse", "--short", "HEAD").stdout.strip()


def setup_template_folder(data_path: Path, templates: str | None) -> tuple[Path, str]:
    """Return the template folder to save in the config, and a one-line note.

    Args:
        data_path: The brasa data directory; a clone goes to
            ``data_path/templates``.
        templates: A git URL to clone, an existing folder to use as is, or
            None to clone the official brasa-templates repo.

    Returns:
        ``(folder, message)``.

    Raises:
        TemplateFolderError: If a given folder doesn't exist, git is missing,
            ``data_path/templates`` exists but is not a git clone, or the
            clone fails.
    """
    if templates is not None and not is_url(templates):
        folder = Path(templates).expanduser().resolve()
        if not folder.is_dir():
            raise TemplateFolderError(f"Template folder not found: {folder}")
        return folder, f"using existing folder {folder}"

    url = templates or OFFICIAL_TEMPLATES_URL
    target = (Path(data_path) / "templates").resolve()
    _require_git()
    if target.exists():
        if _is_git_root(target):
            return target, f"reusing existing clone {target}"
        raise TemplateFolderError(
            f"{target} exists and is not a git clone: remove it, or run "
            "`brasa init --templates <path>` to use a folder as it is."
        )
    result = _git("clone", "--quiet", "--", url, str(target))
    if result.returncode != 0:
        raise TemplateFolderError(f"git clone {url} failed: {result.stderr.strip()}")
    return target, f"cloned {url} into {target}"


def update_template_folders() -> list[tuple[Path, str, bool]]:
    """Fast-forward every template folder that is a git clone.

    Uses ``git pull --ff-only``: it never merges and never overwrites local
    changes. Folders that are not clones, or have no upstream branch, are
    skipped.

    Returns:
        ``(folder, message, ok)`` for each non-bundled folder, highest
        priority first; ``ok`` is False only when git refused the update.
    """
    from .template import get_template_folders

    has_git = shutil.which("git") is not None
    results: list[tuple[Path, str, bool]] = []
    for folder in get_template_folders():
        if folder.bundled:
            continue
        root = folder.root
        if not has_git or not _is_git_root(root):
            results.append((root, "skipped (not a git repository)", True))
            continue
        upstream = _git("-C", str(root), "rev-parse", "--abbrev-ref", "@{upstream}")
        if upstream.returncode != 0:
            results.append((root, "skipped (no upstream branch)", True))
            continue
        old = _short_head(root)
        pulled = _git("-C", str(root), "pull", "--ff-only", "--quiet")
        if pulled.returncode != 0:
            lines = [ln for ln in (pulled.stderr or pulled.stdout).splitlines() if ln]
            reason = lines[0] if lines else "git pull failed"
            results.append((root, f"failed: {reason}", False))
            continue
        new = _short_head(root)
        message = "already up to date" if old == new else f"updated {old} -> {new}"
        results.append((root, message, True))
    return results


def git_info(folder: Path) -> tuple[str | None, str | None]:
    """Return ``(origin URL, short commit)`` of a git folder.

    Args:
        folder: A template folder.

    Returns:
        ``(None, None)`` when the folder is not a git clone (or git is missing).
    """
    if shutil.which("git") is None or not _is_git_root(folder):
        return None, None
    url = _git("-C", str(folder), "remote", "get-url", "origin").stdout.strip()
    return url or None, _short_head(folder) or None
