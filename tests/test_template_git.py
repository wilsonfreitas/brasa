"""Template folders backed by git: clone at init, update later (WIL-199)."""

import os
import subprocess
from pathlib import Path

import pytest

from brasa.engine import template_git
from brasa.engine.exceptions import TemplateFolderError
from brasa.engine.template import clear_template_cache
from brasa.engine.template_git import (
    git_info,
    is_url,
    setup_template_folder,
    update_template_folders,
)

_ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@example.com",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@example.com",
}


def git(*args, cwd):
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True, env=_ENV
    ).stdout.strip()


def make_origin(tmp_path: Path) -> Path:
    """A bare repo whose main branch has templates/t1.yaml."""
    src = tmp_path / "src"
    (src / "templates").mkdir(parents=True)
    (src / "templates" / "t1.yaml").write_text("id: t1\n")
    git("init", "-q", "-b", "main", cwd=src)
    git("add", ".", cwd=src)
    git("commit", "-q", "-m", "first", cwd=src)
    bare = tmp_path / "origin.git"
    git("clone", "-q", "--bare", str(src), str(bare), cwd=tmp_path)
    return bare


def push_change(tmp_path: Path, origin: Path, text: str) -> None:
    work = tmp_path / f"work-{abs(hash(text))}"
    git("clone", "-q", str(origin), str(work), cwd=tmp_path)
    (work / "templates" / "t1.yaml").write_text(text)
    git("commit", "-q", "-am", "change", cwd=work)
    git("push", "-q", cwd=work)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("BRASA_TEMPLATE_PATH", raising=False)
    clear_template_cache()
    yield
    clear_template_cache()


def test_is_url():
    for url in ("https://x/y", "git@github.com:a/b.git", "file:///tmp/x", "ssh://h/x"):
        assert is_url(url)
    assert is_url("some/repo.git")
    for path in ("~/dev/x", "/abs/path", "rel/path", "-x"):
        assert not is_url(path)


def test_setup_clones_url(tmp_path):
    origin = make_origin(tmp_path)
    folder, msg = setup_template_folder(tmp_path / "home", f"file://{origin}")
    assert folder == (tmp_path / "home" / "templates").resolve()
    assert (folder / "templates" / "t1.yaml").exists()
    assert msg.startswith("cloned file://")


def test_setup_uses_existing_path(tmp_path):
    existing = tmp_path / "mine"
    existing.mkdir()
    folder, msg = setup_template_folder(tmp_path / "home", str(existing))
    assert folder == existing.resolve()
    assert "existing folder" in msg
    assert not (tmp_path / "home" / "templates").exists()


def test_setup_missing_path_errors(tmp_path):
    with pytest.raises(TemplateFolderError, match="Template folder not found"):
        setup_template_folder(tmp_path / "home", str(tmp_path / "nope"))


def test_setup_reuses_existing_clone(tmp_path):
    origin = make_origin(tmp_path)
    setup_template_folder(tmp_path / "home", f"file://{origin}")
    folder, msg = setup_template_folder(tmp_path / "home", f"file://{origin}")
    assert msg.startswith("reusing existing clone")
    assert folder == (tmp_path / "home" / "templates").resolve()


def test_setup_existing_non_git_dir_errors(tmp_path):
    (tmp_path / "home" / "templates").mkdir(parents=True)
    with pytest.raises(TemplateFolderError, match="--templates <path>"):
        setup_template_folder(tmp_path / "home", None)


def test_setup_git_missing_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(template_git.shutil, "which", lambda _: None)
    with pytest.raises(TemplateFolderError, match="git is required"):
        setup_template_folder(tmp_path / "home", None)


def test_update_fast_forward(tmp_path, monkeypatch):
    origin = make_origin(tmp_path)
    folder, _ = setup_template_folder(tmp_path / "home", f"file://{origin}")
    push_change(tmp_path, origin, "id: t1\ndescription: new\n")
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", str(folder))
    [(root, msg, ok)] = update_template_folders()
    assert root.resolve() == folder
    assert ok
    assert msg.startswith("updated ") and " -> " in msg
    assert "description: new" in (folder / "templates" / "t1.yaml").read_text()


def test_update_refuses_conflicting_local_change(tmp_path, monkeypatch):
    origin = make_origin(tmp_path)
    folder, _ = setup_template_folder(tmp_path / "home", f"file://{origin}")
    local = folder / "templates" / "t1.yaml"
    local.write_text("id: t1\n# my local edit\n")
    push_change(tmp_path, origin, "id: t1\ndescription: upstream\n")
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", str(folder))
    [(_, msg, ok)] = update_template_folders()
    assert not ok
    assert msg.startswith("failed: ")
    assert local.read_text() == "id: t1\n# my local edit\n"


def test_update_skips_non_git_folder(tmp_path, monkeypatch):
    plain = tmp_path / "plain"
    (plain / "templates").mkdir(parents=True)
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", str(plain))
    assert update_template_folders() == [
        (plain, "skipped (not a git repository)", True)
    ]


def test_update_skips_no_upstream(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / "templates").mkdir(parents=True)
    git("init", "-q", cwd=repo)
    monkeypatch.setenv("BRASA_TEMPLATE_PATH", str(repo))
    assert update_template_folders() == [(repo, "skipped (no upstream branch)", True)]


def test_git_info(tmp_path):
    origin = make_origin(tmp_path)
    folder, _ = setup_template_folder(tmp_path / "home", f"file://{origin}")
    url, commit = git_info(folder)
    assert url == f"file://{origin}"
    assert commit == git("rev-parse", "--short", "HEAD", cwd=folder)
    assert git_info(tmp_path) == (None, None)
