"""Tests for zip extraction helpers in brasa.util (audit Q1.3, Q8.6)."""

import zipfile

import pytest

from brasa.util import _is_zip, unzip_and_get_content, unzip_file_to


def _make_zip(path, members):
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return path


def test_unzip_file_to_extracts_members(tmp_path):
    archive = _make_zip(tmp_path / "ok.zip", {"a.txt": "alpha", "sub/b.txt": "beta"})
    dest = tmp_path / "out"
    dest.mkdir()

    files = unzip_file_to(str(archive), str(dest))

    assert sorted(f.rsplit("/", 1)[-1] for f in files) == ["a.txt", "b.txt"]
    assert (dest / "a.txt").read_text() == "alpha"
    assert (dest / "sub" / "b.txt").read_text() == "beta"


def test_unzip_file_to_rejects_path_traversal(tmp_path):
    archive = _make_zip(
        tmp_path / "evil.zip", {"../evil.txt": "gotcha", "ok.txt": "fine"}
    )
    dest = tmp_path / "out"
    dest.mkdir()

    with pytest.raises(ValueError, match="outside"):
        unzip_file_to(str(archive), str(dest))

    assert not (tmp_path / "evil.txt").exists()


def test_unzip_and_get_content_reads_member(tmp_path):
    archive = _make_zip(tmp_path / "ok.zip", {"a.txt": "alpha"})
    assert unzip_and_get_content(str(archive)) == b"alpha"
    assert unzip_and_get_content(str(archive), encode=True, encoding="utf-8") == "alpha"


def test_is_zip_accepts_path_objects(tmp_path):
    archive = _make_zip(tmp_path / "ok.zip", {"a.txt": "alpha"})
    assert _is_zip(archive)
    assert _is_zip(str(archive))
    assert not _is_zip(str(tmp_path / "missing.zip"))
