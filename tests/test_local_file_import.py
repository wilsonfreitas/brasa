from datetime import datetime
from types import SimpleNamespace

import pytest

from brasa.downloaders import local_file_import
from brasa.engine.exceptions import DownloadException


def test_reads_file_and_builds_provenance(tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("a,b\n1,2\n")
    md = SimpleNamespace(path=str(f), format="csv")

    fp, prov = local_file_import(md)
    try:
        assert fp.read() == b"a,b\n1,2\n"
    finally:
        fp.close()

    assert prov["acquisition"] == "import"
    assert prov["source_path"] == str(f.resolve())
    assert prov["original_name"] == "data.csv"
    assert prov["source_size"] == 8
    assert "mtime" in prov and "imported_at" in prov


def test_missing_file_raises_download_exception():
    md = SimpleNamespace(path="/nonexistent/x.csv", format="csv")
    with pytest.raises(DownloadException, match="import source not found"):
        local_file_import(md)


def test_renders_strftime_and_format(tmp_path):
    sub = tmp_path / "ABC"
    sub.mkdir()
    (sub / "2026-06-20.csv").write_text("x")
    md = SimpleNamespace(path=str(tmp_path / "{asset}/%Y-%m-%d.csv"), format="csv")

    fp, prov = local_file_import(md, asset="ABC", refdate=datetime(2026, 6, 20))
    try:
        assert fp.read() == b"x"
    finally:
        fp.close()
    assert prov["original_name"] == "2026-06-20.csv"


def test_missing_placeholder_fails_loud(tmp_path):
    md = SimpleNamespace(path=str(tmp_path / "{asset}.csv"), format="csv")
    with pytest.raises(DownloadException, match="placeholder"):
        local_file_import(md)


def test_import_path_argument_overrides(tmp_path):
    f = tmp_path / "override.csv"
    f.write_text("y")
    md = SimpleNamespace(path="/should/not/use.csv", format="csv")

    fp, _prov = local_file_import(md, _import_path=str(f))
    try:
        assert fp.read() == b"y"
    finally:
        fp.close()
