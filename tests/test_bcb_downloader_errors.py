"""BCB/B3 downloaders must raise typed errors instead of returning None (Q2.3)."""

from datetime import datetime
from types import SimpleNamespace

import pytest

import brasa.downloaders.downloaders as dl
from brasa.engine.exceptions import DownloadException


def test_bcb_sgs_downloader_wraps_errors(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("SGS is down")

    monkeypatch.setattr(dl.sgs, "get_json", boom)
    downloader = dl.BCBSGSDownloader(
        code=433, start=datetime(2024, 1, 1), end=datetime(2024, 1, 31)
    )
    with pytest.raises(DownloadException) as excinfo:
        downloader.download()
    assert isinstance(excinfo.value.__cause__, RuntimeError)


def test_bcb_currency_downloader_wraps_errors(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("PTAX is down")

    monkeypatch.setattr(dl, "PTAX", boom)
    downloader = dl.BCBCurrencyDownloader(
        currency="USD", start=datetime(2024, 1, 1), end=datetime(2024, 1, 31)
    )
    with pytest.raises(DownloadException) as excinfo:
        downloader.download()
    assert isinstance(excinfo.value.__cause__, RuntimeError)


def test_b3_files_downloader_raises_on_non_200(monkeypatch):
    fake_res = SimpleNamespace(status_code=404)
    monkeypatch.setattr(dl.requests, "get", lambda *a, **kw: fake_res)

    downloader = dl.B3FilesURLDownloader(
        "https://example.invalid/%Y%m%d", verify_ssl=True, refdate=datetime(2024, 1, 2)
    )
    with pytest.raises(DownloadException, match="status_code = 404"):
        downloader.download()
