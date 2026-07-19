"""Unit tests for FormatURLDownloader."""

import pytest

from brasa.downloaders.downloaders import FormatURLDownloader


class TestFormatURLDownloaderURL:
    def test_single_int_arg(self):
        """Integer year arg expands into URL."""
        dl = FormatURLDownloader(
            "https://example.com/COTAHIST_A{year}.ZIP", verify_ssl=False, year=2024
        )
        assert dl.url == "https://example.com/COTAHIST_A2024.ZIP"

    def test_single_str_arg(self):
        """String year arg expands identically to int."""
        dl = FormatURLDownloader(
            "https://example.com/COTAHIST_A{year}.ZIP", verify_ssl=False, year="2024"
        )
        assert dl.url == "https://example.com/COTAHIST_A2024.ZIP"

    def test_multi_arg(self):
        """Multiple named args are all expanded."""
        dl = FormatURLDownloader(
            "https://example.com/FILE_{code}_{year}.zip",
            verify_ssl=False,
            code="X",
            year=2024,
        )
        assert dl.url == "https://example.com/FILE_X_2024.zip"

    def test_missing_placeholder_raises_key_error(self):
        """URL placeholder with no matching kwarg raises KeyError."""
        dl = FormatURLDownloader(
            "https://example.com/COTAHIST_A{year}.ZIP",
            verify_ssl=False,
            wrong_key=2024,
        )
        with pytest.raises(KeyError):
            _ = dl.url


class TestFormatDownloadHelper:
    def test_helper_constructs_correct_url(self):
        """format_download expands the URL and returns bytes + headers."""
        from unittest.mock import MagicMock, patch

        from brasa.downloaders.helpers import format_download

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"fake zip content"
        mock_response.headers = {"Content-Type": "application/zip"}

        mock_md = MagicMock()
        mock_md.url = "https://example.com/COTAHIST_A{year}.ZIP"
        mock_md.verify_ssl = False
        mock_md.format = ""
        mock_md.timeout = None

        with patch("requests.get", return_value=mock_response) as mock_get:
            fp, headers = format_download(mock_md, year=2024)

        mock_get.assert_called_once_with(
            "https://example.com/COTAHIST_A2024.ZIP",
            verify=False,
            timeout=(10, 120),
        )
        assert fp is not None
        assert headers["Content-Type"] == "application/zip"


# ---------------------------------------------------------------------------
# WIL-97 — content validation + timeout in SimpleDownloader
# ---------------------------------------------------------------------------

import io  # noqa: E402
import zipfile  # noqa: E402
from unittest.mock import Mock, patch  # noqa: E402

from brasa.downloaders.downloaders import (  # noqa: E402
    SimpleDownloader,
    validate_download_content,
)
from brasa.engine.exceptions import DownloadException, NoDataException  # noqa: E402


def _zip_bytes(names):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for n in names:
            zf.writestr(n, b"x")
    return buf.getvalue()


def test_validate_empty_body_raises_download_exception():
    with pytest.raises(DownloadException):
        validate_download_content(b"", "zip")


def test_validate_non_zip_body_raises_download_exception():
    with pytest.raises(DownloadException):
        validate_download_content(b"<html>error</html>", "zip")


def test_validate_empty_zip_raises_no_data():
    with pytest.raises(NoDataException):
        validate_download_content(_zip_bytes([]), "zip")


def test_validate_valid_zip_passes():
    assert validate_download_content(_zip_bytes(["ID260615.ex_"]), "zip") is None


def test_validate_non_zip_format_only_checks_empty():
    assert validate_download_content(b"{...}", "json") is None
    with pytest.raises(DownloadException):
        validate_download_content(b"", "json")


@patch("brasa.downloaders.downloaders.requests.get")
def test_simple_downloader_passes_timeout_and_validates_zip(mock_get):
    mock_get.return_value = Mock(status_code=200, content=_zip_bytes(["a.txt"]))
    d = SimpleDownloader("http://x/y.zip", verify_ssl=True)
    d.fmt = "zip"
    d.timeout = 90
    fp = d.download()
    assert mock_get.call_args[1]["timeout"] == 90
    assert fp.read() == _zip_bytes(["a.txt"])


@patch("brasa.downloaders.downloaders.requests.get")
def test_simple_downloader_empty_zip_raises_no_data(mock_get):
    mock_get.return_value = Mock(status_code=200, content=_zip_bytes([]))
    d = SimpleDownloader("http://x/y.zip", verify_ssl=True)
    d.fmt = "zip"
    with pytest.raises(NoDataException):
        d.download()


def test_b3_pregao_download_importable_and_tuned():
    from brasa.downloaders import b3_pregao_download
    from brasa.downloaders.downloaders import B3PregaoDownloader, DatetimeDownloader

    assert issubclass(B3PregaoDownloader, DatetimeDownloader)
    assert B3PregaoDownloader.timeout == (10, 90)
    assert callable(b3_pregao_download)
