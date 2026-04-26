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

        with patch("requests.get", return_value=mock_response) as mock_get:
            fp, headers = format_download(mock_md, year=2024)

        mock_get.assert_called_once_with(
            "https://example.com/COTAHIST_A2024.ZIP", verify=False
        )
        assert fp is not None
        assert headers["Content-Type"] == "application/zip"
