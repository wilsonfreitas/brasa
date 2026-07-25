"""Unit tests for B3PagedURLEncodedDownloader."""

import json
from unittest.mock import MagicMock, patch

from brasa.downloaders.downloaders import B3PagedURLEncodedDownloader


def _make_response(data: dict, status_code: int = 200) -> MagicMock:
    """Create a mock requests.Response."""
    mock = MagicMock()
    mock.status_code = status_code
    mock.content = json.dumps(data).encode()
    return mock


class TestB3PagedURLEncodedDownloaderEmptyResults:
    def test_empty_results_returns_empty_results_file(self):
        """Empty results download cleanly; rejection is the validator's job."""
        response_data = {"page": {"totalPages": 1}, "results": []}
        mock_response = _make_response(response_data)

        with patch("requests.get", return_value=mock_response):
            downloader = B3PagedURLEncodedDownloader(
                "http://example.com/api", verify_ssl=False
            )
            result = downloader.download()

        assert result is not None
        assert json.loads(result.read())["results"] == []

    def test_null_results_returns_empty_results_file(self):
        """B3 returns "results": null for indexes with no portfolio."""
        response_data = {"page": {"totalPages": 1}, "results": None}
        mock_response = _make_response(response_data)

        with patch("requests.get", return_value=mock_response):
            downloader = B3PagedURLEncodedDownloader(
                "http://example.com/api", verify_ssl=False
            )
            result = downloader.download()

        assert result is not None
        assert json.loads(result.read())["results"] == []

    def test_null_total_pages_does_not_crash(self):
        response_data = {"page": {"totalPages": None}, "results": None}
        mock_response = _make_response(response_data)

        with patch("requests.get", return_value=mock_response):
            downloader = B3PagedURLEncodedDownloader(
                "http://example.com/api", verify_ssl=False
            )
            result = downloader.download()

        assert json.loads(result.read())["results"] == []


class TestB3PagedURLEncodedDownloaderSinglePage:
    def test_single_page_with_results_returns_file(self):
        """When results are present on a single page, return a valid BytesIO."""
        response_data = {
            "page": {"totalPages": 1},
            "results": [{"id": 1, "name": "foo"}],
        }
        mock_response = _make_response(response_data)

        with patch("requests.get", return_value=mock_response):
            downloader = B3PagedURLEncodedDownloader(
                "http://example.com/api", verify_ssl=False
            )
            result = downloader.download()

        assert result is not None
        content = json.loads(result.read())
        assert len(content["results"]) == 1
        assert content["results"][0]["id"] == 1


class TestB3PagedURLEncodedDownloaderMultiPage:
    def test_multi_page_results_are_combined(self):
        """When there are multiple pages, all results are combined."""
        page1_data = {
            "page": {"totalPages": 2},
            "results": [{"id": 1}, {"id": 2}],
        }
        page2_data = {
            "page": {"totalPages": 2},
            "results": [{"id": 3}, {"id": 4}],
        }
        mock_response1 = _make_response(page1_data)
        mock_response2 = _make_response(page2_data)

        with patch("requests.get", side_effect=[mock_response1, mock_response2]):
            downloader = B3PagedURLEncodedDownloader(
                "http://example.com/api", verify_ssl=False
            )
            result = downloader.download()

        assert result is not None
        content = json.loads(result.read())
        assert len(content["results"]) == 4
        assert [r["id"] for r in content["results"]] == [1, 2, 3, 4]
