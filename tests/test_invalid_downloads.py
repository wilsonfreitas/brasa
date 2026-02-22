"""Tests for invalid download detection and handling.

Tests cover:
- Detection of invalid content via InvalidContentException
- Metadata persistence of invalid status and reason
- Skipping downloads for invalid cached entries
- Forcing re-download with reprocess=True
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from brasa.engine import CacheManager, CacheMetadata
from brasa.engine.api import _should_download
from brasa.engine.exceptions import InvalidContentException


@pytest.fixture
def temp_cache():
    """Create a temporary cache directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        original_cache = CacheManager.__dict__.get("__it__")
        CacheManager.__it__ = None

        cache = CacheManager()
        cache._cache_folder = tmpdir
        Path(tmpdir).mkdir(parents=True, exist_ok=True)
        Path(cache.cache_path(cache._meta_folder)).mkdir(parents=True, exist_ok=True)
        Path(cache.cache_path(cache._db_folder)).mkdir(parents=True, exist_ok=True)
        cache.create_meta_db()

        yield cache

        if original_cache is not None:
            CacheManager.__it__ = original_cache
        else:
            CacheManager.__it__ = None


def test_invalid_download_metadata_fields():
    """Test that CacheMetadata has invalid download fields."""
    meta = CacheMetadata("test-template")

    assert hasattr(meta, "is_invalid_download")
    assert hasattr(meta, "invalid_download_reason")
    assert meta.is_invalid_download is False
    assert meta.invalid_download_reason == ""


def test_invalid_download_metadata_to_dict():
    """Test that invalid fields are included in to_dict()."""
    meta = CacheMetadata("test-template")
    meta.is_invalid_download = True
    meta.invalid_download_reason = "Test error message"

    meta_dict = meta.to_dict()

    assert meta_dict["is_invalid_download"] is True
    assert meta_dict["invalid_download_reason"] == "Test error message"


def test_invalid_content_exception():
    """Test that InvalidContentException can be raised and caught."""
    with pytest.raises(InvalidContentException) as exc_info:
        raise InvalidContentException("Test invalid content")

    assert "Test invalid content" in str(exc_info.value)


def test_save_and_load_invalid_download_status(temp_cache):
    """Test saving and loading invalid download status from cache."""
    meta = CacheMetadata("test-template")
    meta.download_checksum = "test-checksum"
    meta.download_args = {"test": "arg"}
    meta.is_invalid_download = True
    meta.invalid_download_reason = "File format validation failed"

    # Save metadata
    temp_cache.save_meta(meta)

    # Load metadata to verify persistence
    loaded_meta = CacheMetadata("test-template")
    loaded_meta.download_args = {"test": "arg"}
    temp_cache.load_meta(loaded_meta)

    assert loaded_meta.is_invalid_download is True
    assert loaded_meta.invalid_download_reason == "File format validation failed"


def test_should_download_skips_invalid_cache():
    """Test that _should_download skips when last trial is INVALID."""
    mock_cache = MagicMock()
    meta = CacheMetadata("test-template")
    meta.download_args = {"test": "arg"}

    # Setup: no meta row (new approach), but trial says INVALID
    mock_cache.has_meta.return_value = False
    mock_cache.get_last_download_status.return_value = {
        "code": "I",
        "name": "INVALID",
        "reason": "empty file",
        "http_status": None,
    }

    result = _should_download(mock_cache, meta, reprocess=False)

    # Should return False - skip download for INVALID trial
    assert result is False


def test_should_download_skips_invalid_cache_legacy():
    """Test backward compat: _should_download skips via meta.is_invalid_download."""
    mock_cache = MagicMock()
    meta = CacheMetadata("test-template")
    meta.download_args = {"test": "arg"}

    # Setup: legacy meta row with is_invalid_download flag
    mock_cache.has_meta.return_value = True
    mock_cache.load_meta.side_effect = lambda m: setattr(m, "is_invalid_download", True)

    result = _should_download(mock_cache, meta, reprocess=False)

    # Should return False - skip download for invalid cache (legacy)
    assert result is False


def test_should_download_forces_redownload_for_invalid():
    """Test that reprocess=True forces re-download even if marked invalid."""
    mock_cache = MagicMock()
    meta = CacheMetadata("test-template")
    meta.download_args = {"test": "arg"}
    meta.is_invalid_download = True

    # Setup: reprocess=True should trigger remove_meta
    mock_cache.has_meta.return_value = True

    result = _should_download(mock_cache, meta, reprocess=True)

    assert result is True
    mock_cache.remove_meta.assert_called_once()


def test_should_download_normal_flow_unchanged(temp_cache):
    """Test that normal download flow is unchanged (no invalid flag)."""
    meta = CacheMetadata("test-template")
    meta.download_args = {"test": "arg"}
    meta.is_invalid_download = False

    # When cache has no metadata, should download
    result = _should_download(temp_cache, meta, reprocess=False)
    assert result is True

    # After saving successful attempt
    temp_cache.save_trial(meta, True)
    temp_cache.save_meta(meta)

    # Create new metadata instance with same args
    meta2 = CacheMetadata("test-template")
    meta2.download_args = {"test": "arg"}

    # When cache exists and trial was successful, should not download
    result = _should_download(temp_cache, meta2, reprocess=False)
    assert result is False


def test_invalid_download_metadata_from_dict():
    """Test loading invalid status via from_dict()."""
    meta = CacheMetadata("test-template")

    data = {
        "is_invalid_download": True,
        "invalid_download_reason": "Corrupted data",
        "download_checksum": "checksum123",
    }

    meta.from_dict(data)

    assert meta.is_invalid_download is True
    assert meta.invalid_download_reason == "Corrupted data"


def test_invalid_download_reason_stored_and_retrieved(temp_cache):
    """Test that invalid download reason is properly stored and retrieved."""
    meta = CacheMetadata("test-template")
    meta.download_checksum = "checksum-test"
    meta.download_args = {"arg1": "value1"}

    error_reason = "HTTP 403: Access Denied - Server returned invalid token"
    meta.is_invalid_download = True
    meta.invalid_download_reason = error_reason

    # Save and reload
    temp_cache.save_meta(meta)

    loaded_meta = CacheMetadata("test-template")
    loaded_meta.download_args = {"arg1": "value1"}
    temp_cache.load_meta(loaded_meta)

    # Verify reason is preserved
    assert loaded_meta.invalid_download_reason == error_reason


def test_clear_invalid_status_on_successful_download(temp_cache):
    """Test that invalid status is cleared when reprocess succeeds."""
    meta = CacheMetadata("test-template")
    meta.download_checksum = "old-checksum"
    meta.download_args = {"arg1": "value1"}
    meta.is_invalid_download = True
    meta.invalid_download_reason = "Old error"

    temp_cache.save_meta(meta)

    # After successful reprocess, invalid flags should be reset
    new_meta = CacheMetadata("test-template")
    new_meta.download_args = {"arg1": "value1"}
    new_meta.download_checksum = "new-checksum"
    new_meta.is_invalid_download = False
    new_meta.invalid_download_reason = ""

    temp_cache.save_meta(new_meta)

    loaded_meta = CacheMetadata("test-template")
    loaded_meta.download_args = {"arg1": "value1"}
    temp_cache.load_meta(loaded_meta)

    assert loaded_meta.is_invalid_download is False
    assert loaded_meta.invalid_download_reason == ""
