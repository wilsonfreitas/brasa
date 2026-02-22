"""Download operations for market data.

This module handles the downloading of market data from remote sources,
including file format handling (zip, base64), validation, and compression.
"""

import base64
import gzip
import shutil
from pathlib import Path

from brasa.util import generate_checksum_from_file, unzip_recursive

from .cache import CacheManager, CacheMetadata
from .exceptions import (
    DownloadException,
    DuplicatedFolderException,
    InvalidContentException,
)
from .template import retrieve_template


def _process_downloaded_format(
    template: object,
    file_rel_path: str,
    man: CacheManager,
    meta: CacheMetadata,
) -> list:
    """Process downloaded file based on format (zip, base64, or raw).

    Args:
        template: Template with downloader configuration.
        file_rel_path: Relative path to downloaded file.
        man: Cache manager instance.
        meta: Cache metadata object.

    Returns:
        List of processed file paths.

    Raises:
        Exception: If zip file is empty.
    """
    downloaded_files = []
    if template.downloader.format == "zip":
        filenames = unzip_recursive(man.cache_path(file_rel_path))
        if len(filenames) == 0:
            raise Exception("Market data download failed: empty zip file")
        for filename in filenames:
            fname = Path(filename).name
            _file_rel_path = str(Path(meta.download_folder) / fname)
            shutil.move(filename, man.cache_path(_file_rel_path))
            downloaded_files.append(_file_rel_path)
        Path(man.cache_path(file_rel_path)).unlink()
    elif template.downloader.format == "base64":
        with Path(man.cache_path(file_rel_path)).open("rb") as fp:
            fname = f"decoded.{template.downloader.decoded_format}"
            _file_rel_path = str(Path(meta.download_folder) / fname)
            with Path(man.cache_path(_file_rel_path)).open("wb") as fp_dest:
                base64.decode(fp, fp_dest)
            downloaded_files.append(_file_rel_path)
        Path(man.cache_path(file_rel_path)).unlink()
    else:
        downloaded_files.append(file_rel_path)
    return downloaded_files


def _validate_downloaded_files(
    template: object,
    files: list,
    man: CacheManager,
) -> None:
    """Validate downloaded files.

    Args:
        template: Template with downloader configuration.
        files: List of file paths to validate.
        man: Cache manager instance.

    Raises:
        InvalidContentException: If validation fails.
    """
    for fname in files:
        try:
            template.downloader.validate(man.cache_path(fname))
        except Exception as e:
            raise InvalidContentException(
                f"Downloaded content validation failed for {fname}: {e!s}"
            ) from e


def _gzip_downloaded_files(
    files: list,
    man: CacheManager,
    meta: CacheMetadata,
) -> None:
    """Compress downloaded files with gzip.

    Args:
        files: List of file paths to compress.
        man: Cache manager instance.
        meta: Cache metadata object.
    """
    files_to_gzip = list(files)
    for fname in files_to_gzip:
        _fname = man.cache_path(fname)
        with Path(_fname).open("rb") as f_in, gzip.open(_fname + ".gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        meta.add_downloaded_file(fname + ".gz")
        meta.remove_downloaded_file(fname)
        Path(_fname).unlink()


def _download_marketdata(meta: CacheMetadata, on_attempt_failure=None, **kwargs):
    """Download market data for a cache entry.

    Handles the complete download workflow:
    1. Execute download using template configuration
    2. Handle file formats (zip, base64, raw)
    3. Validate downloaded files
    4. Compress files with gzip

    Args:
        meta: Cache metadata to update with download results.
        on_attempt_failure: Optional callback for per-attempt retry
            persistence. Passed through to downloader.download().
        **kwargs: Download arguments to pass to the download function.

    Returns:
        Dict with retry info keys (attempts_used, attempts_configured,
        success_on_attempt) or empty dict when retry is not configured.

    Raises:
        DownloadException: If download fails.
        DuplicatedFolderException: If download folder already exists.
    """
    template = retrieve_template(meta.template)
    meta.download_args = kwargs
    meta.extra_key = template.downloader.extra_key
    fp, response, retry_info = template.downloader.download(
        on_attempt_failure=on_attempt_failure, **kwargs
    )
    if fp is None:
        raise DownloadException("Market data download failed: null file pointer")
    meta.response = response

    checksum = generate_checksum_from_file(fp)
    meta.download_checksum = checksum
    man = CacheManager()
    if Path(man.cache_path(meta.download_folder)).exists():
        raise DuplicatedFolderException(
            f"Market data download failed: download folder {meta.download_folder} already exists"
        )
    # DownloadException must be raised before creating download folder
    # after this any exception can be raised and it will clean up the download folder
    man.create_download_folder(meta)

    fname = f"downloaded.{template.downloader.format}"
    file_rel_path = str(Path(meta.download_folder) / fname)
    with Path(man.cache_path(file_rel_path)).open("wb") as fp_dest:
        shutil.copyfileobj(fp, fp_dest)
    fp.close()

    downloaded_files = _process_downloaded_format(template, file_rel_path, man, meta)
    meta.downloaded_files = downloaded_files
    _validate_downloaded_files(template, downloaded_files, man)
    _gzip_downloaded_files(downloaded_files, man, meta)
    return retry_info
