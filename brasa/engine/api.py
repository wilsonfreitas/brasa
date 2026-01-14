"""Public API for market data operations.

This module provides the high-level functions for downloading, processing,
and retrieving market data. These are the main entry points for users.
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd

from brasa.util import KwargsIterator

from .cache import CacheManager, CacheMetadata
from .exceptions import DownloadException
from .reporting import (
    TaskReport,
    TaskResult,
    Verbosity,
    capture_warnings,
    create_task_result_from_exception,
    create_task_result_skipped,
    create_task_result_success,
)
from .template import retrieve_template


def _should_download(cache: CacheManager, meta: CacheMetadata, reprocess: bool) -> bool:
    """Determine if data should be downloaded.

    Args:
        cache: Cache manager instance.
        meta: Cache metadata for the template.
        reprocess: If True, force re-download.

    Returns:
        True if data should be downloaded, False otherwise.
    """
    if reprocess:
        if cache.has_meta(meta):
            cache.load_meta(meta)
            cache.remove_meta(meta)
        return True

    if not cache.has_successful_trial(meta):
        if cache.has_meta(meta):
            cache.load_meta(meta)
            check = all(
                Path(cache.cache_path(f)).exists() for f in meta.downloaded_files
            )
            if not check:
                return True
        else:
            return True

    return False


def get_marketdata(
    template_name: str, reprocess: bool = False, **kwargs
) -> pd.DataFrame | dict[str, pd.DataFrame] | None:
    """Get market data for a template with caching support.

    This is the main function for retrieving market data. It handles:
    - Downloading data if not cached
    - Returning cached data if available
    - Reprocessing data if requested

    Args:
        template_name: Name of the template to use.
        reprocess: If True, force re-download and reprocessing.
        **kwargs: Template-specific download arguments (e.g., refdate).

    Returns:
        DataFrame or dict of DataFrames with the market data,
        or None if download fails.
    """
    template = retrieve_template(template_name)
    meta = CacheMetadata(template.id)
    meta.download_args = kwargs
    meta.extra_key = template.downloader.extra_key
    cache = CacheManager()
    if reprocess:
        try:
            return cache.process_without_checks(meta)
        except DownloadException:
            return None
        except Exception:
            cache.remove_meta(meta)
            return None
    elif cache.has_meta(meta):
        return cache.load_marketdata(meta, reprocess)
    else:
        return get_marketdata(template_name, reprocess=True, **kwargs)


def download_marketdata(
    template_name: str,
    reprocess: bool = False,
    verbosity: Verbosity = Verbosity.NORMAL,
    report_file: str | Path | None = None,
    **kwargs,
) -> TaskReport:
    """Download market data for multiple dates/parameters.

    Downloads data in batch mode with pytest-style progress display.
    Supports downloading for multiple dates or parameter combinations.

    Args:
        template_name: Name of the template to use.
        reprocess: If True, force re-download even if data exists.
        verbosity: Output verbosity level (QUIET, NORMAL, VERBOSE).
        report_file: Optional path to save the report (JSON or TXT).
        **kwargs: Template-specific download arguments. Lists are expanded
                  to download for each combination.

    Returns:
        TaskReport with results of all download operations.
    """
    template = retrieve_template(template_name)
    cache = CacheManager()
    kwargs_iter = KwargsIterator(kwargs)

    report = TaskReport(
        operation="download",
        template_name=template_name,
        verbosity=verbosity,
    )
    report.start(total=len(kwargs_iter))

    for args in kwargs_iter:
        start_time = datetime.now()

        meta = CacheMetadata(template.id)
        meta.extra_key = template.downloader.extra_key
        meta.download_args = args
        meta.downloaded_files = []
        meta.processed_files = {}

        with capture_warnings() as captured_warnings:
            try:
                should_download = _should_download(cache, meta, reprocess)

                if should_download:
                    cache.download_marketdata(meta)

                    # Reload meta to get downloaded files
                    if cache.has_meta(meta):
                        cache.load_meta(meta)

                    duration = (datetime.now() - start_time).total_seconds()

                    result = create_task_result_success(
                        operation="download",
                        template_name=template_name,
                        args=args,
                        duration=duration,
                        downloaded_files=meta.downloaded_files,
                        processed_files=meta.processed_files,
                        captured_warnings=captured_warnings,
                    )
                else:
                    # Task was skipped (already downloaded)
                    duration = (datetime.now() - start_time).total_seconds()

                    result = create_task_result_skipped(
                        operation="download",
                        template_name=template_name,
                        args=args,
                        duration=duration,
                        downloaded_files=meta.downloaded_files,
                        processed_files=meta.processed_files,
                    )

            except DownloadException as ex:
                duration = (datetime.now() - start_time).total_seconds()
                result = create_task_result_from_exception(
                    exception=ex,
                    operation="download",
                    template_name=template_name,
                    args=args,
                    duration=duration,
                    downloaded_files=meta.downloaded_files,
                    processed_files=meta.processed_files,
                    captured_warnings=captured_warnings,
                    is_expected_error=True,
                )

            except Exception as ex:
                duration = (datetime.now() - start_time).total_seconds()
                result = create_task_result_from_exception(
                    exception=ex,
                    operation="download",
                    template_name=template_name,
                    args=args,
                    duration=duration,
                    downloaded_files=meta.downloaded_files,
                    processed_files=meta.processed_files,
                    captured_warnings=captured_warnings,
                    is_expected_error=False,
                )

        report.add_result(result)

    report.finish()

    if report_file:
        report_path = Path(report_file)
        file_format = "json" if report_path.suffix == ".json" else "txt"
        report.save_report(report_path, format=file_format)

    return report


def process_marketdata(
    template_name: str,
    reprocess: bool = False,
    verbosity: Verbosity = Verbosity.NORMAL,
    report_file: str | Path | None = None,
    max_workers: int = 4,
) -> TaskReport:
    """Process all downloaded data for a template.

    Reads raw downloaded files and converts them to parquet format.
    Uses parallel processing for file I/O with serialized database writes.
    Shows pytest-style progress display during processing.

    Args:
        template_name: Name of the template to process.
        reprocess: If True, reprocess even if already processed.
        verbosity: Output verbosity level (QUIET, NORMAL, VERBOSE).
        report_file: Optional path to save the report (JSON or TXT).
        max_workers: Maximum number of parallel workers for processing.

    Returns:
        TaskReport with results of all processing operations.
    """
    from .processing import _read_marketdata

    template = retrieve_template(template_name)
    cache = CacheManager()

    with cache.meta_db_connection as conn:
        c = conn.cursor()
        c.execute("select id from cache_metadata where template = ?", (template_name,))
        rows = c.fetchall()

    report = TaskReport(
        operation="process",
        template_name=template_name,
        verbosity=verbosity,
    )
    report.start(total=len(rows))

    # Lock for serializing SQLite writes
    db_lock = threading.Lock()

    def process_single(meta_row: tuple) -> TaskResult:
        """Process a single cache entry.

        Args:
            meta_row: Tuple containing the metadata row ID.

        Returns:
            TaskResult with processing result information.
        """
        start_time = datetime.now()

        _meta = cache._load_meta_dict_by_id(meta_row[0])
        meta = CacheMetadata(template.id)
        meta.from_dict(_meta)

        with capture_warnings() as captured_warnings:
            try:
                should_process = reprocess or len(meta.processed_files) == 0

                if should_process:
                    meta.processing_errors = ""

                    # File I/O - parallelizable
                    _read_marketdata(meta)

                    # Serialized DB write
                    with db_lock:
                        cache.save_meta(meta)

                    duration = (datetime.now() - start_time).total_seconds()

                    result = create_task_result_success(
                        operation="process",
                        template_name=template_name,
                        args=meta.download_args,
                        duration=duration,
                        downloaded_files=meta.downloaded_files,
                        processed_files=meta.processed_files,
                        captured_warnings=captured_warnings,
                    )
                else:
                    # Task was skipped (already processed)
                    duration = (datetime.now() - start_time).total_seconds()

                    result = create_task_result_skipped(
                        operation="process",
                        template_name=template_name,
                        args=meta.download_args,
                        duration=duration,
                        downloaded_files=meta.downloaded_files,
                        processed_files=meta.processed_files,
                    )

            except Exception as ex:
                duration = (datetime.now() - start_time).total_seconds()

                # Save error to metadata (serialized)
                meta.processing_errors = str(ex)
                with db_lock:
                    cache.save_meta(meta)

                result = create_task_result_from_exception(
                    exception=ex,
                    operation="process",
                    template_name=template_name,
                    args=meta.download_args,
                    duration=duration,
                    downloaded_files=meta.downloaded_files,
                    processed_files=meta.processed_files,
                    captured_warnings=captured_warnings,
                    is_expected_error=False,
                )

        return result

    # Process in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single, row): row for row in rows}

        for future in as_completed(futures):
            result = future.result()
            report.add_result(result)

    report.finish()

    if report_file:
        report_path = Path(report_file)
        file_format = "json" if report_path.suffix == ".json" else "txt"
        report.save_report(report_path, format=file_format)

    return report


def process_etl(
    template_name: str,
    verbosity: Verbosity = Verbosity.NORMAL,
    report_file: str | Path | None = None,
) -> TaskReport:
    """Run an ETL process defined in a template.

    ETL templates define custom processing logic that transforms
    multiple data sources into derived datasets. Supports both:
    - Function-based ETL (legacy): Uses a Python function for transformation.
    - Pipeline-based ETL (new): Uses declarative pipeline steps.

    Args:
        template_name: Name of the ETL template to run.
        verbosity: Output verbosity level (QUIET, NORMAL, VERBOSE).
        report_file: Optional path to save the report (JSON or TXT).

    Returns:
        TaskReport with results of the ETL operation.
    """
    report = TaskReport(
        operation="etl",
        template_name=template_name,
        verbosity=verbosity,
    )
    report.start(total=1)

    start_time = datetime.now()

    with capture_warnings() as captured_warnings:
        try:
            template = retrieve_template(template_name)

            if template.etl.is_pipeline:
                # New pipeline-based ETL
                writer = getattr(template, "writer", None)
                fields = getattr(template, "fields", None)
                template.etl.pipeline.execute_and_write(
                    template_id=template.id,
                    writer=writer,
                    fields=fields,
                )
            else:
                # Legacy function-based ETL
                template.etl.process_function(template.etl)

            duration = (datetime.now() - start_time).total_seconds()

            result = create_task_result_success(
                operation="etl",
                template_name=template_name,
                args={},
                duration=duration,
                captured_warnings=captured_warnings,
            )

        except Exception as ex:
            duration = (datetime.now() - start_time).total_seconds()

            result = create_task_result_from_exception(
                exception=ex,
                operation="etl",
                template_name=template_name,
                args={},
                duration=duration,
                captured_warnings=captured_warnings,
                is_expected_error=False,
            )

    report.add_result(result)
    report.finish()

    if report_file:
        report_path = Path(report_file)
        file_format = "json" if report_path.suffix == ".json" else "txt"
        report.save_report(report_path, format=file_format)

    return report
