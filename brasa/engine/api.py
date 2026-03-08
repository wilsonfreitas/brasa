"""Public API for market data operations.

This module handles the downloading of market data from remote sources,
including file format handling (zip, base64), validation, and compression.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from datetime import datetime
from pathlib import Path

import pandas as pd

from brasa.util import KwargsIterator

from .cache import CacheManager, CacheMetadata, DownloadResult
from .exceptions import DownloadException
from .reporting import (
    DownloadAttemptStatus,
    TaskReport,
    TaskResult,
    Verbosity,
    capture_warnings,
    create_task_result_from_exception,
    create_task_result_skipped,
    create_task_result_success,
    to_task_status,
)
from .template import retrieve_template


def _should_download(cache: CacheManager, meta: CacheMetadata, reprocess: bool) -> bool:  # noqa: PLR0911
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

    if cache.has_meta(meta):
        cache.load_meta(meta)
        # If metadata is marked as invalid, skip unless forced with reprocess
        if meta.is_invalid_download:
            return False

    # REQ-011: Treat last status D (DUPLICATED) as skip-eligible
    last_status = cache.get_last_download_status(meta)
    if last_status and last_status["code"] == "D":
        # REQ-012: Guard - redownload if raw files are missing
        if cache.has_meta(meta):
            raw_files_exist = all(
                Path(cache.cache_path(f)).exists() for f in meta.downloaded_files
            )
            return not raw_files_exist
        # No metadata but last trial was D - redownload
        return False

    # Trial-based skip for INVALID — permanent, no meta row saved
    if last_status and last_status["code"] == "I":
        return False

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


def _build_result_from_download(
    dl: DownloadResult,
    template_name: str,
    args: dict,
    duration: float,
    meta: CacheMetadata,
    captured_warnings: list[str],
) -> "TaskResult":
    """Build a TaskResult from a DownloadResult.

    Maps success/failure to the appropriate TaskResult constructor
    and attaches download status metadata to extra_info.

    Args:
        dl: The DownloadResult returned by CacheManager.download_marketdata.
        template_name: Name of the template.
        args: Download arguments for this iteration.
        duration: Elapsed time in seconds.
        meta: Cache metadata (may be reloaded after download).
        captured_warnings: Warnings captured during the download.

    Returns:
        A TaskResult with download status attached.
    """
    if dl.is_success:
        result = create_task_result_success(
            operation="download",
            template_name=template_name,
            args=args,
            duration=duration,
            downloaded_files=meta.downloaded_files,
            is_processed=meta.is_processed,
            captured_warnings=captured_warnings,
        )
    else:
        result = create_task_result_from_exception(
            exception=dl.exception or Exception(dl.reason),
            operation="download",
            template_name=template_name,
            args=args,
            duration=duration,
            downloaded_files=meta.downloaded_files,
            is_processed=meta.is_processed,
            captured_warnings=captured_warnings,
            is_expected_error=dl.is_expected_error,
        )

    result.extra_info["download_status_code"] = dl.status_code
    result.extra_info["download_status_name"] = dl.status_name
    result.extra_info["download_status_reason"] = dl.reason

    # Override generic status with the precise download outcome
    result.status = to_task_status(DownloadAttemptStatus(dl.status_name.lower()))
    if dl.http_status is not None:
        result.extra_info["http_status"] = str(dl.http_status)

    # Retry telemetry (REQ-011)
    if dl.retry_attempts_configured is not None:
        result.extra_info["retry_attempts_configured"] = str(
            dl.retry_attempts_configured
        )
    if dl.retry_attempts_used is not None:
        result.extra_info["retry_attempts_used"] = str(dl.retry_attempts_used)
    if dl.retry_success_on_attempt is not None:
        result.extra_info["retry_success_on_attempt"] = str(dl.retry_success_on_attempt)

    return result


def _build_result_skipped(
    cache: CacheManager,
    meta: CacheMetadata,
    template_name: str,
    args: dict,
    duration: float,
) -> "TaskResult":
    """Build a SKIPPED TaskResult with an appropriate reason.

    Args:
        cache: Cache manager instance.
        meta: Cache metadata (loaded during _should_download).
        template_name: Name of the template.
        args: Download arguments for this iteration.
        duration: Elapsed time in seconds.

    Returns:
        A TaskResult with SKIPPED status and reason.
    """
    skip_reason = "already downloaded"
    if meta.is_invalid_download:
        skip_reason = f"invalid download: {meta.invalid_download_reason}"
    else:
        last = cache.get_last_download_status(meta)
        if last and last["code"] == "D":
            skip_reason = "duplicated (cached)"
        elif last and last["code"] == "I":
            skip_reason = f"invalid download: {last['reason']}"

    result = create_task_result_skipped(
        operation="download",
        template_name=template_name,
        args=args,
        duration=duration,
        downloaded_files=meta.downloaded_files,
        is_processed=meta.is_processed,
    )
    result.extra_info["download_status_code"] = "S"
    result.extra_info["download_status_name"] = "SKIPPED"
    result.extra_info["download_status_reason"] = skip_reason
    return result


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

    If the template declares a ``dependencies`` block, upstream templates
    are automatically executed and their output is used to populate any
    missing download args before downloads begin.

    Args:
        template_name: Name of the template to use.
        reprocess: If True, force re-download even if data exists.
        verbosity: Output verbosity level (QUIET, NORMAL, VERBOSE).
        report_file: Optional path to save the report (JSON or TXT).
        **kwargs: Template-specific download arguments. Lists are expanded
                  to download for each combination.

    Returns:
        TaskReport with results of all download operations.

    Raises:
        DependencyResolutionError: If a required dependency cannot be
            resolved. No downloads are started in this case.
    """
    from .dependency_resolver import resolve_dependencies

    template = retrieve_template(template_name)

    # Resolve declared dependencies and inject missing args
    implicit_reports: list[TaskReport] = []
    resolved = resolve_dependencies(
        template, kwargs, _implicit_reports=implicit_reports
    )
    kwargs = {**kwargs, **resolved}

    cache = CacheManager()
    kwargs_iter = KwargsIterator(kwargs)

    # Get download delay from template (default 0)
    download_delay = template.downloader.download_delay

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
        meta.is_processed = False

        with capture_warnings() as captured_warnings:
            should_download = _should_download(cache, meta, reprocess)

            if should_download:
                # Apply delay between downloads (not before the first one)
                if download_delay > 0:
                    time.sleep(download_delay)

                dl = cache.download_marketdata(meta)

                # Reload meta to get downloaded files
                if cache.has_meta(meta):
                    cache.load_meta(meta)

                duration = (datetime.now() - start_time).total_seconds()
                result = _build_result_from_download(
                    dl,
                    template_name,
                    args,
                    duration,
                    meta,
                    captured_warnings,
                )
            else:
                # Task was skipped (already downloaded or duplicated/invalid)
                duration = (datetime.now() - start_time).total_seconds()
                result = _build_result_skipped(
                    cache,
                    meta,
                    template_name,
                    args,
                    duration,
                )

        report.add_result(result)

    report.finish()
    report.dependency_reports = implicit_reports

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

    with closing(cache.meta_db_connection) as conn, conn:
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
                should_process = reprocess or not meta.is_processed

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
                        is_processed=meta.is_processed,
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
                        is_processed=meta.is_processed,
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
                    is_processed=meta.is_processed,
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
    resolve_dependencies: bool = False,
    force: bool = False,
) -> TaskReport:
    """Run an ETL process defined in a template.

    ETL templates define custom processing logic that transforms
    multiple data sources into derived datasets. Supports both:
    - Function-based ETL (legacy): Uses a Python function for transformation.
    - Pipeline-based ETL (new): Uses declarative pipeline steps.

    When ``resolve_dependencies=True``, the function uses the
    :class:`~brasa.engine.orchestrator.PipelineOrchestrator` to
    automatically discover and execute upstream dependencies before
    running the target template.  The returned ``TaskReport`` is for
    the target template only; the full orchestration report is logged.

    Args:
        template_name: Name of the ETL template to run.
        verbosity: Output verbosity level (QUIET, NORMAL, VERBOSE).
        report_file: Optional path to save the report (JSON or TXT).
        resolve_dependencies: If ``True``, automatically process stale
            upstream dependencies before running this ETL.  Defaults to
            ``False`` for backward compatibility.
        force: If ``True`` (and ``resolve_dependencies=True``), re-execute
            all upstream templates regardless of staleness.

    Returns:
        TaskReport with results of the ETL operation.
    """
    if resolve_dependencies:
        from .orchestrator import PipelineOrchestrator

        orchestrator = PipelineOrchestrator()
        orch_report = orchestrator.execute(
            template_name,
            force=force,
            verbosity=verbosity,
        )

        # Return the target template's report if available,
        # otherwise return the last report from the orchestration
        target_report = orch_report.step_reports.get(template_name)
        if target_report is not None:
            if report_file:
                report_path = Path(report_file)
                file_format = "json" if report_path.suffix == ".json" else "txt"
                target_report.save_report(report_path, format=file_format)
            return target_report

        # If the target was skipped or orchestration failed before
        # reaching it, fall through to direct execution below only
        # if the orchestration succeeded (all upstreams fresh).
        # If orchestration failed, create an error report.
        if not orch_report.success:
            report = TaskReport(
                operation="etl",
                template_name=template_name,
                verbosity=verbosity,
            )
            report.start(total=1)
            result = create_task_result_from_exception(
                exception=RuntimeError(
                    "Upstream dependency processing failed. Check logs for details."
                ),
                operation="etl",
                template_name=template_name,
                args={},
                duration=orch_report.total_duration,
                is_expected_error=True,
            )
            report.add_result(result)
            report.finish()

            if report_file:
                report_path = Path(report_file)
                file_format = "json" if report_path.suffix == ".json" else "txt"
                report.save_report(report_path, format=file_format)

            return report

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


def get_dependency_graph():
    """Build and return the template dependency graph.

    Scans all pipeline-based templates and constructs a directed acyclic
    graph (DAG) of their dependencies.  Legacy function-based templates
    are excluded.

    Returns:
        A ``TemplateDependencyGraph`` instance.
    """
    from .dependency_graph import TemplateDependencyGraph

    return TemplateDependencyGraph()


def get_execution_plan(template_id: str, force: bool = False):
    """Compute an execution plan for processing a template.

    Builds the dependency graph, determines topological order,
    checks staleness for each upstream template, and returns a plan
    describing which steps need execution.

    Args:
        template_id: The target template to process.
        force: If ``True``, all ancestors are marked for execution
            regardless of staleness.

    Returns:
        An ``ExecutionPlan`` with ordered ``ExecutionStep`` entries.

    Raises:
        KeyError: If *template_id* is not in the dependency graph.
    """
    from .dependency_graph import TemplateDependencyGraph

    graph = TemplateDependencyGraph()
    return graph.get_execution_plan(template_id, force=force)
