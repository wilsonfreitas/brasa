"""Public API for market data operations.

This module provides the high-level functions for downloading, processing,
and retrieving market data. These are the main entry points for users.
"""

from pathlib import Path
from typing import Any

import pandas as pd
import progressbar

from brasa.util import KwargsIterator

from .cache import CacheManager, CacheMetadata
from .exceptions import DownloadException
from .template import retrieve_template


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


def download_marketdata(template_name: str, reprocess: bool = False, **kwargs) -> None:
    """Download market data for multiple dates/parameters.

    Downloads data in batch mode with progress bar. Supports downloading
    for multiple dates or parameter combinations.

    Args:
        template_name: Name of the template to use.
        reprocess: If True, force re-download even if data exists.
        **kwargs: Template-specific download arguments. Lists are expanded
                  to download for each combination.
    """
    template = retrieve_template(template_name)
    cache = CacheManager()
    kwargs_iter = KwargsIterator(kwargs)
    widgets = [
        f"D {template_name} ",
        progressbar.SimpleProgress(format="%(value_s)3s/%(max_value_s)-3s"),
        progressbar.Bar(),
        " ",
        progressbar.Timer(),
    ]
    for args in progressbar.progressbar(
        kwargs_iter, max_value=len(kwargs_iter), widgets=widgets
    ):
        meta = CacheMetadata(template.id)
        meta.extra_key = template.downloader.extra_key
        meta.download_args = args
        meta.downloaded_files = []
        meta.processed_files = {}
        if reprocess:
            if cache.has_meta(meta):
                cache.load_meta(meta)
                cache.remove_meta(meta)
            cache.download_marketdata(meta)
        elif not cache.has_successful_trial(meta):
            if cache.has_meta(meta):
                cache.load_meta(meta)
                check = all(
                    Path(cache.cache_path(f)).exists() for f in meta.downloaded_files
                )
                if not check:
                    cache.download_marketdata(meta)
            else:
                cache.download_marketdata(meta)


def process_marketdata(template_name: str, reprocess: bool = False) -> None:
    """Process all downloaded data for a template.

    Reads raw downloaded files and converts them to parquet format.
    Shows progress bar during processing.

    Args:
        template_name: Name of the template to process.
        reprocess: If True, reprocess even if already processed.
    """
    template = retrieve_template(template_name)
    cache = CacheManager()
    with cache.meta_db_connection as conn:
        c = conn.cursor()
        c.execute("select id from cache_metadata where template = ?", (template_name,))
        rows = c.fetchall()
        widgets: list[Any] = [
            f"P {template_name} ",
            progressbar.SimpleProgress(format="%(value_s)3s/%(max_value_s)-3s"),
            progressbar.Bar(),
            " ",
            progressbar.Timer(),
        ]
        errors = []
        with progressbar.ProgressBar(max_value=len(rows), widgets=widgets) as pbar:
            for meta_row in rows:
                pbar.update(pbar.value + 1)
                _meta = cache._load_meta_dict_by_id(meta_row[0])
                meta = CacheMetadata(template.id)
                meta.from_dict(_meta)
                try:
                    if reprocess or len(meta.processed_files) == 0:
                        meta.processing_errors = ""
                        cache.read_marketdata(meta)
                except Exception as ex:
                    errors.append((meta, ex))
                    meta.processing_errors = str(ex)
                    cache.save_meta(meta)

        if len(errors) > 0:
            for err in errors:
                print(err[0].download_args, err[1])


def process_etl(template_name: str) -> None:
    """Run an ETL process defined in a template.

    ETL templates define custom processing logic that transforms
    multiple data sources into derived datasets.

    Args:
        template_name: Name of the ETL template to run.
    """
    widgets: list[Any] = [
        f"ETL {template_name} ",
        progressbar.SimpleProgress(format="%(value_s)3s/%(max_value_s)-3s"),
        progressbar.Bar(),
        " ",
        progressbar.Timer(),
    ]

    with progressbar.ProgressBar(max_value=1, widgets=widgets) as bar:
        bar.update(1)
        template = retrieve_template(template_name)
        template.etl.process_function(template.etl)
