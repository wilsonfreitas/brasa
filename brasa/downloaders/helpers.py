import io
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import IO

from brasa.downloaders.downloaders import (
    B3FilesURLDownloader,
    B3PagedURLEncodedDownloader,
    B3URLEncodedDownloader,
    BCBCurrencyDownloader,
    BCBSGSDownloader,
    DatetimeDownloader,
    FormatURLDownloader,
    SettlementPricesDownloader,
    SimpleDownloader,
)
from brasa.engine import MarketDataDownloader
from brasa.engine.exceptions import DownloadException

logger = logging.getLogger(__name__)


def simple_download(
    md_downloader: MarketDataDownloader, **kwargs
) -> tuple[IO | None, dict[str, str]]:
    downloader = SimpleDownloader(md_downloader.url, md_downloader.verify_ssl, **kwargs)
    return downloader.download(), dict(downloader.response.headers)


def datetime_download(
    md_downloader: MarketDataDownloader, **kwargs
) -> tuple[IO | None, dict[str, str]]:
    downloader = DatetimeDownloader(
        md_downloader.url, md_downloader.verify_ssl, **kwargs
    )
    return downloader.download(), dict(downloader.response.headers)


def format_download(
    md_downloader: MarketDataDownloader, **kwargs
) -> tuple[IO | None, dict[str, str]]:
    downloader = FormatURLDownloader(
        md_downloader.url, md_downloader.verify_ssl, **kwargs
    )
    return downloader.download(), dict(downloader.response.headers)


def b3_url_encoded_download(
    md_downloader: MarketDataDownloader, **kwargs
) -> tuple[IO | None, dict[str, str]]:
    downloader = B3URLEncodedDownloader(
        md_downloader.url, md_downloader.verify_ssl, **kwargs
    )
    return downloader.download(), dict(downloader.response.headers)


def b3_paged_url_encoded_download(
    md_downloader: MarketDataDownloader, **kwargs
) -> tuple[IO | None, dict[str, str]]:
    downloader = B3PagedURLEncodedDownloader(
        md_downloader.url, md_downloader.verify_ssl, **kwargs
    )
    return downloader.download(), dict(downloader.response.headers)


def settlement_prices_download(
    md_downloader: MarketDataDownloader, **kwargs
) -> tuple[IO | None, dict[str, str]]:
    downloader = SettlementPricesDownloader(
        md_downloader.url, md_downloader.verify_ssl, **kwargs
    )
    return downloader.download(), dict(downloader.response.headers)


def b3_files_download(
    md_downloader: MarketDataDownloader, **kwargs
) -> tuple[IO | None, dict[str, str]]:
    downloader = B3FilesURLDownloader(
        md_downloader.url, md_downloader.verify_ssl, **kwargs
    )
    return downloader.download(), dict(downloader.response.headers)


def bcb_sgs_download(
    _md_downloader: MarketDataDownloader, **kwargs
) -> tuple[IO | None, dict[str, str]]:
    downloader = BCBSGSDownloader(**kwargs)
    return downloader.download(), {}


def bcb_currency_download(
    _md_downloader: MarketDataDownloader, **kwargs
) -> tuple[IO | None, dict[str, str]]:
    downloader = BCBCurrencyDownloader(**kwargs)
    return downloader.download(), {}


def validate_empty_file(fname: str) -> None:
    with Path(fname).open("rb") as fp:
        fp.seek(0, io.SEEK_END)
        size = fp.tell()
    if size == 0:
        raise Exception("Downloaded file is empty")


def validate_json_empty_file(fname: str) -> None:
    with Path(fname).open("rb") as fp:
        if fp.readlines() == []:
            raise Exception("JSON file is empty")
        fp.seek(0)
        obj = json.load(fp)
        if len(obj) == 0:
            raise Exception("JSON file is empty")


def _render_import_path(raw_path: str, kwargs: dict) -> str:
    """Render an import path pattern: strftime(refdate) then format(**rest).

    Args:
        raw_path: Path pattern, may contain strftime codes and ``{name}``
            placeholders.
        kwargs: Acquisition arguments. A ``refdate`` of type date/datetime
            drives strftime; remaining non-date values fill ``.format``.

    Returns:
        The concrete rendered path.

    Raises:
        DownloadException: If a ``{placeholder}`` has no matching argument.
    """
    rendered = raw_path
    refdate = kwargs.get("refdate")
    if isinstance(refdate, (datetime, date)):
        rendered = refdate.strftime(rendered)
    fmt_kwargs = {
        k: v for k, v in kwargs.items() if not isinstance(v, (datetime, date))
    }
    try:
        rendered = rendered.format(**fmt_kwargs)
    except KeyError as e:
        raise DownloadException(
            f"import path placeholder {e} has no matching argument: {raw_path}"
        ) from e
    return rendered


def local_file_import(
    md_downloader, _import_path: str | None = None, **kwargs
) -> tuple[IO, dict]:
    """Acquisition function that reads bytes from a local file.

    Mirrors the download-function contract ``fn(md, **kwargs) -> (fp, headers)``
    but the transport is the local filesystem. The returned dict carries local
    provenance instead of HTTP headers.

    Args:
        md_downloader: The MarketDataDownloader (provides ``path`` / ``format``).
        _import_path: Explicit path pattern (from ``--path`` / ``path=`` override).
            Takes precedence over a ``path`` kwarg and over ``md_downloader.path``.
        **kwargs: Acquisition arguments used to render the path pattern.

    Returns:
        Tuple of (open binary file pointer, provenance dict).

    Raises:
        DownloadException: If no path is provided or the source file is missing.
    """
    raw_path = (
        _import_path or kwargs.pop("path", None) or getattr(md_downloader, "path", None)
    )
    if raw_path is None:
        raise DownloadException(
            "no import path provided (set --path or a template path:)"
        )

    path = _render_import_path(raw_path, kwargs)
    source = Path(path)
    if not source.is_file():
        raise DownloadException(f"import source not found: {path}")

    fmt = (getattr(md_downloader, "format", "") or "").lower()
    suffix = source.suffix.lstrip(".").lower()
    if fmt and suffix and suffix != fmt:
        logger.warning(
            "import file extension '.%s' does not match template format '%s': %s",
            suffix,
            fmt,
            path,
        )

    stat = source.stat()
    provenance = {
        "acquisition": "import",
        "source_path": str(source.resolve()),
        "original_name": source.name,
        "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "source_size": stat.st_size,
        "imported_at": datetime.now().isoformat(),
    }
    return source.open("rb"), provenance
