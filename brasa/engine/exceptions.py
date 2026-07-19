"""Custom exceptions for the engine module."""


class DownloadException(Exception):
    """Raised when a market data download fails."""

    pass


class DuplicatedFolderException(Exception):
    """Raised when attempting to create a download folder that already exists."""

    pass


class InvalidContentException(Exception):
    """Raised when downloaded content is invalid or fails validation.

    Used for permanent validation failures (e.g. empty files, no data
    available for the requested parameters). Downloads marked with this
    exception are skipped in future trials.
    """

    pass


class CorruptedContentException(Exception):
    """Raised when downloaded content is corrupted but may succeed on retry.

    Used for transient validation failures (e.g. truncated files,
    encoding errors, incomplete archives due to network issues).
    Unlike InvalidContentException, downloads marked with this
    exception are retried in future trials.
    """

    pass


class DependencyResolutionError(Exception):
    """Raised when a template dependency cannot be resolved.

    Used when a required upstream dataset cannot be processed or its
    SQL query returns no rows. Aborts before any download starts.
    """

    pass


class CacheError(Exception):
    """Raised for cache management errors (missing entries, invalid IDs)."""

    pass


class NoDataException(Exception):
    """The source responded successfully but has no data for this request.

    Non-retriable and non-error: e.g. B3's pregão endpoint returns a valid
    but empty (0-entry) zip for dates outside its retention window. Distinct
    from InvalidContentException so it is not persisted as a permanent skip.
    """

    pass


#: Domain exceptions that carry classification meaning for the engine.
#: Pipeline executors re-raise these unwrapped so callers can match on type.
DOMAIN_EXCEPTIONS: tuple[type[Exception], ...] = (
    DownloadException,
    DuplicatedFolderException,
    InvalidContentException,
    CorruptedContentException,
    DependencyResolutionError,
    CacheError,
    NoDataException,
)
