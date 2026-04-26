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
