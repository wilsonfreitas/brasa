"""Custom exceptions for the engine module."""


class DownloadException(Exception):
    """Raised when a market data download fails."""

    pass


class DuplicatedFolderException(Exception):
    """Raised when attempting to create a download folder that already exists."""

    pass
