"""
Custom exception classes for the fieldset-schema library.
"""


class FieldError(Exception):
    """Exception raised for field-related errors."""
    pass


class FieldsetError(Exception):
    """Exception raised for fieldset-related errors."""
    pass


class TypeParseError(Exception):
    """Exception raised when type parsing fails."""
    pass


class TypeDefinitionError(Exception):
    """Exception raised when type definition is invalid."""
    pass
