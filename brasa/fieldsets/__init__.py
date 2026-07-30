"""
Declarative field/type system for brasa templates.

Defines dataset schemas (Fieldset/Field) and applies them via the
pandas adapter (type coercion) and the pyarrow schema builder.
"""

from .adapters.pandas_adapter import PandasAdapter
from .adapters.pyarrow_adapter import get_target_schema
from .field import Field
from .fieldset import Fieldset

__all__ = [
    "Field",
    "Fieldset",
    "PandasAdapter",
    "get_target_schema",
]
