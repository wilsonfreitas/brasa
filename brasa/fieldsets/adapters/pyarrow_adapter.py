"""PyArrow schema builder for Fieldset."""

import pyarrow as pa

from ..field import Field
from ..fieldset import Fieldset

_TYPE_MAPPING = {
    "integer": pa.int64(),
    "date": pa.date32(),
    "datetime": pa.timestamp("us"),
    "time": pa.time64("us"),
    "boolean": pa.bool_(),
    "string": pa.string(),
    "character": pa.string(),
}


def _pyarrow_type(field: Field) -> pa.DataType:
    if field.type_name == "numeric":
        return pa.float64()
    return _TYPE_MAPPING.get(field.type_name, pa.string())


def get_target_schema(fieldset: Fieldset) -> pa.Schema:
    """Build the target PyArrow schema for a fieldset."""
    return pa.schema(
        [
            pa.field(f.name, _pyarrow_type(f), nullable=True)
            for f in fieldset.get_all_fields()
        ]
    )
