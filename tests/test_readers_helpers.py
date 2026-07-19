"""Tests for brasa.readers.helpers."""

from brasa.engine.template import MarketDataTemplate
from brasa.fieldsets import Fieldset

SETTLEMENT_TEMPLATE = "brasa/files/templates/b3/futures/b3-futures-settlement-prices.yaml"


def test_template_fields_is_a_fieldset_equivalent_to_from_template_fields():
    """Regression for audit Q8.1.

    template.fields is a Fieldset since the fieldsets refactor, and building
    one via Fieldset.from_template_fields must yield the same field types —
    so PandasAdapter accepts either construction interchangeably.
    """
    template = MarketDataTemplate(SETTLEMENT_TEMPLATE)

    assert isinstance(template.fields, Fieldset)

    rebuilt = Fieldset.from_template_fields(
        template.fields, raw_fields=template.template.get("fields")
    )
    assert [(f.name, f.type_name) for f in template.fields] == [
        (f.name, f.type_name) for f in rebuilt
    ]
