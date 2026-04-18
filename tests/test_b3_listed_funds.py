from unittest.mock import MagicMock

import pandas as pd

from brasa.engine import MarketDataTemplate, retrieve_template
from brasa.engine.pipeline.context import PipelineContext
from brasa.engine.pipeline.steps.column_steps import AddColumnStep
from brasa.fieldsets import Fieldset


def test_load_b3_listed_funds_template():
    tpl = MarketDataTemplate("templates/b3/equities/b3-listed-funds.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
    assert tpl.id == "b3-listed-funds"


def test_b3_listed_funds_template_fields():
    tpl = MarketDataTemplate("templates/b3/equities/b3-listed-funds.yaml")

    assert isinstance(tpl.fields, Fieldset)
    expected = {
        "fund_id": "string",
        "type": "string",
        "acronym": "string",
        "fund_name": "string",
        "trading_name": "string",
        "refdate": "date",
    }
    assert len(tpl.fields) == len(expected)
    for name, type_name in expected.items():
        assert tpl.fields[name].type_name == type_name, name


def test_retrieve_b3_listed_funds_template():
    tpl = retrieve_template("b3-listed-funds")
    assert tpl is not None
    assert tpl.id == "b3-listed-funds"


def test_add_column_injects_typefund_from_download_args():
    """Verifies the critical pipeline step: the `type` column is populated
    per-call from the `typeFund` download arg (replaces the four old
    set_column literals).
    """
    meta = MagicMock()
    meta.download_args = {"typeFund": "ETF-CRIPTO"}
    meta.extra_key = None
    context = PipelineContext(meta=meta, reader_config={})

    df = pd.DataFrame({"acronym": ["BOVA", "IVVB"]})

    step = AddColumnStep(
        params={"name": "type", "from": {"where": "download_args", "key": "typeFund"}},
    )
    result = step.execute(df, context)

    assert "type" in result.columns
    assert list(result["type"]) == ["ETF-CRIPTO", "ETF-CRIPTO"]


def test_b3_listed_funds_consolidated_template():
    tpl = MarketDataTemplate("templates/b3/equities/b3-listed-funds-consolidated.yaml")
    assert tpl.id == "b3-listed-funds-consolidated"
    assert hasattr(tpl, "etl") and tpl.etl is not None

    expected = {
        "refdate",
        "asset_name",
        "symbol",
        "fund_name",
        "fund_type",
        "fund_id",
        "trading_name",
    }
    assert {f.name for f in tpl.fields} == expected
