"""Test script for the pipeline-based reader system.

This script tests the new pipeline infrastructure by:
1. Listing available steps
2. Testing step creation from configuration
3. Testing pipeline construction and execution
"""

from brasa.engine.pipeline import ReaderPipeline, StepRegistry


def test_step_registry():
    """Test that steps are properly registered."""
    steps = StepRegistry.list_steps()

    # Check that we have registered steps
    assert len(steps) > 0, "No steps registered"

    # Check that each step has a class
    for step in steps:
        step_class = StepRegistry.get(step)
        assert step_class is not None, f"Step {step} has no class"

    # Check for expected steps
    expected_steps = [
        "read_html",
        "read_csv",
        "first_table",
        "set_columns",
        "apply_fields",
        "parse_numeric",
    ]
    for expected in expected_steps:
        assert expected in steps, f"Expected step '{expected}' not found"


def test_step_creation():
    """Test creating steps from configuration."""
    # Test creating various steps
    test_configs = [
        {"step": "read_html", "attrs": {"id": "myTable"}},
        {"step": "first_table"},
        {"step": "set_columns", "names": ["col1", "col2", "col3"]},
        {"step": "parse_numeric", "columns": ["price", "quantity"]},
        {"step": "apply_fields", "errors": "coerce"},
    ]

    for config in test_configs:
        step_name = config["step"]
        step = StepRegistry.create(step_name, config)
        assert step is not None, f"Failed to create step {step_name}"
        assert step.name == step_name, f"Step name mismatch: {step.name} != {step_name}"


def test_pipeline_construction():
    """Test constructing a pipeline from configuration."""
    pipeline_config = [
        {"step": "read_html", "attrs": {"id": "tblDadosAjustes"}},
        {"step": "first_table"},
        {
            "step": "set_columns",
            "names": ["col1", "col2", "col3", "col4", "col5", "col6"],
        },
        {"step": "apply_fields"},
    ]

    pipeline = ReaderPipeline.from_config(pipeline_config)

    assert pipeline is not None, "Failed to create pipeline"
    assert len(pipeline) == 4, f"Expected 4 steps, got {len(pipeline)}"


def test_template_loading():
    """Test loading a template with pipeline configuration."""
    from brasa.engine.template import retrieve_template

    # Load a template with pipeline-based reader
    template = retrieve_template("b3-futures-settlement-prices")
    assert template.id == "b3-futures-settlement-prices"
    assert template.has_reader
    assert template.reader.has_pipeline
    assert template.reader.read_function is None


def test_etl_pipeline_template_loading():
    """Test loading an ETL template with pipeline configuration."""
    from brasa.engine.template import retrieve_template

    # Load a legacy function-based ETL template
    template = retrieve_template("b3-curves-dap")
    assert template.id == "b3-curves-dap"
    assert template.is_etl
    assert not template.etl.is_pipeline
    assert template.etl.process_function is not None
    assert "b3-futures-dap" in template.etl.get_input_datasets()

    # Load a new pipeline-based ETL template
    template = retrieve_template("b3-futures-settlement-prices-consolidated")
    assert template.id == "b3-futures-settlement-prices-consolidated"
    assert template.is_etl
    assert template.etl.is_pipeline
    assert template.etl.pipeline is not None
    assert len(template.etl.pipeline) == 1
    assert "b3-futures-settlement-prices" in template.etl.get_input_datasets()


def test_etl_step_registry():
    """Test the ETL step registry."""
    from brasa.engine.pipeline import ETLStepRegistry

    # Check that all built-in steps are registered
    steps = ETLStepRegistry.get_all_steps()
    assert "load" in steps
    assert "filter" in steps
    assert "select" in steps
    assert "sort" in steps
    assert "to_dataframe" in steps
    # New steps using shared transforms
    assert "drop_columns" in steps
    assert "rename_columns" in steps
    assert "drop_duplicates" in steps
    assert "fill_na" in steps


def test_shared_transforms():
    """Test that shared transforms can be used directly."""
    import pandas as pd

    from brasa.engine.pipeline import shared_transforms

    # Create test data
    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 2, 1],
            "b": ["x", "y", "z", "y", "x"],
            "c": [10.0, 20.0, 30.0, 40.0, 50.0],
        }
    )

    # Test filter_data
    filtered = shared_transforms.filter_data(df, {"a": 2})
    assert len(filtered) == 2

    # Test select_columns
    selected = shared_transforms.select_columns(df, ["a", "b"])
    assert list(selected.columns) == ["a", "b"]

    # Test sort_data
    sorted_df = shared_transforms.sort_data(df, "c", descending=True)
    assert sorted_df["c"].iloc[0] == 50.0

    # Test drop_duplicates
    deduped = shared_transforms.drop_duplicates(df, subset=["a"])
    assert len(deduped) == 3

    # Test rename_columns
    renamed = shared_transforms.rename_columns(df, {"a": "alpha"})
    assert "alpha" in renamed.columns
    assert "a" not in renamed.columns


def test_add_column_where_now():
    """Test that add_column with from.where: now adds a datetime column."""
    from datetime import datetime
    from unittest.mock import MagicMock

    import pandas as pd

    from brasa.engine.pipeline.context import PipelineContext
    from brasa.engine.pipeline.steps.column_steps import AddColumnStep

    # Build minimal context (download_args not needed for where: now)
    meta = MagicMock()
    meta.download_args = {}
    meta.extra_key = None
    context = PipelineContext(meta=meta, reader_config={})

    df = pd.DataFrame({"value": [1, 2, 3]})

    step = AddColumnStep(
        params={"name": "downloaded_at", "from": {"where": "now"}},
    )

    before = datetime.now()
    result = step.execute(df, context)
    after = datetime.now()

    assert "downloaded_at" in result.columns
    ts = result["downloaded_at"].iloc[0]
    assert isinstance(ts, datetime)
    assert before <= ts <= after


def test_standard_terms_interpolation_transform():
    """standard_terms_interpolation flat-forward interpolates a curve per refdate."""
    from datetime import date

    import numpy as np
    import pandas as pd
    import pytest
    from bizdays import Calendar

    from brasa.engine.pipeline.steps import shared_transforms

    # One refdate, three knots
    df = pd.DataFrame(
        {
            "refdate": pd.to_datetime([date(2020, 1, 2)] * 3),
            "business_days": [21, 252, 504],
            "adjusted_tax": [0.10, 0.11, 0.12],
        }
    )

    result = shared_transforms.standard_terms_interpolation(
        df,
        standard_terms=[21, 126, 252, 756],
        symbol_prefix="DI1T",
        calendar="ANBIMA",
    )

    # One row per standard term, in term order
    assert list(result["business_days"]) == [21, 126, 252, 756]
    assert list(result["symbol"]) == ["DI1T21", "DI1T126", "DI1T252", "DI1T756"]

    by_term = dict(zip(result["business_days"], result["adjusted_tax"], strict=True))
    # At exact knots the interpolated rate equals the knot rate
    assert by_term[21] == pytest.approx(0.10)
    assert by_term[252] == pytest.approx(0.11)
    # Off-knot (126) and extrapolated (756 > 504) terms match a direct interp_ff call
    knots = np.array([21, 252, 504])
    rates = np.array([0.10, 0.11, 0.12])
    expected = shared_transforms.interp_ff(np.array([126, 756]), rates, knots)
    assert by_term[126] == pytest.approx(expected[0])
    assert by_term[756] == pytest.approx(expected[1])

    # Maturities use the calendar offset from refdate
    mat_by_term = dict(
        zip(result["business_days"], result["maturity_date"], strict=True)
    )
    cal = Calendar.load("ANBIMA")
    assert (
        pd.Timestamp(mat_by_term[252]).date()
        == pd.Timestamp(cal.offset(date(2020, 1, 2), 252)).date()
    )


def test_standard_terms_step_executes():
    """The standard_terms step interpolates curve vertices via the registry."""
    from datetime import date

    import pandas as pd

    from brasa.engine.pipeline import StepRegistry

    df = pd.DataFrame(
        {
            "refdate": pd.to_datetime([date(2020, 1, 2)] * 2),
            "business_days": [21, 252],
            "adjusted_tax": [0.10, 0.11],
        }
    )

    step = StepRegistry.create(
        "standard_terms",
        {
            "standard_terms": [21, 252],
            "symbol_prefix": "DI1T",
            "calendar": "ANBIMA",
        },
    )
    result = step.execute(df, None)

    assert list(result["symbol"]) == ["DI1T21", "DI1T252"]
    assert list(result["business_days"]) == [21, 252]
    assert "maturity_date" in result.columns
    assert "adjusted_tax" in result.columns
