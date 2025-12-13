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

    # Load the original template (legacy function-based)
    template = retrieve_template("b3-futures-settlement-prices")
    assert template.id == "b3-futures-settlement-prices"
    assert template.has_reader
    assert not template.reader.has_pipeline
    assert template.reader.read_function is not None

    # Load the new pipeline-based template
    template = retrieve_template("b3-futures-settlement-prices-pipeline")
    assert template.id == "b3-futures-settlement-prices-pipeline"
    assert template.has_reader
    assert template.reader.has_pipeline
    assert template.reader.read_function is None
