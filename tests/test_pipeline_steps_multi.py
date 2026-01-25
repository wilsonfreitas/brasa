"""
Tests for multi-output pipeline steps.

These tests validate the new pipeline steps for handling multi-dataset
outputs like BVBG087 (b3_read_bvbg087_xml, apply_fields_multi).

Note: Some tests are marked as expected failures (xfail) because they test
functionality that will be implemented as part of the BVBG087 migration.
"""

import gzip
import shutil
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from brasa.engine.pipeline import StepRegistry
from brasa.engine.template import DatasetConfig
from brasa.fieldsets import Field, Fieldset


@pytest.fixture
def bvbg087_test_file():
    """Get the path to the test BVBG087 file, extracted and gzipped like cache does."""
    # Data is in project root's data folder
    data_dir = Path(__file__).parent.parent / "data"
    zip_path = data_dir / "IR210423.zip"

    if not zip_path.exists():
        pytest.skip(f"Test data file not found: {zip_path}")

    # Extract ZIP and gzip the XML file (mimicking cache behavior)
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
            zf.extractall(tmpdir)

        # Gzip the extracted XML file
        xml_path = Path(tmpdir) / names[0]
        gz_path = xml_path.with_suffix(xml_path.suffix + ".gz")
        with xml_path.open("rb") as f_in, gzip.open(gz_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        yield gz_path


@pytest.fixture
def bvbg087_datasets():
    """Create DatasetConfig objects with field tags for BVBG087 testing."""
    # indexes_info dataset
    indexes_fs = Fieldset()
    indexes_fs.add_fields(
        Field(name="refdate", description="Reference date", type_definition="date"),
        Field(
            name="symbol",
            description="Symbol",
            type_definition="string",
            tag="SctyInf/SctyId/TckrSymb",
        ),
        Field(
            name="security_id",
            description="Security ID",
            type_definition="integer",
            tag="SctyInf/FinInstrmId/OthrId/Id",
        ),
        Field(
            name="settlement_price",
            description="Settlement price",
            type_definition="numeric",
            tag="SttlmVal",
        ),
    )

    # iopv_info dataset
    iopv_fs = Fieldset()
    iopv_fs.add_fields(
        Field(name="refdate", description="Reference date", type_definition="date"),
        Field(
            name="symbol",
            description="Symbol",
            type_definition="string",
            tag="SctyId/TckrSymb",
        ),
        Field(
            name="last_price",
            description="Last price",
            type_definition="numeric",
            tag="IndxVal",
        ),
    )

    # bdr_info dataset
    bdr_fs = Fieldset()
    bdr_fs.add_fields(
        Field(name="refdate", description="Reference date", type_definition="date"),
        Field(
            name="symbol",
            description="Symbol",
            type_definition="string",
            tag="SctyId/TckrSymb",
        ),
        Field(
            name="ref_price",
            description="Reference price",
            type_definition="numeric",
            tag="RefPric",
        ),
    )

    return {
        "indexes_info": DatasetConfig(
            name="indexes_info", tag="IndxInf", fields=indexes_fs
        ),
        "iopv_info": DatasetConfig(name="iopv_info", tag="IOPVInf", fields=iopv_fs),
        "bdr_info": DatasetConfig(name="bdr_info", tag="BDRInf", fields=bdr_fs),
    }


@pytest.fixture
def sample_multi_data():
    """Create sample multi-dataset data for testing."""
    return {
        "indexes_info": pd.DataFrame(
            {
                "refdate": ["2021-04-23", "2021-04-23"],
                "symbol": ["IBOV", "IFIX"],
                "settlement_price": ["120000.5", "2800.25"],
            }
        ),
        "iopv_info": pd.DataFrame(
            {
                "refdate": ["2021-04-23"],
                "symbol": ["BOVA11"],
                "last_price": ["100.50"],
            }
        ),
        "bdr_info": pd.DataFrame(
            {
                "refdate": ["2021-04-23"],
                "symbol": ["AAPL34"],
                "ref_price": ["150.00"],
            }
        ),
    }


@pytest.fixture
def sample_fieldsets():
    """Create sample fieldsets for each dataset."""
    indexes_fs = Fieldset()
    indexes_fs.add_fields(
        Field(name="refdate", description="Reference date", type_definition="date"),
        Field(name="symbol", description="Symbol", type_definition="string"),
        Field(
            name="settlement_price",
            description="Settlement price",
            type_definition="numeric",
        ),
    )

    iopv_fs = Fieldset()
    iopv_fs.add_fields(
        Field(name="refdate", description="Reference date", type_definition="date"),
        Field(name="symbol", description="Symbol", type_definition="string"),
        Field(name="last_price", description="Last price", type_definition="numeric"),
    )

    bdr_fs = Fieldset()
    bdr_fs.add_fields(
        Field(name="refdate", description="Reference date", type_definition="date"),
        Field(name="symbol", description="Symbol", type_definition="string"),
        Field(
            name="ref_price", description="Reference price", type_definition="numeric"
        ),
    )

    return {
        "indexes_info": indexes_fs,
        "iopv_info": iopv_fs,
        "bdr_info": bdr_fs,
    }


class TestExistingStepsRegistry:
    """Tests for existing steps in the registry (sanity check)."""

    def test_registry_has_expected_steps(self):
        """Test that registry has expected existing steps."""
        steps = StepRegistry.list_steps()

        expected = [
            "read_html",
            "read_csv",
            "apply_fields",
            "parse_numeric",
            "parse_date",
        ]

        for step_name in expected:
            assert step_name in steps, f"Step '{step_name}' not found in registry"

    def test_b3_read_bvbg086_xml_exists(self):
        """Test that the BVBG086 XML step exists."""
        steps = StepRegistry.list_steps()
        assert "b3_read_bvbg086_xml" in steps


class TestBVBG087XmlStep:
    """Tests for the b3_read_bvbg087_xml pipeline step.

    These tests are marked as xfail because the step will be implemented
    as part of the BVBG087 migration.
    """

    @pytest.mark.xfail(reason="b3_read_bvbg087_xml step not yet implemented")
    def test_step_is_registered(self):
        """Test that b3_read_bvbg087_xml step is registered."""
        steps = StepRegistry.list_steps()
        assert "b3_read_bvbg087_xml" in steps

    @pytest.mark.xfail(reason="b3_read_bvbg087_xml step not yet implemented")
    def test_step_can_be_created(self):
        """Test that the step can be created from config."""
        step = StepRegistry.create(
            "b3_read_bvbg087_xml", {"step": "b3_read_bvbg087_xml"}
        )
        assert step is not None
        assert step.name == "b3_read_bvbg087_xml"

    @pytest.mark.xfail(reason="b3_read_bvbg087_xml step not yet implemented")
    def test_step_returns_dict_of_dataframes(self, bvbg087_test_file, bvbg087_datasets):
        """Test that step returns dict of DataFrames."""
        from brasa.engine.pipeline.context import PipelineContext
        from brasa.engine.pipeline.steps.b3_steps import B3ReadBVBG087XmlStep

        # Create mock context with datasets
        mock_meta = MagicMock()
        mock_meta.downloaded_files = ["dummy_path.gz"]

        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            datasets=bvbg087_datasets,
        )

        # Patch the downloaded_file property to return our test file
        with patch.object(
            PipelineContext,
            "downloaded_file",
            new_callable=lambda: property(lambda self: str(bvbg087_test_file)),
        ):
            step = B3ReadBVBG087XmlStep({"step": "b3_read_bvbg087_xml"})
            result = step.execute(None, context)

        assert isinstance(result, dict)
        assert len(result) == 3

    @pytest.mark.xfail(reason="b3_read_bvbg087_xml step not yet implemented")
    def test_step_uses_output_names_as_keys(self, bvbg087_test_file, bvbg087_datasets):
        """Test that step uses output names (not XML tags) as dict keys."""
        from brasa.engine.pipeline.context import PipelineContext
        from brasa.engine.pipeline.steps.b3_steps import B3ReadBVBG087XmlStep
        from brasa.engine.template import DatasetConfig

        # Create datasets config
        datasets = {
            "indexes_info": DatasetConfig(
                name="indexes_info", tag="IndxInf", fields=Fieldset()
            ),
            "iopv_info": DatasetConfig(
                name="iopv_info", tag="IOPVInf", fields=Fieldset()
            ),
            "bdr_info": DatasetConfig(name="bdr_info", tag="BDRInf", fields=Fieldset()),
        }

        mock_meta = MagicMock()
        mock_meta.downloaded_files = ["dummy_path.gz"]

        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            datasets=datasets,
        )

        # Patch the downloaded_file property to return our test file
        with patch.object(
            PipelineContext,
            "downloaded_file",
            new_callable=lambda: property(lambda self: str(bvbg087_test_file)),
        ):
            step = B3ReadBVBG087XmlStep({"step": "b3_read_bvbg087_xml"})
            result = step.execute(None, context)

        # Keys should be output names, not XML tags
        assert "indexes_info" in result
        assert "iopv_info" in result
        assert "bdr_info" in result
        assert "IndxInf" not in result


class TestApplyFieldsMultiStep:
    """Tests for the apply_fields_multi pipeline step.

    These tests are marked as xfail because the step will be implemented
    as part of the BVBG087 migration.
    """

    @pytest.mark.xfail(reason="apply_fields_multi step not yet implemented")
    def test_step_is_registered(self):
        """Test that apply_fields_multi step is registered."""
        steps = StepRegistry.list_steps()
        assert "apply_fields_multi" in steps

    @pytest.mark.xfail(reason="apply_fields_multi step not yet implemented")
    def test_step_can_be_created(self):
        """Test that the step can be created from config."""
        step = StepRegistry.create(
            "apply_fields_multi", {"step": "apply_fields_multi", "errors": "coerce"}
        )
        assert step is not None
        assert step.name == "apply_fields_multi"

    @pytest.mark.xfail(reason="apply_fields_multi step not yet implemented")
    def test_step_applies_types_per_dataset(self, sample_multi_data, sample_fieldsets):
        """Test that step applies fieldset to each dataset."""
        from brasa.engine.pipeline.context import PipelineContext
        from brasa.engine.pipeline.steps.transform_steps import ApplyFieldsMultiStep
        from brasa.engine.template import DatasetConfig

        # Create datasets config with fieldsets
        datasets = {
            name: DatasetConfig(name=name, tag="", fields=fs)
            for name, fs in sample_fieldsets.items()
        }

        mock_meta = MagicMock()
        mock_meta.downloaded_files = []

        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            datasets=datasets,
        )

        step = ApplyFieldsMultiStep({"step": "apply_fields_multi", "errors": "coerce"})
        result = step.execute(sample_multi_data, context)

        # Check types were converted
        assert pd.api.types.is_datetime64_any_dtype(result["indexes_info"]["refdate"])
        assert pd.api.types.is_numeric_dtype(result["indexes_info"]["settlement_price"])

        assert pd.api.types.is_datetime64_any_dtype(result["iopv_info"]["refdate"])
        assert pd.api.types.is_numeric_dtype(result["iopv_info"]["last_price"])

    @pytest.mark.xfail(reason="apply_fields_multi step not yet implemented")
    def test_step_preserves_dict_keys(self, sample_multi_data, sample_fieldsets):
        """Test that step preserves the input dict keys."""
        from brasa.engine.pipeline.context import PipelineContext
        from brasa.engine.pipeline.steps.transform_steps import ApplyFieldsMultiStep
        from brasa.engine.template import DatasetConfig

        datasets = {
            name: DatasetConfig(name=name, tag="", fields=fs)
            for name, fs in sample_fieldsets.items()
        }

        mock_meta = MagicMock()
        context = PipelineContext(meta=mock_meta, reader_config={}, datasets=datasets)

        step = ApplyFieldsMultiStep({"step": "apply_fields_multi"})
        result = step.execute(sample_multi_data, context)

        assert set(result.keys()) == set(sample_multi_data.keys())

    @pytest.mark.xfail(reason="apply_fields_multi step not yet implemented")
    def test_step_raises_on_non_dict_input(self):
        """Test that step raises error if input is not a dict."""
        from brasa.engine.pipeline.context import PipelineContext
        from brasa.engine.pipeline.steps.transform_steps import ApplyFieldsMultiStep

        mock_meta = MagicMock()
        context = PipelineContext(meta=mock_meta, reader_config={})

        step = ApplyFieldsMultiStep({"step": "apply_fields_multi"})

        with pytest.raises(ValueError, match="expects.*DataFrame"):
            step.execute(pd.DataFrame(), context)


class TestExistingApplyFieldsStep:
    """Tests to ensure the existing apply_fields step still works."""

    def test_apply_fields_step_exists(self):
        """Test that apply_fields step exists."""
        steps = StepRegistry.list_steps()
        assert "apply_fields" in steps

    def test_apply_fields_step_can_be_created(self):
        """Test that apply_fields step can be created."""
        step = StepRegistry.create("apply_fields", {"step": "apply_fields"})
        assert step is not None

    def test_apply_fields_works_with_single_dataframe(self):
        """Test that apply_fields works with a single DataFrame."""
        from brasa.engine.pipeline.context import PipelineContext
        from brasa.engine.pipeline.steps.transform_steps import ApplyFieldsStep

        # Create a fieldset
        fieldset = Fieldset()
        fieldset.add_fields(
            Field(name="value", description="Numeric value", type_definition="numeric"),
        )

        mock_meta = MagicMock()
        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            fields=fieldset,
        )

        df = pd.DataFrame({"value": ["100.5", "200.25"]})

        step = ApplyFieldsStep({"step": "apply_fields", "errors": "coerce"})
        result = step.execute(df, context)

        assert pd.api.types.is_numeric_dtype(result["value"])
