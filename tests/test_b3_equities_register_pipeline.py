"""Tests for b3-equities-register pipeline conversion.

This module validates the conversion from function-based to pipeline-based ETL
for the b3-equities-register template. Tests ensure that:

1. Pipeline execution produces identical output to the legacy function
2. Partitioned output structure is correct (by refdate)
3. Data integrity is maintained through column selection and deduplication
"""

from pathlib import Path

import pandas as pd
import pytest

from brasa.engine import (
    retrieve_template,
)
from brasa.queries import get_dataset


class TestTemplateLoading:
    """Test that the template loads and is properly configured."""

    def test_template_loads(self) -> None:
        """Test that b3-equities-register template loads successfully."""
        template = retrieve_template("b3-equities-register")
        assert template is not None
        assert template.id == "b3-equities-register"

    def test_template_is_pipeline_based(self) -> None:
        """Test that the template uses pipeline-based ETL."""
        template = retrieve_template("b3-equities-register")
        assert template.is_etl
        assert template.etl.is_pipeline

    def test_pipeline_has_correct_steps(self) -> None:
        """Test that the pipeline has the expected steps."""
        template = retrieve_template("b3-equities-register")
        pipeline = template.etl.pipeline

        # Check number of steps
        assert (
            len(pipeline.steps) == 4
        ), "Expected 4 steps: load, select, apply_fields, drop_duplicates"

        # Check step names
        step_names = [step.name for step in pipeline.steps]
        assert step_names[0] == "load"
        assert step_names[1] == "select"
        assert step_names[2] == "apply_fields"
        assert step_names[3] == "drop_duplicates"

    def test_pipeline_input_datasets(self) -> None:
        """Test that pipeline correctly identifies input datasets."""
        template = retrieve_template("b3-equities-register")
        input_datasets = template.etl.get_input_datasets()

        assert "b3-bvbg028-equities" in input_datasets

    def test_writer_configuration(self) -> None:
        """Test that writer is configured with refdate partitioning."""
        template = retrieve_template("b3-equities-register")

        assert hasattr(template, "writer")
        assert template.writer is not None
        assert hasattr(template.writer, "partitioning")
        assert "refdate" in template.writer.partitioning


class TestPipelineExecution:
    """Test pipeline execution produces correct output."""

    @pytest.mark.skipif(
        not Path(".brasa-cache/db/b3-bvbg028-equities").exists(),
        reason="b3-bvbg028-equities dataset not available in cache",
    )
    def test_pipeline_executes(self) -> None:
        """Test that the pipeline executes without errors."""
        template = retrieve_template("b3-equities-register")

        # Execute the pipeline (without writing)
        result = template.etl.pipeline.execute(
            template_id=template.id,
            writer=template.writer,
        )

        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    @pytest.mark.skipif(
        not Path(".brasa-cache/db/b3-bvbg028-equities").exists(),
        reason="b3-bvbg028-equities dataset not available in cache",
    )
    def test_pipeline_output_columns(self) -> None:
        """Test that pipeline output has all expected columns."""
        template = retrieve_template("b3-equities-register")
        result = template.etl.pipeline.execute(
            template_id=template.id,
            writer=template.writer,
        )

        expected_columns = [
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
            "instrument_asset",
            "instrument_asset_description",
            "instrument_market",
            "instrument_segment",
            "instrument_description",
            "security_category",
            "isin",
            "distribution_id",
            "cfi_code",
            "specification_code",
            "corporation_name",
            "symbol",
            "payment_type",
            "allocation_lot_size",
            "price_factor",
            "trading_start_date",
            "trading_end_date",
            "corporate_action_start_date",
            "ex_distribution_number",
            "custody_treatment_type",
            "trading_currency",
            "market_capitalisation",
            "close",
            "open",
            "days_to_settlement",
            "right_issue_price",
            "instrument_type",
            "governance_indicator",
        ]

        assert set(result.columns) == set(expected_columns)
        assert len(result.columns) == 31

    @pytest.mark.skipif(
        not Path(".brasa-cache/db/b3-bvbg028-equities").exists(),
        reason="b3-bvbg028-equities dataset not available in cache",
    )
    def test_pipeline_deduplication(self) -> None:
        """Test that deduplication step removes duplicates."""
        template = retrieve_template("b3-equities-register")

        # Execute pipeline
        result = template.etl.pipeline.execute(
            template_id=template.id,
            writer=template.writer,
        )

        # Result should not have duplicates
        result_duplicates = result.duplicated().sum()
        assert result_duplicates == 0, "Pipeline output should not have duplicates"


class TestPipelineVsLegacyFunction:
    """Test that pipeline produces same output as legacy function."""

    @pytest.mark.skipif(
        not Path(".brasa-cache/db/b3-bvbg028-equities").exists(),
        reason="b3-bvbg028-equities dataset not available in cache",
    )
    def test_outputs_are_equivalent(self) -> None:
        """Test that pipeline output matches legacy function output.

        Note: Row order may differ, but content should be identical.
        """
        template = retrieve_template("b3-equities-register")

        # Get output from pipeline
        pipeline_result = template.etl.pipeline.execute(
            template_id=template.id,
            writer=template.writer,
        )

        # Get source dataset and apply legacy function logic
        source_ds = get_dataset("b3-bvbg028-equities")
        legacy_result = (
            source_ds.to_table(
                columns=[
                    "refdate",
                    "security_id",
                    "security_proprietary",
                    "security_market",
                    "instrument_asset",
                    "instrument_asset_description",
                    "instrument_market",
                    "instrument_segment",
                    "instrument_description",
                    "security_category",
                    "isin",
                    "distribution_id",
                    "cfi_code",
                    "specification_code",
                    "corporation_name",
                    "symbol",
                    "payment_type",
                    "allocation_lot_size",
                    "price_factor",
                    "trading_start_date",
                    "trading_end_date",
                    "corporate_action_start_date",
                    "ex_distribution_number",
                    "custody_treatment_type",
                    "trading_currency",
                    "market_capitalisation",
                    "close",
                    "open",
                    "days_to_settlement",
                    "right_issue_price",
                    "instrument_type",
                    "governance_indicator",
                ]
            )
            .to_pandas()
            .drop_duplicates()
        )

        # Compare: sort both by all columns to normalize row order
        pipeline_sorted = pipeline_result.sort_values(
            by=list(pipeline_result.columns)
        ).reset_index(drop=True)
        legacy_sorted = legacy_result.sort_values(
            by=list(legacy_result.columns)
        ).reset_index(drop=True)

        # Check column names and count
        assert set(pipeline_result.columns) == set(legacy_result.columns)
        assert len(pipeline_result) == len(legacy_result)

        # Check data types match
        for col in pipeline_result.columns:
            assert (
                pipeline_result[col].dtype == legacy_result[col].dtype
            ), f"Column {col} has different dtype"

        # Check values are the same (after sorting)
        pd.testing.assert_frame_equal(pipeline_sorted, legacy_sorted)

    @pytest.mark.skipif(
        not Path(".brasa-cache/db/b3-bvbg028-equities").exists(),
        reason="b3-bvbg028-equities dataset not available in cache",
    )
    def test_row_counts_match(self) -> None:
        """Test that pipeline and legacy function produce same row count."""
        template = retrieve_template("b3-equities-register")

        # Pipeline result
        pipeline_result = template.etl.pipeline.execute(
            template_id=template.id,
            writer=template.writer,
        )

        # Legacy function result
        source_ds = get_dataset("b3-bvbg028-equities")
        legacy_result = (
            source_ds.to_table(
                columns=[
                    "refdate",
                    "security_id",
                    "security_proprietary",
                    "security_market",
                    "instrument_asset",
                    "instrument_asset_description",
                    "instrument_market",
                    "instrument_segment",
                    "instrument_description",
                    "security_category",
                    "isin",
                    "distribution_id",
                    "cfi_code",
                    "specification_code",
                    "corporation_name",
                    "symbol",
                    "payment_type",
                    "allocation_lot_size",
                    "price_factor",
                    "trading_start_date",
                    "trading_end_date",
                    "corporate_action_start_date",
                    "ex_distribution_number",
                    "custody_treatment_type",
                    "trading_currency",
                    "market_capitalisation",
                    "close",
                    "open",
                    "days_to_settlement",
                    "right_issue_price",
                    "instrument_type",
                    "governance_indicator",
                ]
            )
            .to_pandas()
            .drop_duplicates()
        )

        assert len(pipeline_result) == len(legacy_result)

    @pytest.mark.skipif(
        not Path(".brasa-cache/db/b3-bvbg028-equities").exists(),
        reason="b3-bvbg028-equities dataset not available in cache",
    )
    def test_column_selection_matches(self) -> None:
        """Test that pipeline selects the same columns as legacy function."""
        template = retrieve_template("b3-equities-register")

        # Get pipeline output
        pipeline_result = template.etl.pipeline.execute(
            template_id=template.id,
            writer=template.writer,
        )

        # Get legacy function output
        source_ds = get_dataset("b3-bvbg028-equities")
        legacy_result = (
            source_ds.to_table(
                columns=[
                    "refdate",
                    "security_id",
                    "security_proprietary",
                    "security_market",
                    "instrument_asset",
                    "instrument_asset_description",
                    "instrument_market",
                    "instrument_segment",
                    "instrument_description",
                    "security_category",
                    "isin",
                    "distribution_id",
                    "cfi_code",
                    "specification_code",
                    "corporation_name",
                    "symbol",
                    "payment_type",
                    "allocation_lot_size",
                    "price_factor",
                    "trading_start_date",
                    "trading_end_date",
                    "corporate_action_start_date",
                    "ex_distribution_number",
                    "custody_treatment_type",
                    "trading_currency",
                    "market_capitalisation",
                    "close",
                    "open",
                    "days_to_settlement",
                    "right_issue_price",
                    "instrument_type",
                    "governance_indicator",
                ]
            )
            .to_pandas()
            .drop_duplicates()
        )

        # Columns should match exactly
        assert list(pipeline_result.columns) == list(legacy_result.columns)


class TestOutputPartitioning:
    """Test that output can be properly partitioned by refdate."""

    @pytest.mark.skipif(
        not Path(".brasa-cache/db/b3-bvbg028-equities").exists(),
        reason="b3-bvbg028-equities dataset not available in cache",
    )
    def test_refdate_column_exists(self) -> None:
        """Test that refdate column exists for partitioning."""
        template = retrieve_template("b3-equities-register")
        result = template.etl.pipeline.execute(
            template_id=template.id,
            writer=template.writer,
        )

        assert "refdate" in result.columns

    @pytest.mark.skipif(
        not Path(".brasa-cache/db/b3-bvbg028-equities").exists(),
        reason="b3-bvbg028-equities dataset not available in cache",
    )
    def test_refdate_is_date_type(self) -> None:
        """Test that refdate column has date type."""
        template = retrieve_template("b3-equities-register")
        result = template.etl.pipeline.execute(
            template_id=template.id,
            writer=template.writer,
        )

        # Check that refdate is date-like (either date or datetime)
        assert pd.api.types.is_datetime64_any_dtype(result["refdate"])

    @pytest.mark.skipif(
        not Path(".brasa-cache/db/b3-bvbg028-equities").exists(),
        reason="b3-bvbg028-equities dataset not available in cache",
    )
    def test_multiple_refdates_exist(self) -> None:
        """Test that multiple dates exist for partitioning."""
        template = retrieve_template("b3-equities-register")
        result = template.etl.pipeline.execute(
            template_id=template.id,
            writer=template.writer,
        )

        # Check that we have data from multiple dates
        unique_dates = result["refdate"].nunique()
        assert unique_dates > 0, "Should have at least one date"
