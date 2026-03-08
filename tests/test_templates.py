from datetime import datetime

import pandas as pd
import pytest

from brasa.engine import (
    CacheManager,
    CacheMetadata,
    MarketDataTemplate,
    _download_marketdata,
    _read_marketdata,
    get_marketdata,
    retrieve_template,
)
from brasa.fieldsets import Fieldset


def test_load_template():
    tpl = MarketDataTemplate("templates/bcb-sgs-data.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader


def test_template_load_fields():
    tpl = MarketDataTemplate("templates/bcb-sgs-data.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
    # Template.fields is now a Fieldset
    assert isinstance(tpl.fields, Fieldset)
    assert len(tpl.fields) == 3
    assert tpl.fields["refdate"].name == "refdate"
    assert tpl.fields["refdate"].description == "Data de referência"
    # Field now has type_name instead of handler
    assert tpl.fields["refdate"].type_name == "date"
    assert tpl.fields["value"].type_name == "numeric"
    assert tpl.fields["code"].type_name == "integer"


def test_retrieve_temlate():
    tpl = retrieve_template("bcb-sgs-data")
    assert tpl is not None
    assert isinstance(tpl, MarketDataTemplate)
    assert tpl.id == "bcb-sgs-data"


@pytest.mark.skip(
    reason="External API issues: www2.bmf.com.br and www2.cetip.com.br are unreachable or have changed"
)
def test_get_marketdata():
    df = get_marketdata("b3-futures-settlement-prices", refdate=datetime(2023, 5, 19))
    assert isinstance(df, pd.DataFrame)
    df = get_marketdata("bcb-sgs-data", code=12, refdate=datetime(2023, 5, 19))
    assert isinstance(df, dict)


def test_save_empty_metadata():
    meta = CacheMetadata("bcb-sgs-data")
    assert meta.id == "fef009a135f746ed3216a0b87358422f"

    man = CacheManager()
    assert not man.has_meta(meta)
    man.save_meta(meta)
    assert man.has_meta(meta)

    man.remove_meta(meta)


@pytest.mark.skip(reason="External API issue: SGS endpoint is unstable in CI")
def test_metadata_fulfilment():
    meta = CacheMetadata("bcb-sgs-data")
    assert len(meta.downloaded_files) == 0

    _download_marketdata(meta)
    assert len(meta.downloaded_files) == 1

    man = CacheManager()
    man.save_meta(meta)

    df = _read_marketdata(meta)
    assert df is not None
    man.save_meta(meta)

    tpl = retrieve_template("bcb-sgs-data")
    meta2 = CacheMetadata("bcb-sgs-data")
    meta2.extra_key = tpl.downloader.extra_key

    man.load_meta(meta2)
    assert meta2.timestamp == meta.timestamp
    assert meta2.template == meta.template
    assert meta2.downloaded_files == meta.downloaded_files
    assert meta2.processed_files == meta.processed_files

    man.remove_meta(meta)


def test_run_query_step_loads():
    """Test that the run_query ETL step is registered and loads correctly."""
    from brasa.engine.pipeline.registry import StepRegistry

    # Verify run_query step is registered
    run_query_step = StepRegistry.get("sql_query")
    assert run_query_step is not None

    # Verify it can be created from config
    config = {
        "step": "sql_query",
        "datasets": ["input.test-dataset"],
        "query": "SELECT * FROM 'input.test-dataset'",
    }
    step = StepRegistry.create("sql_query", config)
    assert step is not None
    assert step.name == "sql_query"


def test_brasa_companies_template_loads():
    """Test that the brasa-companies template loads correctly."""
    tpl = MarketDataTemplate("templates/brasa-companies.yaml")

    assert tpl.id == "brasa-companies"
    assert tpl.is_etl
    assert not tpl.has_downloader
    assert not tpl.has_reader
    assert tpl.etl.is_pipeline

    # Check that the template has the expected field structure
    assert tpl.fields is not None
    assert len(tpl.fields) > 0

    # Verify key fields are defined
    field_names = {f.name for f in tpl.fields}
    expected_fields = {
        "code_cvm",
        "company_name",
        "trading_name",
        "asset_name",
        "company_status",
        "isin",
        "industry_classification",
        "sector",
        "subsector",
        "segment",
        "cvm_cnpj",
        "b3_cnpj",
    }
    assert expected_fields.issubset(field_names), (
        f"Missing fields: {expected_fields - field_names}"
    )


@pytest.mark.skip(
    reason="Requires external datalake datasets (input.cvm-companies-registration, input.b3-company-details)"
)
def test_brasa_companies_pipeline_execution():
    """Test that the brasa-companies template can be executed (requires datalake)."""
    tpl = retrieve_template("brasa-companies")
    # This would normally execute the full ETL pipeline
    # Skipped since it requires real datalake data
    assert tpl is not None
    assert tpl.is_etl


def test_brasa_industry_sectors_template_loads():
    """Test that the brasa-industry-sectors template loads correctly."""
    tpl = MarketDataTemplate("templates/brasa-industry-sectors.yaml")

    assert tpl.id == "brasa-industry-sectors"
    assert tpl.is_etl
    assert not tpl.has_downloader
    assert not tpl.has_reader
    assert tpl.etl.is_pipeline

    # Check that the template has the expected field structure
    assert tpl.fields is not None
    assert len(tpl.fields) > 0

    # Verify mandatory output fields are defined
    field_names = {f.name for f in tpl.fields}
    expected_fields = {
        "sector",
        "subsector",
        "gics_sector",
        "icb_sector",
        "normalized_sector",
        "normalized_subsector",
    }
    assert expected_fields.issubset(field_names), (
        f"Missing fields: {expected_fields - field_names}"
    )

    # Verify all fields are of type string
    for field in tpl.fields:
        assert field.type_name == "string", (
            f"Field '{field.name}' expected type 'string', got '{field.type_name}'"
        )


@pytest.mark.skip(reason="Requires local datalake dataset staging.brasa-companies")
def test_brasa_industry_sectors_pipeline_execution():
    """Test that the brasa-industry-sectors template can be executed (requires datalake)."""
    tpl = retrieve_template("brasa-industry-sectors")
    # This would normally execute the full ETL pipeline against staging.brasa-companies
    # Skipped since it requires real datalake data
    assert tpl is not None
    assert tpl.is_etl


# --- Retry configuration tests (TASK-013) ---


class TestRetryConfigParsing:
    """Tests for retry configuration parsing and validation."""

    def test_default_retry_config(self):
        """Templates without retry keys must preserve single-attempt behavior."""
        tpl = retrieve_template("bcb-sgs-data")
        dl = tpl.downloader
        assert dl.retry_attempts == 0
        assert dl.retry_delay == 0.0
        assert dl.retry_backoff == 1.0
        assert dl.retry_on_status_codes == [408, 425, 429, 500, 502, 503, 504]
        assert dl.retry_on_download_exception is True

    def test_b3_company_details_retry_config(self):
        """b3-company-details template should have explicit retry keys."""
        tpl = retrieve_template("b3-company-details")
        dl = tpl.downloader
        assert dl.retry_attempts == 10
        assert dl.retry_delay == 3.0
        assert dl.retry_backoff == 2.0

    def test_retry_attempts_negative_raises(self):
        """retry_attempts < 0 must raise ValueError."""
        from brasa.engine.template import MarketDataDownloader

        with pytest.raises(ValueError, match="retry_attempts must be >= 0"):
            MarketDataDownloader(
                {
                    "function": "brasa.downloaders.simple_download",
                    "retry_attempts": -1,
                }
            )

    def test_retry_delay_negative_raises(self):
        """retry_delay < 0 must raise ValueError."""
        from brasa.engine.template import MarketDataDownloader

        with pytest.raises(ValueError, match="retry_delay must be >= 0"):
            MarketDataDownloader(
                {
                    "function": "brasa.downloaders.simple_download",
                    "retry_delay": -0.5,
                }
            )

    def test_retry_backoff_below_one_raises(self):
        """retry_backoff < 1.0 must raise ValueError."""
        from brasa.engine.template import MarketDataDownloader

        with pytest.raises(ValueError, match="retry_backoff must be >= 1.0"):
            MarketDataDownloader(
                {
                    "function": "brasa.downloaders.simple_download",
                    "retry_backoff": 0.5,
                }
            )

    def test_retry_on_status_codes_out_of_range_raises(self):
        """Status codes outside [100, 599] must raise ValueError."""
        from brasa.engine.template import MarketDataDownloader

        with pytest.raises(ValueError, match="retry_on_status_codes values must be in"):
            MarketDataDownloader(
                {
                    "function": "brasa.downloaders.simple_download",
                    "retry_on_status_codes": [200, 999],
                }
            )
