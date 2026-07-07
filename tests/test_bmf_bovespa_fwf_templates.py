"""End-to-end reader tests for the migrated B3/BM&F fixed-width templates.

Covers three templates created from legacy BM&F/BOVESPA fixed-width files:

- ``b3-derivatives-daily``    (BD_Arbit.txt)   single-dataset FWF with per-row
  dynamic decimals and +/- sign columns.
- ``b3-registered-contracts`` (CONTRCAD.TXT)  single-dataset FWF with per-row
  dynamic decimals.
- ``b3-bdin``                 (BDIN)           multi-record FWF exploded into one
  dataset per record type via the ``b3_read_bdin_fwf`` step.

Each test drives the real template reader pipeline against a committed sample
file in ``data/`` (skipping if the fixture is absent), so field widths, type
conversions, dynamic-decimal scaling, sign handling and record dispatch are all
exercised as they run in production.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from brasa.engine.pipeline.context import PipelineContext
from brasa.engine.template import retrieve_template

DATA_DIR = Path(__file__).parent.parent / "data"


def _run_reader(template_id: str, filename: str):
    """Run a template's reader pipeline against a sample file in ``data/``.

    Returns the reader output (a DataFrame for single-dataset templates or a
    ``dict[str, DataFrame]`` for multi-dataset ones). Skips the test when the
    fixture file is not present.
    """
    path = DATA_DIR / filename
    if not path.exists():
        pytest.skip(f"Test data file not found: {path}")

    template = retrieve_template(template_id)
    meta = MagicMock()
    meta.downloaded_files = ["dummy"]

    with patch.object(
        PipelineContext,
        "downloaded_file",
        new_callable=lambda: property(lambda self: str(path)),
    ):
        return template, template.reader.read(meta)


# ---------------------------------------------------------------------------
# b3-derivatives-daily (BD_Arbit.txt)
# ---------------------------------------------------------------------------


class TestDerivativesDaily:
    def test_template_loads_with_pipeline(self):
        t = retrieve_template("b3-derivatives-daily")
        assert t.id == "b3-derivatives-daily"
        assert t.reader.has_pipeline
        step_names = [s.__class__.__name__ for s in t.reader._pipeline.steps]
        assert step_names == ["ReadFwfStep", "ApplyFieldsStep", "ExecCodeStep"]

    def test_template_has_importer(self):
        t = retrieve_template("b3-derivatives-daily")
        assert t.has_downloader
        assert t.downloader.path == "tmp/ContratosPregaoFinal/BF%y%m%d/BD_Final.txt"
        from brasa.downloaders import local_file_import

        assert t.downloader.download_function is local_file_import

    def test_reads_all_rows(self):
        _, df = _run_reader("b3-derivatives-daily", "BD_Arbit.txt")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 45

    def test_dynamic_decimals_and_dates(self):
        _, df = _run_reader("b3-derivatives-daily", "BD_Arbit.txt")
        row = df[(df["cod_mercadoria"] == "DI1") & (df["serie"] == "F16")].iloc[0]
        # cotacao fields scaled by num_casas_decimais_2 (== 3 for DI1)
        assert row["num_casas_decimais_2"] == 3
        assert row["cot_primeiro_negocio"] == pytest.approx(14.58)
        assert row["cot_maior_negocio"] == pytest.approx(14.82)
        assert row["cot_menor_negocio"] == pytest.approx(14.56)
        # ajuste fields scaled by num_casas_decimais (== 2)
        assert row["cot_ajuste"] == pytest.approx(96434.89)
        assert row["cot_ajuste_anterior"] == pytest.approx(96424.14)
        # fixed-decimal fields
        assert row["valor_ajuste_contr"] == pytest.approx(10.75)
        assert row["tamanho_contr"] == pytest.approx(1.0)
        # date parsed
        assert pd.Timestamp(row["data_vencimento"]) == pd.Timestamp("2016-01-04")

    def test_trading_limits_scaled_and_signed(self):
        # limite_min/max_negociacao scale by num_casas_decimais_2 (quote-side
        # decimals), not num_casas_decimais (settlement-side) -- verified by
        # checking the traded price falls within the [min, max] band.
        _, df = _run_reader("b3-derivatives-daily", "BD_Arbit.txt")
        row = df[(df["cod_mercadoria"] == "DI1") & (df["serie"] == "F16")].iloc[0]
        assert row["limite_min_negociacao"] == pytest.approx(13.87)
        assert row["limite_max_negociacao"] == pytest.approx(15.47)
        assert (
            row["limite_min_negociacao"]
            < row["cot_primeiro_negocio"]
            < row["limite_max_negociacao"]
        )

    def test_valor_diferenca_sign_applied(self):
        # sinal_diferenca has real negative values in the fixture; the sign
        # must be folded into valor_diferenca, not silently dropped.
        _, df = _run_reader("b3-derivatives-daily", "BD_Arbit.txt")
        row = df[(df["cod_mercadoria"] == "DI1") & (df["serie"] == "G16")].iloc[0]
        assert row["valor_diferenca"] == pytest.approx(-644.0)

    def test_sign_values_applied_but_columns_retained(self):
        # Sign columns are folded into their target numeric fields but kept
        # (vestigial) in the output: the writer's parquet schema is derived
        # from the full `fields:` list, so dropping them would break writing
        # (see test_output_matches_writer_schema below, which guards this).
        _, df = _run_reader("b3-derivatives-daily", "BD_Arbit.txt")
        assert len(df.columns) == 80
        for col in (
            "sinal_cot_primeiro_negocio",
            "sinal_cot_fechamento",
            "sinal_cot_ajuste",
            "sinal_oscilacao",
        ):
            assert col in df.columns

    def test_output_matches_writer_schema(self):
        """Regression guard: `_read_marketdata` builds the parquet schema from
        the full `fields:` list and requires every field to be present as a
        column in the reader's output DataFrame (extra pipeline steps that drop
        declared fields, e.g. `drop_columns`, break writing with a KeyError)."""
        from brasa.engine.processing import _get_schema_from_fields

        t, df = _run_reader("b3-derivatives-daily", "BD_Arbit.txt")
        schema = _get_schema_from_fields(t.fields)
        assert schema is not None
        missing = [name for name in schema.names if name not in df.columns]
        assert missing == []

    def test_import_then_process_end_to_end(self):
        """Full import_marketdata -> process_marketdata cycle via the template's
        `importer:` block, overriding `path` to the committed fixture (the
        template's own default path points at a local, uncommitted archive)."""
        import pyarrow.dataset as ds

        import brasa
        from brasa.engine.cache import CacheManager

        fixture = DATA_DIR / "BD_Arbit.txt"
        if not fixture.exists():
            pytest.skip(f"Test data file not found: {fixture}")

        report = brasa.import_marketdata(
            "b3-derivatives-daily",
            path=str(fixture),
            refdate="2015-09-25",
            verbosity=brasa.Verbosity.QUIET,
        )
        assert report.operation == "import"
        assert all(r.status.name == "PASSED" for r in report.results)

        process_report = brasa.process_marketdata("b3-derivatives-daily")
        assert all(r.status.name == "PASSED" for r in process_report.results)

        cache = CacheManager()
        template = brasa.retrieve_template("b3-derivatives-daily")
        folder = cache.cache_path(cache.db_folder(template))
        table = ds.dataset(folder, format="parquet", partitioning="hive").to_table()
        assert table.num_rows == 45
        assert "sinal_cot_ajuste" in table.column_names


# ---------------------------------------------------------------------------
# b3-registered-contracts (CONTRCAD.TXT)
# ---------------------------------------------------------------------------


class TestDerivativesContracts:
    def test_template_loads_with_pipeline(self):
        t = retrieve_template("b3-registered-contracts")
        assert t.id == "b3-registered-contracts"
        assert t.reader.has_pipeline
        step_names = [s.__class__.__name__ for s in t.reader._pipeline.steps]
        assert step_names == ["ReadFwfStep", "ApplyFieldsStep", "ExecCodeStep"]

    def test_reads_all_rows(self):
        _, df = _run_reader("b3-registered-contracts", "CONTRCAD.TXT")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 45
        assert len(df.columns) == 31

    def test_dynamic_decimals_and_fields(self):
        _, df = _run_reader("b3-registered-contracts", "CONTRCAD.TXT")
        row = df[df["serie"] == "K15"].iloc[0]
        assert row["num_casas_decimais"] == 3
        # variacao_minima 000000000000001 / 10**3 == 0.001 (DI tick)
        assert row["variacao_minima"] == pytest.approx(0.001)
        assert row["preco_exercicio"] == pytest.approx(0.0)
        assert str(row["cod_moeda"]).strip() == "02"
        assert str(row["cod_isin"]).strip() == "BRBMEFD1I4P1"
        assert pd.Timestamp(row["data_vencimento"]) == pd.Timestamp("2015-05-04")


# ---------------------------------------------------------------------------
# b3-bdin (multi-record FWF)
# ---------------------------------------------------------------------------

BDIN_FIXTURE = "BDIN_2010-12-20.txt"
BDIN_EXPECTED_DATASETS = {
    "indexes-summary": 16,
    "stocks-summary": 2103,
    "trades-summary-bdi": 21,
    "high-oscilations": 20,
    "ibovespa-oscilations": 20,
    "most-traded-stocks": 10,
    "most-traded-assets": 30,
    "iopv-summary": 6,
}


class TestBdin:
    def test_template_loads_multi_dataset(self):
        t = retrieve_template("b3-bdin")
        assert t.id == "b3-bdin"
        assert t.datasets is not None
        assert set(t.datasets) == set(BDIN_EXPECTED_DATASETS)
        step_names = [s.__class__.__name__ for s in t.reader._pipeline.steps]
        assert step_names == ["B3ReadBdinFwfStep", "ApplyFieldsMultiStep"]

    def test_reader_step_registered(self):
        from brasa.engine.pipeline import StepRegistry

        assert "b3_read_bdin_fwf" in StepRegistry.list_steps()

    def test_returns_dict_with_expected_row_counts(self):
        _, result = _run_reader("b3-bdin", BDIN_FIXTURE)
        assert isinstance(result, dict)
        assert set(result) == set(BDIN_EXPECTED_DATASETS)
        for name, expected_rows in BDIN_EXPECTED_DATASETS.items():
            assert len(result[name]) == expected_rows, name

    def test_refdate_injected_everywhere(self):
        _, result = _run_reader("b3-bdin", BDIN_FIXTURE)
        for name, df in result.items():
            assert "refdate" in df.columns, name
            if len(df):
                assert pd.Timestamp(df["refdate"].iloc[0]) == pd.Timestamp(
                    "2010-12-20"
                ), name

    def test_stocks_summary_prices_scaled(self):
        _, result = _run_reader("b3-bdin", BDIN_FIXTURE)
        ss = result["stocks-summary"]
        petr4 = ss[ss["cod_negociacao"].astype("string").str.strip() == "PETR4"].iloc[0]
        # N(09)V99 prices -> dec=2 applied
        assert petr4["preco_ult"] == pytest.approx(25.57)
        assert petr4["preco_max"] == pytest.approx(26.06)
        assert pd.api.types.is_datetime64_any_dtype(ss["data_vencimento"])

    def test_record_03_alignment_fixed(self):
        """Record 03 VOLTOT is N(15)V99 (17 chars); the legacy width was 15."""
        _, result = _run_reader("b3-bdin", BDIN_FIXTURE)
        tb = result["trades-summary-bdi"]
        lote = tb[tb["descricao_cod_bdi"].astype("string").str.strip() == "LOTE PADRAO"]
        assert lote.iloc[0]["volume_titulos_negociados"] == pytest.approx(6146410279.0)

    def test_all_datasets_are_byte_aligned(self):
        """The trailing ``reserva`` filler must be blank in every record type;
        any mis-sized field upstream would leak non-blank bytes into it."""
        _, result = _run_reader("b3-bdin", BDIN_FIXTURE)
        for name, df in result.items():
            if "reserva" in df.columns and len(df):
                blanks = df["reserva"].astype("string").fillna("").str.strip() == ""
                assert blanks.all(), f"{name} has non-blank reserva (misaligned)"
