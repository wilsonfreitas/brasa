"""Parametrized tests for b3-bvbg028 datasets.

One DatasetCase per dataset. The module-scoped fixture runs the pipeline once
against data/IN210423.zip and caches the resulting dict[str, DataFrame].
Each sub-issue that adds a new dataset appends one DatasetCase to DATASETS.
"""

from __future__ import annotations

import gzip
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pytest

from brasa.engine import CacheManager, CacheMetadata, retrieve_template

DATA_DIR = Path(__file__).parent.parent / "data"
FIXTURE_ZIP = DATA_DIR / "IN210423.zip"


@dataclass
class DatasetCase:
    dataset: str
    xml_tag: str
    expected_count: int
    required_non_null: list[str]
    dtypes: dict[str, str]
    spot_check: dict[str, str]


DATASETS: list[DatasetCase] = [
    DatasetCase(
        dataset="equities",
        xml_tag="EqtyInf",
        expected_count=4764,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000304675",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "HAPV",
            "security_category": "25",
            "isin": "BRHAPVACNOR4",
        },
    ),
    DatasetCase(
        dataset="options_on_equities",
        xml_tag="OptnOnEqtsInf",
        expected_count=25239,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000546598",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "VALE",
            "security_category": "7",
            "isin": "BRVALE3A0FE5",
        },
    ),
    DatasetCase(
        dataset="future_contracts",
        xml_tag="FutrCtrctsInf",
        expected_count=472,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000548494",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "COGNO",
            "security_category": "80",
        },
    ),
    DatasetCase(
        dataset="exercise_of_equities",
        xml_tag="ExrcEqtsInf",
        expected_count=3267,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000546584",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "ITUB",
            "security_category": "17",
            "symbol": "ITUBE249E",
            "isin": "BRITUB4E0QV5",
            "trading_currency": "BRL",
            "delivery_type": "1",
            "option_exercise_security_id": "200000538823",
            "option_exercise_security_proprietary": "8",
            "option_exercise_security_market": "BVMF",
        },
    ),
    DatasetCase(
        dataset="fixed_income",
        xml_tag="FxdIncmInf",
        expected_count=369,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000304649",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "VERT",
            "security_category": "70",
            "isin": "BRVERTCRA0V7",
            "symbol": "VERT-CRAV1B0",
            "trading_currency": "BRL",
            "payment_type": "1",
            "days_to_settlement": "0",
            "allocation_lot_size": "1",
            "underlying_security_id": "200000304505",
            "underlying_security_proprietary": "8",
            "underlying_security_market": "BVMF",
        },
    ),
    DatasetCase(
        dataset="options_on_spot_and_futures",
        xml_tag="OptnOnSpotAndFutrsInf",
        expected_count=7131,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000548079",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "BGI",
            "instrument_asset_description": "Boi Gordo",
            "instrument_market": "4",
            "instrument_segment": "4",
            "instrument_description": "BOI GORDO R$",
            "isin": "BRBMEFCBVXH2",
            "symbol": "BGIV21C033600",
            "exercise_price": "336.0",
            "exercise_style": "AMER",
            "maturity_date": "2021-10-29",
            "expiration_code": "VPLK",
            "option_type": "CALL",
            "contract_multiplier": "330.0",
            "asset_quotation_quantity": "1.0",
            "payment_type": "0",
            "allocation_lot_size": "1",
            "cfi_code": "OCAFPS",
            "underlying_security_id": "200000467555",
            "underlying_security_proprietary": "8",
            "underlying_security_market": "BVMF",
            "premium_upfront_indicator": "true",
            "trading_start_date": "2021-03-05",
            "trading_end_date": "2021-10-29",
            "opening_position_limit_date": "2021-10-28",
            "trading_currency": "BRL",
            "withdrawal_days": "132",
            "working_days": "131",
            "calendar_days": "189",
        },
    ),
    DatasetCase(
        dataset="derivatives_option_exercise",
        xml_tag="DrvsOptnExrcInf",
        expected_count=7131,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000548078",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "CCM",
            "instrument_asset_description": "Milho",
            "instrument_market": "4",
            "instrument_segment": "4",
            "instrument_description": "MILHO CAMPINAS",
            "security_category": "17",
            "symbol": "CCMU21P009100E",
            "isin": "BRBMEFOVHD47",
            "option_delivery_type": "0",
            "derivative_exercise_security_id": "200000548077",
            "derivative_exercise_security_proprietary": "8",
            "derivative_exercise_security_market": "BVMF",
            "withdrawal_days": "101",
            "working_days": "100",
            "calendar_days": "145",
        },
    ),
    DatasetCase(
        dataset="equity_forwards",
        xml_tag="EqtyFwdInf",
        expected_count=554,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000306440",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "ALSO",
            "instrument_asset_description": "ALSO",
            "instrument_market": "30",
            "instrument_segment": "2",
            "instrument_description": "ALIANSCSONAEON      NM",
            "security_category": "2",
            "symbol": "ALSO3T",
            "isin": "BRALSOTNO007",
            "distribution_id": "100",
            "cfi_code": "JESXFP",
            "payment_type": "0",
            "allocation_lot_size": "1",
            "price_factor": "1",
            "trading_start_date": "2019-08-06",
            "custody_treatment_type": "1",
            "trading_currency": "BRL",
            "underlying_security_id": "200000306433",
            "underlying_security_proprietary": "8",
            "underlying_security_market": "BVMF",
        },
    ),
    DatasetCase(
        dataset="international_bonds",
        xml_tag="IntlBdInf",
        expected_count=349,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000067057",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "INTB",
            "instrument_asset_description": "TÍTULOS PÚBLICOS INTERNACIONAIS",
            "instrument_market": "5",
            "instrument_segment": "8",
            "instrument_description": "United States Treasury Note/Bond (Bond) - T 7 5/8 02/15/25",
            "security_category": "36",
            "isin": "US912810ET17",
            "cusip": "912810ET1",
            "issuer_country": "840",
            "bond_type": "US TREASURY BONDS",
            "issue_price": "99.707993",
            "issue_date": "1995-02-15",
            "maturity_date": "2025-02-15",
            "currency": "USD",
        },
    ),
    DatasetCase(
        dataset="national_bonds",
        xml_tag="NtlBdInf",
        expected_count=371,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "300000021795",
            "refdate": "2021-04-23",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "FGB",
            "instrument_asset_description": "TÍTULOS PÚBLICOS FEDERAIS",
            "instrument_market": "5",
            "instrument_segment": "8",
            "instrument_description": "Letras Financeiras do Tesouro",
            "security_category": "33",
            "isin": "BRSTNCLF1RC4",
            "selic_code": "210100",
            "base_date": "2000-07-01",
            "base_date_price": "1000.0",
            "issue_date": "2018-10-26",
            "maturity_date": "2025-03-01",
            "bond_type_code": "7",
        },
    ),
    DatasetCase(
        dataset="fixed_income_non_tradable",
        xml_tag="FxdIncmNonTrdblInf",
        expected_count=124,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000306399",
            "refdate": "2021-04-23",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "EGIE",
            "instrument_asset_description": "EGIE",
            "instrument_market": "5",
            "instrument_segment": "3",
            "instrument_description": "ENGIE BRASILD91     NM",
            "security_category": "38",
            "isin": "BREGIEDBS076",
            "distribution_id": "100",
            "ex_distribution_number": "0",
            "custody_treatment_type": "0",
            "cfi_code": "DBVUAR",
            "specification_code": "D91     NM",
            "corporation_name": "ENGIE BRASIL ENERGIA S.A.",
            "asset_registration_date": "2019-08-05",
            "issue_code": "6634",
            "series_number": "1",
            "asset_collateral_type": "0",
            "asset_additional_collateral_type": "0",
            "asset_subordinated_type": "2",
            "debenture_convertibility_type": "0",
            "debenture_tax_benefit_article1": "false",
            "debenture_tax_benefit_article2": "false",
            "perpetual_debenture_indicator": "false",
            "base_date": "2050-08-15",
            "interest_rate": "0.0",
            "interest_rate_correction_type": "2",
            "interest_rate_correction_time_base": "252",
            "early_redemption_indicator": "false",
            "total_series_issue_value": "1.0",
            "unit_value": "1.0",
            "market_capitalisation": "1",
            "maturity_date": "2050-08-15",
            "symbol": "EGIE-DEB91",
            "trading_currency": "BRL",
        },
    ),
    DatasetCase(
        dataset="adrs",
        xml_tag="ADRInf",
        expected_count=30,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "300000031244",
            "refdate": "2021-04-23",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "ADR",
            "instrument_asset_description": "American Depositary Receipt",
            "instrument_market": "10",
            "instrument_segment": "1",
            "instrument_description": "VIVT3 BZ ADR",
            "security_category": "39",
            "symbol": "VIVT3 BZ",
            "isin": "US87936R2058",
            "cfi_code": "MMXXXX",
            "cusip": "87936R205",
            "program_level": "4",
            "proportion": "1",
            "trading_currency": "USD",
        },
    ),
    DatasetCase(
        dataset="securities_lending",
        xml_tag="BTCInf",
        expected_count=4,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "100000038605",
            "refdate": "2021-04-23",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "OTC",
            "instrument_asset_description": "OTC DERIVATIVES",
            "instrument_market": "91",
            "instrument_segment": "12",
            "instrument_description": "SecurityLending OTC",
            "security_category": "54",
            "symbol": "OTCSECLEND",
            "fungibility_indicator": "true",
            "payment_type": "0",
        },
    ),
    DatasetCase(
        dataset="otc_derivatives",
        xml_tag="OTCInf",
        expected_count=5,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "10009252",
            "refdate": "2021-04-23",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "OTC",
            "instrument_asset_description": "OTC DERIVATIVES",
            "instrument_market": "81",
            "instrument_segment": "10",
            "instrument_description": "DERIVATIVO DE BALCAO - SWAP",
            "contract_type": "1",
            "trade_origin_code": "2",
            "fungibility_indicator": "false",
        },
    ),
    DatasetCase(
        dataset="cash",
        xml_tag="CshInf",
        expected_count=2,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "10000001",
            "refdate": "2021-04-23",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "CASH",
            "instrument_asset_description": "CASH",
            "instrument_market": "1",
            "instrument_segment": "5",
            "instrument_description": "COLLATERAL CASH - REAL",
            "security_category": "37",
            "cfi_code": "MMXXXX",
            "currency_code": "BRL",
        },
    ),
    DatasetCase(
        dataset="investment_funds",
        xml_tag="FICInf",
        expected_count=2,
        required_non_null=[
            "refdate",
            "security_id",
            "security_proprietary",
            "security_market",
        ],
        dtypes={"security_id": "string"},
        spot_check={
            "security_id": "200000125482",
            "refdate": "2021-04-23",
            "security_proprietary": "8",
            "security_market": "BVMF",
            "instrument_asset": "FIC",
            "instrument_asset_description": "FUNDOS DE INVESTIMENTOS FIC",
            "instrument_market": "1",
            "instrument_segment": "5",
            "instrument_description": "FILCB BVMFBOVESPA",
            "security_category": "35",
            "fund_name": "FIC BBVMBOVESPA",
            "currency": "BRL",
        },
    ),
]


@pytest.fixture(scope="module")
def bvbg028_datasets():
    """Run b3-bvbg028 pipeline once on data/IN210423.zip; cache results in memory."""
    if not FIXTURE_ZIP.exists():
        pytest.skip(f"Fixture not found: {FIXTURE_ZIP}")

    with zipfile.ZipFile(FIXTURE_ZIP) as zf:
        inner_name = next(n for n in zf.namelist() if "BVBG.028" in n)
        inner_data = zf.read(inner_name)

    man = CacheManager()
    raw_dir = Path(man.cache_path("raw/b3-bvbg028"))
    raw_dir.mkdir(parents=True, exist_ok=True)
    inner_filename = Path(inner_name).name + ".gz"
    (raw_dir / inner_filename).write_bytes(gzip.compress(inner_data))

    meta = CacheMetadata("b3-bvbg028")
    meta.downloaded_files = [f"raw/b3-bvbg028/{inner_filename}"]

    tpl = retrieve_template("b3-bvbg028")
    return tpl.reader.read(meta)


@pytest.mark.parametrize("case", DATASETS, ids=[d.dataset for d in DATASETS])
def test_dataset_registered(case):
    """Template declares this dataset with the expected XML tag."""
    tpl = retrieve_template("b3-bvbg028")
    assert case.dataset in tpl.reader.datasets, (
        f"Dataset '{case.dataset}' not found in template"
    )
    assert tpl.reader.datasets[case.dataset].tag == case.xml_tag


@pytest.mark.parametrize("case", DATASETS, ids=[d.dataset for d in DATASETS])
def test_dataset_present_in_output(case, bvbg028_datasets):
    """Pipeline output contains this dataset key."""
    assert case.dataset in bvbg028_datasets, (
        f"Dataset '{case.dataset}' missing from pipeline output"
    )


@pytest.mark.parametrize("case", DATASETS, ids=[d.dataset for d in DATASETS])
def test_record_count(case, bvbg028_datasets):
    """Record count matches expected value from the 2021-04-23 fixture."""
    df = bvbg028_datasets[case.dataset]
    assert len(df) == case.expected_count, (
        f"Expected {case.expected_count} records, got {len(df)}"
    )


@pytest.mark.parametrize("case", DATASETS, ids=[d.dataset for d in DATASETS])
def test_required_columns_non_null(case, bvbg028_datasets):
    """Required columns have zero null values."""
    df = bvbg028_datasets[case.dataset]
    for col in case.required_non_null:
        null_count = df[col].isna().sum()
        assert null_count == 0, f"Column '{col}' has {null_count} null values"


@pytest.mark.parametrize("case", DATASETS, ids=[d.dataset for d in DATASETS])
def test_dtypes(case, bvbg028_datasets):
    """Declared dtype assertions match the DataFrame."""
    df = bvbg028_datasets[case.dataset]
    for col, expected_dtype in case.dtypes.items():
        actual = str(df[col].dtype)
        assert actual == expected_dtype, (
            f"Column '{col}': expected dtype '{expected_dtype}', got '{actual}'"
        )


@pytest.mark.parametrize("case", DATASETS, ids=[d.dataset for d in DATASETS])
def test_spot_check_row(case, bvbg028_datasets):
    """Spot-check row exists and all declared field values match."""
    df = bvbg028_datasets[case.dataset]
    sid = case.spot_check["security_id"]
    matches = df[df["security_id"] == sid]
    assert len(matches) >= 1, f"No row with security_id={sid!r}"
    row = matches.iloc[0]
    for key, expected in case.spot_check.items():
        actual = row[key]
        if hasattr(actual, "date"):
            assert str(actual.date()) == str(expected), (
                f"Field '{key}': expected {expected!r}, got {actual!r}"
            )
        else:
            assert str(actual) == str(expected), (
                f"Field '{key}': expected {expected!r}, got {actual!r}"
            )
