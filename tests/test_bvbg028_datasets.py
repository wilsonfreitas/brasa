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
