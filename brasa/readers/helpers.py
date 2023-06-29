import gzip
import json
from typing import IO

import pandas as pd

from brasa.engine import CacheManager, CacheMetadata, MarketDataReader, retrieve_template
from brasa.parsers.b3.bvbg028 import BVBG028Parser
from brasa.parsers.b3.bvbg086 import BVBG086Parser
from brasa.parsers.b3.cdi import CDIParser
from brasa.parsers.b3.cotahist import COTAHISTParser
from brasa.parsers.b3.futures_settlement_prices import \
    future_settlement_prices_parser
from brasa.util import SuppressUserWarnings


def read_json(reader: MarketDataReader, fname: IO | str) -> pd.DataFrame:
    if isinstance(fname, str):
        with open(fname, "r", encoding=reader.encoding) as f:
            data = json.load(f)
    else:
        data = json.load(fname)
    return pd.DataFrame(data, index=[0], columns=reader.fields.names)


def read_b3_trades_intraday(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    template = retrieve_template(meta.template)
    reader = template.reader
    converters = {"trade_time": str,}
    df = pd.read_csv(fname,
                     encoding=reader.encoding,
                     header=None,
                     skiprows=reader.skip,
                     sep=reader.separator,
                     converters=converters,
                     names=reader.fields.names,)
    
    df["traded_quantity"] = pd.to_numeric(df["traded_quantity"], errors="coerce")
    df["traded_price"] = pd.to_numeric(df["traded_price"].str.replace(",", "."), errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["refdate"] = pd.to_datetime(df["refdate"])

    return df


def read_b3_otc_trade_information(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    template = retrieve_template(meta.template)
    reader = template.reader
    df = pd.read_csv(fname,
                     encoding=reader.encoding,
                     header=None,
                     skiprows=reader.skip,
                     sep=reader.separator,
                     names=reader.fields.names,)
    df["traded_quantity"] = pd.to_numeric(df["traded_quantity"], errors="coerce")
    df["traded_price"] = pd.to_numeric(df["traded_price"].str.replace(",", "."), errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"].str.replace(",", "."), errors="coerce")
    df["traded_interest_rate"] = pd.to_numeric(df["traded_interest_rate"].str.replace(",", "."), errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce", dayfirst=True)
    df["settlement_date"] = pd.to_datetime(df["settlement_date"], errors="coerce", dayfirst=True)
    df["refdate"] = df["trade_date"]
    
    return df


def read_b3_lending_trades(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    template = retrieve_template(meta.template)
    reader = template.reader
    # converters = {n:str for n in reader.fields.names}
    df = pd.read_csv(fname,
                     encoding=reader.encoding,
                     header=None,
                     skiprows=reader.skip,
                     sep=reader.separator,
                     #    converters=converters,
                     names=reader.fields.names,)
    df["interest_rate_term_trade"] = pd.to_numeric(df["interest_rate_term_trade"].str.replace(",", "."), errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["refdate"] = pd.to_datetime(df["refdate"], errors="coerce")
    return df


def read_b3_cotahist(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    parser = COTAHISTParser(man.cache_path(fname))
    return parser._data._tables["data"]


def read_b3_bvbg028(meta: CacheMetadata) -> dict[str, pd.DataFrame]:
    paths = meta.downloaded_files
    paths.sort()
    fname = paths[-1]
    man = CacheManager()
    with gzip.open(man.cache_path(fname)) as f:
        parser = BVBG028Parser(f)

    df_equities = parser.data["EqtyInf"]
    df_equities["creation_date"] = pd.to_datetime(df_equities["creation_date"])
    df_equities["refdate"] = pd.to_datetime(df_equities["refdate"])
    df_equities["security_id"] = pd.to_numeric(df_equities["security_id"])
    df_equities["security_proprietary"] = pd.to_numeric(df_equities["security_proprietary"])
    df_equities["instrument_market"] = pd.to_numeric(df_equities["instrument_market"])
    df_equities["instrument_segment"] = pd.to_numeric(df_equities["instrument_segment"])
    df_equities["security_category"] = pd.to_numeric(df_equities["security_category"])
    df_equities["distribution_id"] = pd.to_numeric(df_equities["distribution_id"])
    df_equities["payment_type"] = pd.to_numeric(df_equities["payment_type"])
    df_equities["allocation_lot_size"] = pd.to_numeric(df_equities["allocation_lot_size"])
    df_equities["price_factor"] = pd.to_numeric(df_equities["price_factor"])
    df_equities["ex_distribution_number"] = pd.to_numeric(df_equities["ex_distribution_number"])
    df_equities["custody_treatment_type"] = pd.to_numeric(df_equities["custody_treatment_type"])
    df_equities["market_capitalisation"] = pd.to_numeric(df_equities["market_capitalisation"])
    df_equities["close"] = pd.to_numeric(df_equities["close"])
    df_equities["open"] = pd.to_numeric(df_equities["open"])
    df_equities["days_to_settlement"] = pd.to_numeric(df_equities["days_to_settlement"])
    df_equities["right_issue_price"] = pd.to_numeric(df_equities["right_issue_price"])
    with SuppressUserWarnings():
        df_equities["trading_start_date"] = pd.to_datetime(df_equities["trading_start_date"], errors="coerce")
        df_equities["trading_end_date"] = pd.to_datetime(df_equities["trading_end_date"], errors="coerce")
        df_equities["corporate_action_start_date"] = pd.to_datetime(df_equities["corporate_action_start_date"], errors="coerce")

    df_futures = parser.data["FutrCtrctsInf"]
    df_futures["creation_date"] = pd.to_datetime(df_futures["creation_date"])
    df_futures["refdate"] = pd.to_datetime(df_futures["refdate"])
    df_futures["security_id"] = pd.to_numeric(df_futures["security_id"])
    df_futures["security_proprietary"] = pd.to_numeric(df_futures["security_proprietary"])
    df_futures["instrument_market"] = pd.to_numeric(df_futures["instrument_market"])
    df_futures["instrument_segment"] = pd.to_numeric(df_futures["instrument_segment"])
    df_futures["security_category"] = pd.to_numeric(df_futures["security_category"])
    df_futures["value_type_code"] = pd.to_numeric(df_futures["value_type_code"])
    df_futures["delivery_type"] = pd.to_numeric(df_futures["delivery_type"])
    df_futures["payment_type"] = pd.to_numeric(df_futures["payment_type"])
    df_futures["contract_multiplier"] = pd.to_numeric(df_futures["contract_multiplier"])
    df_futures["asset_settlement_indicator"] = pd.to_numeric(df_futures["asset_settlement_indicator"])
    df_futures["allocation_lot_size"] = pd.to_numeric(df_futures["allocation_lot_size"])
    df_futures["underlying_security_id"] = pd.to_numeric(df_futures["underlying_security_id"])
    df_futures["underlying_security_proprietary"] = pd.to_numeric(df_futures["underlying_security_proprietary"])
    df_futures["withdrawal_days"] = pd.to_numeric(df_futures["withdrawal_days"])
    df_futures["working_days"] = pd.to_numeric(df_futures["working_days"])
    df_futures["calendar_days"] = pd.to_numeric(df_futures["calendar_days"])
    with SuppressUserWarnings():
        df_futures["maturity_date"] = pd.to_datetime(df_futures["maturity_date"], errors="coerce")
        df_futures["trading_start_date"] = pd.to_datetime(df_futures["trading_start_date"], errors="coerce")
        df_futures["trading_end_date"] = pd.to_datetime(df_futures["trading_end_date"], errors="coerce")

    df_eq_options = parser.data["OptnOnEqtsInf"]
    df_eq_options["creation_date"] = pd.to_datetime(df_eq_options["creation_date"])
    df_eq_options["refdate"] = pd.to_datetime(df_eq_options["refdate"])
    df_eq_options["security_id"] = pd.to_numeric(df_eq_options["security_id"])
    df_eq_options["security_proprietary"] = pd.to_numeric(df_eq_options["security_proprietary"])
    df_eq_options["instrument_market"] = pd.to_numeric(df_eq_options["instrument_market"])
    df_eq_options["instrument_segment"] = pd.to_numeric(df_eq_options["instrument_segment"])
    df_eq_options["security_category"] = pd.to_numeric(df_eq_options["security_category"])
    df_eq_options["exercise_price"] = pd.to_numeric(df_eq_options["exercise_price"])
    df_eq_options["underlying_security_id"] = pd.to_numeric(df_eq_options["underlying_security_id"])
    df_eq_options["underlying_security_proprietary"] = pd.to_numeric(df_eq_options["underlying_security_proprietary"])
    df_eq_options["payment_type"] = pd.to_numeric(df_eq_options["payment_type"])
    df_eq_options["allocation_lot_size"] = pd.to_numeric(df_eq_options["allocation_lot_size"])
    df_eq_options["price_factor"] = pd.to_numeric(df_eq_options["price_factor"])
    df_eq_options["days_to_settlement"] = pd.to_numeric(df_eq_options["days_to_settlement"])
    df_eq_options["delivery_type"] = pd.to_numeric(df_eq_options["delivery_type"])
    df_eq_options["automatic_exercise_indicator"] = df_eq_options["automatic_exercise_indicator"].str.lower() == "true"
    df_eq_options["protection_flag"] = df_eq_options["protection_flag"].str.lower() == "true"
    df_eq_options["premium_upfront_indicator"] = df_eq_options["premium_upfront_indicator"].str.lower() == "true"
    with SuppressUserWarnings():
        df_eq_options["maturity_date"] = pd.to_datetime(df_eq_options["maturity_date"], errors="coerce")
        df_eq_options["trading_start_date"] = pd.to_datetime(df_eq_options["trading_start_date"], errors="coerce")
        df_eq_options["trading_end_date"] = pd.to_datetime(df_eq_options["trading_end_date"], errors="coerce")

    return parser.data


def read_b3_bvbg086(meta: CacheMetadata) -> pd.DataFrame:
    paths = meta.downloaded_files
    paths.sort()
    fname = paths[-1]
    man = CacheManager()
    parser = BVBG086Parser(man.cache_path(fname))
    df = parser.data
    df["refdate"] = pd.to_datetime(df["refdate"])
    df["creation_date"] = pd.to_datetime(df["creation_date"])
    df["security_id"] = pd.to_numeric(df["security_id"])
    df["security_proprietary"] = pd.to_numeric(df["security_proprietary"])
    df["open_interest"] = pd.to_numeric(df["open_interest"])
    df["trade_quantity"] = pd.to_numeric(df["trade_quantity"])
    df["volume"] = pd.to_numeric(df["volume"])
    df["traded_contracts"] = pd.to_numeric(df["traded_contracts"])
    df["best_ask_price"] = pd.to_numeric(df["best_ask_price"])
    df["best_bid_price"] = pd.to_numeric(df["best_bid_price"])
    df["open"] = pd.to_numeric(df["open"])
    df["low"] = pd.to_numeric(df["low"])
    df["high"] = pd.to_numeric(df["high"])
    df["average"] = pd.to_numeric(df["average"])
    df["close"] = pd.to_numeric(df["close"])
    df["regular_transactions_quantity"] = pd.to_numeric(df["regular_transactions_quantity"])
    df["regular_traded_contracts"] = pd.to_numeric(df["regular_traded_contracts"])
    df["regular_volume"] = pd.to_numeric(df["regular_volume"])
    df["oscillation_percentage"] = pd.to_numeric(df["oscillation_percentage"])
    df["adjusted_quote"] = pd.to_numeric(df["adjusted_quote"])
    df["adjusted_tax"] = pd.to_numeric(df["adjusted_tax"])
    df["previous_adjusted_quote"] = pd.to_numeric(df["previous_adjusted_quote"])
    df["previous_adjusted_tax"] = pd.to_numeric(df["previous_adjusted_tax"])
    df["variation_points"] = pd.to_numeric(df["variation_points"])
    df["adjusted_value_contract"] = pd.to_numeric(df["adjusted_value_contract"])
    df["nonregular_transactions_quantity"] = pd.to_numeric(df["nonregular_transactions_quantity"])
    df["nonregular_traded_contracts"] = pd.to_numeric(df["nonregular_traded_contracts"])
    df["nonregular_volume"] = pd.to_numeric(df["nonregular_volume"])

    return parser.data


def read_b3_cdi(meta: CacheMetadata) -> pd.DataFrame:
    man = CacheManager()
    parser = CDIParser(man.cache_path(meta.downloaded_files[0]))
    return parser.data


def read_b3_futures_settlement_prices(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[-1]
    man = CacheManager()
    with gzip.open(man.cache_path(fname)) as f:
        df = future_settlement_prices_parser(f)
    return df