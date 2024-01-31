import gzip
import json
from typing import IO

import numpy as np
import pandas as pd

from ..engine import CacheManager, CacheMetadata, MarketDataReader, retrieve_template
from ..parsers.b3.bvbg028 import BVBG028Parser
from ..parsers.b3.bvbg086 import BVBG086Parser
from ..parsers.b3.bvbg087 import BVBG087Parser
from ..parsers.b3.cdi import CDIParser
from ..parsers.b3.cotahist import COTAHISTParser
from ..parsers.b3.futures_settlement_prices import future_settlement_prices_parser
from ..util import SuppressUserWarnings


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
                     names=reader.fields.names, dtype_backend="pyarrow")
    
    df["traded_quantity"] = pd.to_numeric(df["traded_quantity"], errors="coerce")
    df["traded_price"] = pd.to_numeric(df["traded_price"].str.replace(",", "."), errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"] + " " + df["trade_time"], format="%Y-%m-%d %H%M%S%f", errors="coerce")
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
                     names=reader.fields.names, dtype_backend="pyarrow")
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
                     names=reader.fields.names, dtype_backend="pyarrow")
    df["interest_rate_term_trade"] = pd.to_numeric(df["interest_rate_term_trade"].str.replace(",", "."), errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df["refdate"] = pd.to_datetime(df["refdate"], errors="coerce")
    return df


def read_b3_cotahist(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    with gzip.open(man.cache_path(fname)) as f:
        parser = COTAHISTParser(f)
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
    df_eq_options["underlying_security_id"] = pd.to_numeric(df_eq_options["underlying_security_id"], errors="coerce")
    df_eq_options["underlying_security_proprietary"] = pd.to_numeric(df_eq_options["underlying_security_proprietary"], errors="coerce")
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
    with gzip.open(man.cache_path(fname)) as f:
        parser = BVBG086Parser(f)
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
    df["adjusted_value_contract"] = pd.to_numeric(df["adjusted_value_contract"].str.replace(",", "."))
    df["nonregular_transactions_quantity"] = pd.to_numeric(df["nonregular_transactions_quantity"])
    df["nonregular_traded_contracts"] = pd.to_numeric(df["nonregular_traded_contracts"])
    df["nonregular_volume"] = pd.to_numeric(df["nonregular_volume"])

    return parser.data


def read_b3_bvbg087(meta: CacheMetadata) -> dict[str, pd.DataFrame]:
    paths = meta.downloaded_files
    paths.sort()
    fname = paths[-1]
    man = CacheManager()
    with gzip.open(man.cache_path(fname)) as f:
        parser = BVBG087Parser(f)

    df_indexes = parser.data["IndxInf"]
    df_indexes["trade_date"] = pd.to_datetime(df_indexes["trade_date"])
    df_indexes["security_id"] = pd.to_numeric(df_indexes["security_id"])
    df_indexes["security_proprietary"] = pd.to_numeric(df_indexes["security_proprietary"])
    df_indexes["settlement_price"] = pd.to_numeric(df_indexes["settlement_price"])
    df_indexes["open_price"] = pd.to_numeric(df_indexes["open_price"])
    df_indexes["min_price"] = pd.to_numeric(df_indexes["min_price"])
    df_indexes["max_price"] = pd.to_numeric(df_indexes["max_price"])
    df_indexes["average_price"] = pd.to_numeric(df_indexes["average_price"])
    df_indexes["close_price"] = pd.to_numeric(df_indexes["close_price"])
    df_indexes["last_price"] = pd.to_numeric(df_indexes["last_price"])
    df_indexes["oscillation_val"] = pd.to_numeric(df_indexes["oscillation_val"])
    df_indexes["rising_shares_number"] = pd.to_numeric(df_indexes["rising_shares_number"])
    df_indexes["falling_shares_number"] = pd.to_numeric(df_indexes["falling_shares_number"])
    df_indexes["stable_shares_number"] = pd.to_numeric(df_indexes["stable_shares_number"])
    df_indexes.rename(columns={"trade_date": "refdate", "ticker_symbol": "symbol"}, inplace=True)

    df_iopv = parser.data["IOPVInf"]
    df_iopv["trade_date"] = pd.to_datetime(df_iopv["trade_date"])
    df_iopv["security_id"] = pd.to_numeric(df_iopv["security_id"])
    df_iopv["security_proprietary"] = pd.to_numeric(df_iopv["security_proprietary"])
    df_iopv["open_price"] = pd.to_numeric(df_iopv["open_price"])
    df_iopv["min_price"] = pd.to_numeric(df_iopv["min_price"])
    df_iopv["max_price"] = pd.to_numeric(df_iopv["max_price"])
    df_iopv["average_price"] = pd.to_numeric(df_iopv["average_price"])
    df_iopv["close_price"] = pd.to_numeric(df_iopv["close_price"])
    df_iopv["last_price"] = pd.to_numeric(df_iopv["last_price"])
    df_iopv["oscillation_val"] = pd.to_numeric(df_iopv["oscillation_val"])
    df_iopv.rename(columns={"trade_date": "refdate", "ticker_symbol": "symbol"}, inplace=True)

    df_bdr = parser.data["BDRInf"]
    df_bdr["trade_date"] = pd.to_datetime(df_bdr["trade_date"])
    df_bdr["security_id"] = pd.to_numeric(df_bdr["security_id"])
    df_bdr["security_proprietary"] = pd.to_numeric(df_bdr["security_proprietary"])
    df_bdr["ref_price"] = pd.to_numeric(df_bdr["ref_price"])
    df_bdr.rename(columns={"trade_date": "refdate", "ticker_symbol": "symbol"}, inplace=True)

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


def read_b3_economic_indicators_price(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    template = retrieve_template(meta.template)
    reader = template.reader
    converters = {"refdate": str,}
    df = pd.read_csv(fname,
                     encoding=reader.encoding,
                     header=None,
                     skiprows=reader.skip,
                     sep=reader.separator,
                     converters=converters,
                     names=reader.fields.names, dtype_backend="pyarrow")
    
    df["price"] = pd.to_numeric(df["price"].str.replace(",", "."), errors="coerce")
    df["refdate"] = pd.to_datetime(df["refdate"], errors="coerce")

    return df


def _read_b3_equity_options_files(meta: CacheMetadata) -> pd.DataFrame:
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
                     names=reader.fields.names, dtype_backend="pyarrow")
    df["maturity_date"] = pd.to_datetime(df["maturity_date"], format="%Y%m%d", errors="coerce")
    df["refdate"] = pd.to_datetime(meta.download_args["refdate"])
    return df


read_b3_equity_volatility_surface = _read_b3_equity_options_files
read_b3_equity_options = _read_b3_equity_options_files


def read_b3_company_info(meta: CacheMetadata) -> dict[str, pd.DataFrame]:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    with gzip.open(fname) as f:
        obj = json.load(f)
    data = {}
    # Info ----
    df = pd.DataFrame(obj)
    df = df.drop(columns=["cashDividends", "stockDividends", "subscriptions"])
    df["stockCapital"] = pd.to_numeric(df["stockCapital"].str.replace(".", "").str.replace(",", "."))
    df["numberCommonShares"] = pd.to_numeric(df["numberCommonShares"].str.replace(".", "").str.replace(",", "."))
    df["numberPreferredShares"] = pd.to_numeric(df["numberPreferredShares"].str.replace(".", "").str.replace(",", "."))
    df["totalNumberShares"] = pd.to_numeric(df["totalNumberShares"].str.replace(".", "").str.replace(",", "."))
    with SuppressUserWarnings():
        df["quotedPerSharSince"] = pd.to_datetime(df["quotedPerSharSince"], format="%d/%m/%Y", errors="coerce")
    df["refdate"] = pd.to_datetime(meta.timestamp.date())
    data["Info"] = df
    # Cash Dividends ----
    df = pd.DataFrame(obj[0]["cashDividends"])
    if df.shape[0] > 0:
        df["rate"] = pd.to_numeric(df["rate"].str.replace(".", "").str.replace(",", "."))
        with SuppressUserWarnings():
            df["paymentDate"] = pd.to_datetime(df["paymentDate"], format="%d/%m/%Y", errors="coerce")
            df["approvedOn"] = pd.to_datetime(df["approvedOn"], format="%d/%m/%Y", errors="coerce")
            df["lastDatePrior"] = pd.to_datetime(df["lastDatePrior"], format="%d/%m/%Y", errors="coerce")
        df["refdate"] = pd.to_datetime(meta.timestamp.date())
    data["CashDividends"] = df
    # Stock Dividends ----
    df = pd.DataFrame(obj[0]["stockDividends"])
    if df.shape[0] > 0:
        df["factor"] = pd.to_numeric(df["factor"].str.replace(".", "").str.replace(",", "."))
        with SuppressUserWarnings():
            df["approvedOn"] = pd.to_datetime(df["approvedOn"], format="%d/%m/%Y", errors="coerce")
            df["lastDatePrior"] = pd.to_datetime(df["lastDatePrior"], format="%d/%m/%Y", errors="coerce")
        df["refdate"] = pd.to_datetime(meta.timestamp.date())
    data["StockDividends"] = df
    # Subscriptions ----
    df = pd.DataFrame(obj[0]["subscriptions"])
    if df.shape[0] > 0:
        df["percentage"] = pd.to_numeric(df["percentage"].str.replace(".", "").str.replace(",", "."))
        df["priceUnit"] = pd.to_numeric(df["priceUnit"].str.replace(".", "").str.replace(",", "."))
        with SuppressUserWarnings():
            df["subscriptionDate"] = pd.to_datetime(df["subscriptionDate"], format="%d/%m/%Y", errors="coerce")
            df["approvedOn"] = pd.to_datetime(df["approvedOn"], format="%d/%m/%Y", errors="coerce")
            df["lastDatePrior"] = pd.to_datetime(df["lastDatePrior"], format="%d/%m/%Y", errors="coerce")
        df["refdate"] = pd.to_datetime(meta.timestamp.date())
    data["Subscriptions"] = df
    return data


def read_b3_company_details(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    with gzip.open(fname) as f:
        obj = json.load(f)
    if isinstance(obj, list):
        df = pd.DataFrame(obj)
    else:
        df = pd.DataFrame([obj])
    if df["otherCodes"].item() is not None:
        codes = [d["code"] for d in df["otherCodes"].item()]
        isins = [d["isin"] for d in df["otherCodes"].item()]
        df = pd.concat([df] * len(codes), ignore_index=True)
        df["code"] = codes
        df["isin"] = isins
    else:    
        df["code"] = np.nan
        df["isin"] = np.nan
    df.drop(columns=["otherCodes"], inplace=True)
    df["refdate"] = pd.to_datetime(meta.timestamp.date())
    return df


def read_b3_cash_dividends(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    with gzip.open(fname) as f:
        obj = json.load(f)
    df = pd.DataFrame(obj["results"])
    df["valueCash"] = pd.to_numeric(df["valueCash"].str.replace(".", "").str.replace(",", "."))
    df["ratio"] = pd.to_numeric(df["ratio"].str.replace(".", "").str.replace(",", "."))
    df["quotedPerShares"] = pd.to_numeric(df["quotedPerShares"].str.replace(".", "").str.replace(",", "."))
    df["closingPricePriorExDate"] = pd.to_numeric(df["closingPricePriorExDate"].str.replace(".", "").str.replace(",", "."))
    df["corporateActionPrice"] = pd.to_numeric(df["corporateActionPrice"].str.replace(".", "").str.replace(",", "."))
    with SuppressUserWarnings():
        df["dateApproval"] = pd.to_datetime(df["dateApproval"], format="%d/%m/%Y", errors="coerce")
        df["dateClosingPricePriorExDate"] = pd.to_datetime(df["dateClosingPricePriorExDate"], format="%d/%m/%Y", errors="coerce")
        df["lastDateTimePriorEx"] = pd.to_datetime(df["lastDateTimePriorEx"], format="%Y-%m-%dT%H:%M:%S", errors="coerce")
        df["lastDatePriorEx"] = pd.to_datetime(df["lastDatePriorEx"], format="%d/%m/%Y", errors="coerce")
    df["tradingName"] = meta.download_args["tradingName"]
    df["refdate"] = pd.to_datetime(meta.timestamp.date())
    return df


def read_b3_index_theoretical_portfolio(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    with gzip.open(fname) as f:
        obj = json.load(f)

    df = pd.DataFrame(obj["results"])
    df["part"] = pd.to_numeric(df["part"].str.replace(".", "").str.replace(",", "."))
    df["theoricalQty"] = pd.to_numeric(df["theoricalQty"].str.replace(".", "").str.replace(",", "."))
    df["total_theorical_qty"] = obj["header"]["theoricalQty"]
    df["reductor"] = obj["header"]["reductor"]
    df["total_theorical_qty"] = pd.to_numeric(df["total_theorical_qty"].str.replace(".", "").str.replace(",", "."))
    df["reductor"] = pd.to_numeric(df["reductor"].str.replace(".", "").str.replace(",", "."))
    df["index_name"] = meta.download_args["index"]
    df["refdate"] = pd.to_datetime(meta.timestamp.date())

    return df


def read_b3_indexes_composition(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    with gzip.open(fname) as f:
        obj = json.load(f)

    df = pd.DataFrame(obj["results"])
    df_stock_indexes = df\
        .groupby(["company", "spotlight", "code"])\
        .apply(lambda x: x.indexes.str.split(",").explode())\
        .reset_index()
    header = obj["header"]
    df_stock_indexes["start_month"] = f"{header['year']}-{str.zfill(str(header['startMonth']), 2)}"
    df_stock_indexes["end_month"] = f"{header['year']}-{str.zfill(str(header['endMonth']), 2)}"
    df_stock_indexes["index_update_date"] = pd.to_datetime(header["update"])
    df_stock_indexes["refdate"] = pd.to_datetime(meta.timestamp.date())

    return df_stock_indexes


def read_b3_listed_funds(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    with gzip.open(fname) as f:
        obj = json.load(f)

    template = retrieve_template(meta.template)
    df = pd.DataFrame(obj["results"])
    df["typeFund"] = template.downloader.args["typeFund"]
    df["refdate"] = pd.to_datetime(meta.timestamp.date())

    return df