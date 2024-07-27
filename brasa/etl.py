import re
from datetime import datetime

from bcb import sgs, PTAX
import pandas as pd
import numpy as np
import pyarrow
import pyarrow.acero as ac
import pyarrow.compute as pc
from bizdays import Calendar

from .queries import get_dataset, write_dataset
from .engine import MarketDataETL
from .parsers.b3.futures_settlement_prices import maturity2date


def create_b3_rate_futures(handler: MarketDataETL):
    df = get_dataset(handler.futures_dataset)
    cal = Calendar()
    bcal = Calendar.load("ANBIMA")

    df_contracts = df.to_table().filter(pc.field("commodity") == handler.commodity).to_pandas()
    day = handler.maturity_day
    df_contracts["maturity_date"] = df_contracts["maturity_code"].apply(lambda x: maturity2date(x, cal, day))
    adj_maturity = bcal.following(df_contracts["maturity_date"])
    df_contracts["business_days"] = bcal.bizdays(df_contracts["refdate"], adj_maturity)
    df_contracts["calendar_days"] = cal.bizdays(df_contracts["refdate"], adj_maturity)

    if handler.compounding == "simple":
        t = df_contracts["calendar_days"] / 360
        df_contracts["adjusted_tax"] = (100_000 / df_contracts["settlement_price"] - 1) * (1 / t)
    elif handler.compounding == "discrete":
        t = df_contracts["business_days"] / 252
        df_contracts["adjusted_tax"] = (100_000 / df_contracts["settlement_price"]) ** (1 / t) - 1

    df_contracts.sort_values(["refdate", "maturity_date"], inplace=True)
    df_contracts = df_contracts[
        ["refdate", "symbol", "maturity_date", "settlement_price", "adjusted_tax", "business_days", "calendar_days"]
    ]

    write_dataset(df_contracts, handler.template_id)


def create_b3_price_futures(handler: MarketDataETL):
    df = get_dataset(handler.futures_dataset)

    cal = Calendar()
    bcal = Calendar.load("ANBIMA")

    df_contracts = df.filter(pc.field("commodity") == handler.commodity).to_table().to_pandas()
    day = handler.maturity_day
    df_contracts["maturity_date"] = df_contracts["maturity_code"].apply(lambda x: maturity2date(x, cal, day))
    adj_maturity = bcal.following(df_contracts["maturity_date"])
    df_contracts["business_days"] = bcal.bizdays(df_contracts["refdate"], adj_maturity)
    df_contracts["calendar_days"] = cal.bizdays(df_contracts["refdate"], adj_maturity)
    df_contracts.sort_values(["refdate", "maturity_date"], inplace=True)
    df_contracts = df_contracts[
        ["refdate", "symbol", "maturity_date", "settlement_price", "business_days", "calendar_days"]
    ]

    write_dataset(df_contracts, handler.template_id)


def create_b3_price_futures_adjusted(handler: MarketDataETL):
    df_contracts = get_dataset(handler.futures_dataset).to_table().to_pandas()

    first = df_contracts.groupby("refdate").nth(0)
    second = df_contracts.groupby("refdate").nth(1)
    merged = first.merge(second, on="refdate", how="left", suffixes=("_1", "_2")).set_index("refdate")
    idx = merged.index[merged["business_days_1"] == 0]
    roll = merged.loc[idx, :]
    roll["ratio"] = roll.settlement_price_2 / roll.settlement_price_1
    merged["ratio"] = roll.ratio.iloc[::-1].cumprod().iloc[::-1]
    merged["ratio"] = merged["ratio"].bfill().fillna(1)
    merged["adjusted"] = merged.settlement_price_1 * merged.ratio
    merged["symbol"] = handler.first_generic_symbol
    merged.rename(columns={"adjusted": "price"}, inplace=True)
    merged.reset_index(inplace=True)

    write_dataset(merged[["refdate", "symbol", "price"]], handler.template_id)


def create_b3_futures_first_generic(handler: MarketDataETL):
    df_contracts = get_dataset(handler.futures_dataset).to_table().to_pandas()

    first = df_contracts.groupby("refdate").nth(0)
    second = df_contracts.groupby("refdate").nth(1)
    merged = first.merge(second, on="refdate", how="left").set_index("refdate")
    first_contracts = first.copy().reset_index(drop=True).set_index("refdate")
    second_contracts = second.copy().reset_index(drop=True).set_index("refdate")
    idx = merged.index[merged["business_days_x"].isin(handler.business_days_to_ignore)]
    first_contracts.loc[idx, :] = second_contracts.loc[idx, :]
    first_contracts["ref"] = first_contracts["symbol"]
    first_contracts["symbol"] = handler.first_generic_symbol
    first_contracts.reset_index(inplace=True)

    write_dataset(first_contracts, handler.template_id)


def create_bcb_data(handler: MarketDataETL):
    dd = sgs.get({"CDI": 4389}, start=datetime(2000, 1, 1))
    dd_cdi = dd.reset_index()
    dd_cdi["symbol"] = "CDI"
    dd_cdi.columns = ["refdate", "value", "symbol"]

    dd = sgs.get({"SELIC": 1178}, start=datetime(2000, 1, 1))
    dd_selic = dd.reset_index()
    dd_selic["symbol"] = "SELIC"
    dd_selic.columns = ["refdate", "value", "symbol"]

    dd = sgs.get({"SETA": 432}, start=datetime(2000, 1, 1))
    dd_seta = dd.reset_index()
    dd_seta["symbol"] = "SETA"
    dd_seta.columns = ["refdate", "value", "symbol"]

    dd = sgs.get({"IPCA": 433}, start=datetime(1980, 1, 1))
    dd_ipca = dd.reset_index()
    dd_ipca["symbol"] = "IPCA"
    dd_ipca.columns = ["refdate", "value", "symbol"]

    dd = sgs.get({"IGPM": 189}, start=datetime(1980, 1, 1))
    dd_igpm = dd.reset_index()
    dd_igpm["symbol"] = "IGPM"
    dd_igpm.columns = ["refdate", "value", "symbol"]

    df_bcb = pd.concat([dd_cdi, dd_selic, dd_seta, dd_ipca, dd_igpm])

    write_dataset(df_bcb, handler.template_id)


def _create_currency_candle(df: pd.DataFrame) -> pd.DataFrame:
    d = {
        "refdate": df["refdate"].iloc[0],
        "symbol": df["symbol"].iloc[0],
        "close": df["value"].iloc[-1],
        "open": df["value"].iloc[0],
        "high": df["value"].max().item(),
        "low": df["value"].min().item(),
    }
    return pd.DataFrame([d])


def _get_currency_data(
    ep: PTAX, currency: str, parity: str, start=datetime(2000, 1, 1), end=datetime.today()
) -> pd.DataFrame:
    if currency == "USD":
        dd = ep.query().select(ep.dataHoraCotacao, ep.cotacaoVenda, ep.tipoBoletim)
        symbol = "BRLUSD"
    else:
        dd = ep.query().select(ep.dataHoraCotacao, ep.paridadeVenda, ep.tipoBoletim)
        symbol = f"{currency}USD"
    dd = dd.parameters(
        moeda=currency, dataInicial=start.strftime("%m/%d/%Y"), dataFinalCotacao=end.strftime("%m/%d/%Y")
    ).collect()

    dd_cur = dd.copy()
    dd_cur = dd_cur.rename(columns={"dataHoraCotacao": "refdate", "paridadeVenda": "value", "cotacaoVenda": "value"})
    dd_cur["symbol"] = symbol
    dd_cur["trade_date"] = pd.to_datetime(dd_cur["refdate"])
    dd_cur["refdate"] = pd.to_datetime(dd_cur["refdate"].str[:10])
    dd_cur = dd_cur.sort_values("trade_date")
    dd_cur = dd_cur.groupby("refdate").apply(_create_currency_candle).reset_index(drop=True)
    dd_cur = dd_cur.loc[:, ["refdate", "symbol", "open", "high", "low", "close"]]
    if parity == "B" or currency == "USD":
        dd_cur[["open", "high", "low", "close"]] = 1 / dd_cur[["open", "high", "low", "close"]]
    return dd_cur


def create_bcb_currency_data(handler: MarketDataETL):
    ptax = PTAX()
    moedas = ptax.get_endpoint("Moedas").query().collect()
    ep = ptax.get_endpoint("CotacaoMoedaPeriodo")
    # tipo A
    dsa = [_get_currency_data(ep, moeda, "A") for moeda in moedas.query("tipoMoeda == 'A'")["simbolo"]]
    # tipo B
    dsb = [_get_currency_data(ep, moeda, "B") for moeda in moedas.query("tipoMoeda == 'B'")["simbolo"]]

    dd_dol = pd.concat(dsa + dsb)
    write_dataset(dd_dol, handler.template_id)


def create_b3_curves_di1(handler: MarketDataETL):
    tb_di1 = get_dataset(handler.futures_dataset).filter(pc.field("business_days") > 0).to_table()
    tb_cdi = (
        get_dataset(handler.bcb_dataset)
        .filter(pc.field("symbol") == "CDI")
        .filter(pc.field("refdate").isin(tb_di1.column("refdate").unique().to_pylist()))
        .to_table()
    )
    cal = Calendar.load(handler.calendar)
    tb_v1 = (
        tb_cdi.set_column(1, "adjusted_tax", pc.divide(tb_cdi.column("value"), 100))
        .append_column("maturity_date", pyarrow.array(cal.offset(tb_cdi.column("refdate").to_pylist(), 1)))
        .append_column("business_days", pyarrow.array([1] * tb_cdi.shape[0]))
        .select(["refdate", "symbol", "maturity_date", "business_days", "adjusted_tax"])
    )
    tb_di1_curve = pyarrow.concat_tables(
        [tb_v1, tb_di1.select(["refdate", "symbol", "maturity_date", "business_days", "adjusted_tax"])]
    ).sort_by([("refdate", "ascending"), ("business_days", "ascending")])

    write_dataset(tb_di1_curve.to_pandas(), handler.template_id)


def create_b3_curves(handler: MarketDataETL):
    tb = get_dataset(handler.futures_dataset).filter(pc.field("business_days") > 0).to_table()
    tb_curve = tb.select(["refdate", "symbol", "maturity_date", "business_days", "adjusted_tax"]).sort_by(
        [("refdate", "ascending"), ("business_days", "ascending")]
    )
    write_dataset(tb_curve.to_pandas(), handler.template_id)


def interp_ff(term, rates, terms):
    log_pu = np.log((1 + rates) ** (terms / 252))
    pu = np.exp(np.interp(term, terms, log_pu))
    return pu ** (252 / term) - 1


def create_b3_curves_standard_terms(handler: MarketDataETL):
    tb_curve = get_dataset(handler.curves_dataset).to_table()
    business_days_standard = np.array(handler.standard_terms)
    symbols_standard = pyarrow.array([f"{handler.symbol_prefix}{d}" for d in business_days_standard])
    cal = Calendar.load(handler.calendar)
    tables = []
    for date in tb_curve.column("refdate").unique():
        rates = tb_curve.filter(pc.field("refdate") == date).column("adjusted_tax").to_numpy()
        terms = tb_curve.filter(pc.field("refdate") == date).column("business_days").to_numpy()
        interp_rates = pyarrow.array(interp_ff(business_days_standard, rates, terms))
        mat_dates = pyarrow.array(cal.offset(date.as_py(), business_days_standard))
        ta = pyarrow.table(
            [
                pyarrow.array([date.as_py()] * len(interp_rates)),
                symbols_standard,
                mat_dates,
                pyarrow.array(business_days_standard),
                interp_rates,
            ],
            names=["refdate", "symbol", "maturity_date", "business_days", "adjusted_tax"],
        )
        tables.append(ta)
    tb_curve_standard = pyarrow.concat_tables(tables).sort_by(
        [("refdate", "ascending"), ("business_days", "ascending")]
    )
    write_dataset(tb_curve_standard.to_pandas(), handler.template_id)


def create_rate_returns(handler: MarketDataETL):
    tb_curve_standard = get_dataset(handler.curves_dataset).to_table()
    tables = []
    for symbol in tb_curve_standard.column("symbol").unique():
        rates = tb_curve_standard.filter(pc.field("symbol") == symbol).column("adjusted_tax").to_numpy()
        dates = tb_curve_standard.filter(pc.field("symbol") == symbol).column("refdate")
        symbols = tb_curve_standard.filter(pc.field("symbol") == symbol).column("symbol")
        returns = np.concatenate([np.array([np.nan]), np.diff(rates)])
        ta = pyarrow.table([dates, symbols, pyarrow.array(returns)], names=["refdate", "symbol", "returns"])
        tables.append(ta)

    tb_curve_standard_returns = pyarrow.concat_tables(tables).sort_by(
        [("refdate", "ascending"), ("symbol", "ascending")]
    )
    write_dataset(tb_curve_standard_returns.to_pandas(), handler.template_id)


def create_cotahist_dataset(handler: MarketDataETL):
    tb_cotahist_yearly = get_dataset(handler.yearly_dataset).to_table()
    tb_cotahist_daily = get_dataset(handler.daily_dataset).to_table()
    tb_cotahist = pyarrow.concat_tables([tb_cotahist_yearly, tb_cotahist_daily])
    tb_cotahist.sort_by([("refdate", "ascending")])
    write_dataset(tb_cotahist.to_pandas(), handler.template_id)


def copy_dataset_and_drop_duplicates(handler: MarketDataETL):
    ds_register = get_dataset(handler.futures_dataset)
    df_futures = ds_register.to_table(columns=handler.columns).to_pandas().drop_duplicates()
    write_dataset(df_futures, handler.template_id)


def create_b3_price_futures_from_register(handler: MarketDataETL):
    tb = get_dataset(handler.futures_dataset).to_table()
    symbols = tb.filter(pc.field("instrument_asset") == handler.commodity).column("symbol").unique()
    df_futures_settl = (
        tb.filter(pc.field("instrument_asset") == handler.commodity)
        .select(["symbol", "maturity_date"])
        .to_pandas()
        .drop_duplicates()
    )
    tb_futures = get_dataset(handler.futures_settlement_dataset).filter(pc.field("symbol").isin(symbols)).to_table()
    df_futures = tb_futures.to_pandas()
    df_futures = df_futures.merge(df_futures_settl, on="symbol", how="left")
    cal = Calendar()
    bcal = Calendar.load(handler.calendar)
    adj_maturity = cal.following(df_futures["maturity_date"])
    df_futures["business_days"] = bcal.bizdays(df_futures["refdate"], adj_maturity)
    df_futures["calendar_days"] = cal.bizdays(df_futures["refdate"], adj_maturity)
    df_futures.sort_values(["refdate", "maturity_date"], inplace=True)
    df_futures = df_futures[
        ["refdate", "symbol", "maturity_date", "settlement_price", "business_days", "calendar_days"]
    ]
    write_dataset(df_futures, handler.template_id)


def create_equities_spot_market_dataset(handler: MarketDataETL):
    tb = (
        get_dataset(handler.equities_dataset)
        .filter(pc.field("instrument_market") == 10)
        .filter(pc.field("instrument_segment") == 1)
        .filter(pc.field("instrument_asset") != "TAXA")
        .filter(pc.field("trading_start_date") != datetime(9999, 12, 31))
        .filter(pc.field("security_category").isin(pyarrow.array([1, 11, 6, 21, 3, 13])))
        .to_table()
    )
    write_dataset(tb.to_pandas(), handler.template_id)


def create_equities_returns(handler: MarketDataETL):
    tb_equities = get_dataset(handler.equities_dataset).to_table()
    symbols = tb_equities.column("symbol").unique()
    ds_returns = get_dataset(handler.marketdata_dataset)
    cols = [
        "refdate",
        "symbol",
        "oscillation_percentage",
    ]
    df_returns = (
        ds_returns.filter(pc.field("symbol").isin(symbols))
        .filter(~pc.is_null(pc.field("oscillation_percentage")))
        .to_table(columns=cols)
        .to_pandas()
    )
    df_returns["pct_return"] = df_returns["oscillation_percentage"] / 100
    df_returns["log_return"] = np.log(1 + df_returns["pct_return"])
    # cotahist values to correct missing date 20210610 ----
    df = (
        get_dataset(handler.cotahist_dataset)
        .filter(pc.field("symbol").isin(symbols))
        .filter(pc.field("refdate") >= datetime(2021, 6, 9))
        .filter(pc.field("refdate") <= datetime(2021, 6, 10))
        .scanner(["refdate", "symbol", "close", "distribution_id"])
        .to_table()
        .to_pandas()
    )
    symbols = df.groupby(["symbol"]).apply(lambda x: x.shape[0])
    symbols_to_ignore = symbols[symbols == 1].index
    df_clean = df[~df["symbol"].isin(symbols_to_ignore)]
    symbols = df_clean.groupby(["symbol"]).apply(lambda x: len(x["distribution_id"].unique()))
    symbols_to_use = symbols[symbols == 1].index
    df_final = df[df["symbol"].isin(symbols_to_use)]
    df_final = (
        df_final.groupby(["symbol"])
        .apply(
            lambda x: pd.DataFrame(
                [(x.refdate.iloc[1], x.close.iloc[1] / x.close.iloc[0] - 1)], columns=["refdate", "pct_return"]
            )
        )
        .reset_index()
    )
    df_final = df_final[["refdate", "symbol", "pct_return"]]
    df_final["log_return"] = np.log(1 + df_final["pct_return"])
    # ----
    df_returns = pd.concat([df_returns, df_final])
    df_returns.sort_values(["refdate", "symbol"], inplace=True)
    write_dataset(df_returns[["refdate", "symbol", "pct_return", "log_return"]], handler.template_id)


def create_etf_returns_before_20180101(handler: MarketDataETL):
    symbols = (
        get_dataset(handler.listed_funds_dataset)
        .filter(pc.field("fund_type") == "ETF")
        .scanner(columns=["symbol"])
        .to_table()
        .to_pandas()
        .iloc[:, 0]
    )
    cal = Calendar.load("B3")
    df_cotahist = (
        get_dataset(handler.cotahist_dataset)
        .filter(pc.field("refdate") >= cal.startdate)
        .filter(pc.field("symbol").isin(symbols))
        .scanner(columns=["refdate", "symbol", "close"])
        .to_table()
        .to_pandas()
    )

    def calc_pct_change(df):
        dfi = df[["refdate", "close"]].set_index("refdate").sort_index()
        idx = pd.DatetimeIndex(cal.seq(dfi.index[0], dfi.index[-1]))
        dfi = dfi.reindex(idx)
        dfi.index.name = "refdate"
        dfi = dfi.pct_change(fill_method=None).iloc[1:]
        dfi.columns = ["pct_return"]
        dfi["log_return"] = np.log(1 + dfi["pct_return"])
        return dfi

    df_cotahist_etfs_returns = df_cotahist.groupby("symbol").apply(calc_pct_change).reset_index()
    ix = df_cotahist_etfs_returns["refdate"] < datetime(2018, 1, 1)
    write_dataset(df_cotahist_etfs_returns[ix], handler.template_id)


def create_indexes_returns(handler: MarketDataETL):
    cols = [
        "refdate",
        "symbol",
        "oscillation_val",
    ]
    df_index = get_dataset(handler.indexes_dataset).to_table(columns=cols).to_pandas()
    df_index["pct_return"] = df_index["oscillation_val"]
    df_index["log_return"] = np.log(1 + df_index["pct_return"])
    write_dataset(df_index[["refdate", "symbol", "pct_return", "log_return"]], handler.template_id)


def _calc_returns(df: pd.DataFrame, value_column: str) -> pd.DataFrame:
    df = df.set_index("refdate").sort_index()
    df["log_return"] = np.log(df[value_column]).diff()
    df["pct_return"] = df[value_column].pct_change()
    return df.reset_index()


def create_returns_for_long_datasets(handler: MarketDataETL):
    df = get_dataset(handler.dataset_name).to_table(columns=handler.dataset_columns).to_pandas()
    df = df.groupby("symbol").apply(_calc_returns, handler.dataset_columns[-1]).dropna(axis=0).reset_index(drop=True)
    write_dataset(df[["refdate", "symbol", "pct_return", "log_return"]], handler.template_id)


def concat_datasets(handler: MarketDataETL):
    names = handler.dataset_names
    tables = []
    for ds in handler.datasets:
        tb = get_dataset(ds["name"]).scanner(columns=ds["columns"]).to_table().rename_columns(names)
        tables.append(tb)
    rets = pyarrow.concat_tables(tables)
    write_dataset(rets.to_pandas(), handler.template_id)


def create_b3_companies_details(handler: MarketDataETL):
    df = get_dataset(handler.companies_dataset).scanner(columns=["issuingCompany", "refdate"]).to_table().to_pandas()
    df = df.groupby(["issuingCompany"], sort=True).last().reset_index()

    sc = pyarrow.schema(
        [
            ("issuingCompany", pyarrow.string()),
            ("companyName", pyarrow.string()),
            ("tradingName", pyarrow.string()),
            ("cnpj", pyarrow.string()),
            ("industryClassification", pyarrow.string()),
            ("industryClassificationEng", pyarrow.string()),
            ("activity", pyarrow.string()),
            ("website", pyarrow.string()),
            ("hasQuotation", pyarrow.bool_()),
            ("status", pyarrow.string()),
            ("marketIndicator", pyarrow.string()),
            ("market", pyarrow.string()),
            ("institutionCommon", pyarrow.string()),
            ("institutionPreferred", pyarrow.string()),
            ("code", pyarrow.string()),
            ("codeCVM", pyarrow.string()),
            ("lastDate", pyarrow.string()),
            ("hasEmissions", pyarrow.bool_()),
            ("hasBDR", pyarrow.bool_()),
            ("typeBDR", pyarrow.string()),
            ("describleCategoryBVMF", pyarrow.string()),
            ("isin", pyarrow.string()),
            ("refdate", pyarrow.timestamp("us")),
        ]
    )
    comp_det = get_dataset(handler.companies_dataset, schema=sc).to_table().to_pandas()

    comp_det = pd.merge(df, comp_det, on=["issuingCompany", "refdate"], how="inner")

    comp_det["issuingCompany"] = comp_det["issuingCompany"].astype(str).str.strip()
    comp_det["companyName"] = comp_det["companyName"].astype(str).str.strip()
    comp_det["tradingName"] = comp_det["tradingName"].astype(str).str.strip()
    comp_det["cnpj"] = comp_det["cnpj"].astype(str).str.strip()
    comp_det["industryClassification"] = comp_det["industryClassification"].astype(str).str.strip()
    comp_det["industryClassificationEng"] = comp_det["industryClassificationEng"].astype(str).str.strip()
    comp_det["activity"] = comp_det["activity"].astype(str).str.strip()
    comp_det["website"] = comp_det["website"].astype(str).str.strip()
    comp_det["hasQuotation"] = comp_det["hasQuotation"].astype(bool)
    comp_det["status"] = comp_det["status"].astype(str).str.strip()
    comp_det["marketIndicator"] = comp_det["marketIndicator"].astype("int64")
    comp_det["market"] = comp_det["market"].astype(str).str.strip()
    comp_det["institutionCommon"] = comp_det["institutionCommon"].astype(str).str.strip()
    comp_det["institutionPreferred"] = comp_det["institutionPreferred"].astype(str).str.strip()
    comp_det["code"] = comp_det["code"].astype(str).str.strip()
    comp_det["codeCVM"] = comp_det["codeCVM"].astype("int64")
    comp_det["lastDate"] = pd.to_datetime(comp_det["lastDate"], dayfirst=True)
    comp_det["hasEmissions"] = comp_det["hasEmissions"].astype(bool)
    comp_det["hasBDR"] = comp_det["hasBDR"].astype(bool)
    comp_det["typeBDR"] = comp_det["typeBDR"].astype(str).str.strip()
    comp_det["describleCategoryBVMF"] = comp_det["describleCategoryBVMF"].astype(str).str.strip()
    comp_det["isin"] = comp_det["isin"].astype(str).str.strip()
    comp_det["refdate"] = pd.to_datetime(comp_det["refdate"])

    comp_det = comp_det.rename(
        columns={
            "issuingCompany": "asset_name",
            "companyName": "company_name",
            "tradingName": "trading_name",
            "industryClassification": "industry_classification",
            "industryClassificationEng": "industry_classification_eng",
            "hasQuotation": "has_quotation",
            "marketIndicator": "market_indicator",
            "market": "market",
            "institutionCommon": "institution_common",
            "institutionPreferred": "institution_preferred",
            "codeCVM": "code_cvm",
            "lastDate": "last_date",
            "hasEmissions": "has_emissions",
            "hasBDR": "has_bdr",
            "typeBDR": "type_bdr",
            "describleCategoryBVMF": "describle_category_bvmf",
            "code": "symbol",
        }
    )

    industry_sectors = (
        comp_det["industry_classification"]
        .str.replace(r" +/ +", "/", regex=True)
        .str.split("/", expand=True)
        .rename(columns={0: "sector", 1: "subsector", 2: "segment"})
    )
    comp_det[["sector", "subsector", "segment"]] = industry_sectors
    comp_det = comp_det.replace({"None": pd.NA})

    write_dataset(comp_det, handler.template_id)


def create_b3_companies_info(handler: MarketDataETL):
    df = get_dataset(handler.companies_dataset).scanner(columns=["code", "refdate"]).to_table().to_pandas()
    df = df.groupby(["code"], sort=True).last().reset_index()

    comp_info = get_dataset(handler.companies_dataset).to_table().to_pandas()

    comp_info = pd.merge(df, comp_info, on=["code", "refdate"], how="inner")

    comp_info["stockCapital"] = comp_info["stockCapital"].astype("float64")
    comp_info["commonSharesForm"] = comp_info["commonSharesForm"].astype(str).str.strip()
    comp_info["preferredSharesForm"] = comp_info["preferredSharesForm"].astype(str).str.strip()
    comp_info["hasCommom"] = comp_info["hasCommom"].astype(bool)
    comp_info["hasPreferred"] = comp_info["hasPreferred"].astype(bool)
    comp_info["roundLot"] = comp_info["roundLot"].astype("int64")
    comp_info["tradingName"] = comp_info["tradingName"].astype(str).str.strip()
    comp_info["numberCommonShares"] = comp_info["numberCommonShares"].astype("int64")
    comp_info["numberPreferredShares"] = comp_info["numberPreferredShares"].astype("int64")
    comp_info["totalNumberShares"] = comp_info["totalNumberShares"].astype("int64")
    comp_info["code"] = comp_info["code"].astype(str).str.strip()
    comp_info["codeCVM"] = comp_info["codeCVM"].astype("int64")
    comp_info["segment"] = comp_info["segment"].astype(str).str.strip()
    comp_info["refdate"] = pd.to_datetime(comp_info["refdate"])

    comp_info = comp_info.rename(
        columns={
            "tradingName": "trading_name",
            "code": "asset_name",
            "codeCVM": "code_cvm",
            "quotedPerSharSince": "quoted_per_shar_since",
            "commonSharesForm": "common_shares_form",
            "preferredSharesForm": "preferred_shares_form",
            "hasCommom": "has_common",
            "hasPreferred": "has_preferred",
            "roundLot": "round_lot",
            "stockCapital": "stock_capital",
            "numberCommonShares": "number_common_shares",
            "numberPreferredShares": "number_preferred_shares",
            "totalNumberShares": "total_number_shares",
        }
    )

    comp_info = comp_info.replace({"None": pd.NA})

    write_dataset(comp_info, handler.template_id)


def create_b3_companies_properties(handler: MarketDataETL):
    cols = [
        "asset_name",
        "company_name",
        "trading_name",
        "cnpj",
        "code_cvm",
        "industry_classification",
        "activity",
        "website",
        "market_indicator",
        "market",
        "sector",
        "subsector",
        "segment",
    ]
    cd0 = (
        get_dataset(handler.companies_details_dataset)
        .filter(pc.field("code_cvm") != 0)
        .scanner(columns=cols)
        .to_table()
        .to_pandas()
        .drop_duplicates()
    )

    cols = [
        "trading_name",
        "asset_name",
        "code_cvm",
        "segment",
        "has_common",
        "has_preferred",
        "quoted_per_shar_since",
        "round_lot",
        "stock_capital",
        "number_common_shares",
        "number_preferred_shares",
        "total_number_shares",
        "refdate",
    ]
    ci0 = (
        get_dataset(handler.companies_info_dataset)
        .filter(pc.field("code_cvm") != 0)
        .scanner(columns=cols)
        .to_table()
        .to_pandas()
        .rename(columns={"segment": "exchange_segment"})
    )

    companies_properties = pd.merge(cd0, ci0, on=["asset_name", "code_cvm", "trading_name"], how="outer")
    write_dataset(companies_properties, handler.template_id)


def create_b3_equity_symbols_properties(handler: MarketDataETL):
    cols = [
        "symbol",
        "asset_name",
        "trading_name",
        "company_name",
        "code_cvm",
        "isin",
        "sector",
        "subsector",
        "segment",
    ]
    companies_symbols = (
        get_dataset(handler.companies_details_dataset)
        .filter(pc.field("code_cvm") != 0)
        .scanner(columns=cols)
        .to_table()
        .to_pandas()
    )
    companies_symbols["stock_type"] = (
        companies_symbols["isin"].str[9:11].map({"PR": "PN", "OR": "ON", "PA": "PNA", "PB": "PNB", "M1": "UNT"})
    )
    companies_symbols = companies_symbols[~companies_symbols["symbol"].isna()]

    write_dataset(companies_symbols, handler.template_id)


def create_b3_listed_funds(handler: MarketDataETL):
    tables = []
    cols = ["refdate", "acronym", "fundName", "typeFund"]
    for ds in handler.datasets:
        tb = get_dataset(ds).scanner(columns=cols).to_table()
        tables.append(tb)
    df = pyarrow.concat_tables(tables).to_pandas()
    df["symbol"] = df["acronym"] + "11"
    df["typeFund"] = df["typeFund"].map({7: "FII", 20: "ETF", 19: "Fixed Income ETF"})
    df = df.rename(columns={"fundName": "fund_name", "acronym": "asset_name", "typeFund": "fund_type"})
    write_dataset(df, handler.template_id)


def create_b3_companies_cash_dividends(handler: MarketDataETL):
    sp = (
        get_dataset(handler.symbols_properties_dataset)
        .scanner(columns=["stock_type", "trading_name", "isin", "symbol"])
        .to_table()
        .to_pandas()
    )
    sp["trading_name"] = sp["trading_name"].apply(lambda x: re.sub("[^A-Z0-9]", "", x))

    # cash dividends ----
    df = get_dataset(handler.cash_dividends_dataset).scanner(columns=["tradingName", "refdate"]).to_table().to_pandas()
    df["tradingName"] = df["tradingName"].apply(lambda x: re.sub("[^A-Z0-9]", "", x))
    df = df.groupby(["tradingName"], sort=True).last().reset_index()

    cd = get_dataset(handler.cash_dividends_dataset).to_table().to_pandas()
    cd["tradingName"] = cd["tradingName"].apply(lambda x: re.sub("[^A-Z0-9]", "", x))
    cd = pd.merge(df, cd, on=["tradingName", "refdate"], how="inner")
    cd_ = cd[
        [
            "typeStock",
            "tradingName",
            "dateApproval",
            "lastDatePriorEx",
            "valueCash",
            "ratio",
            "corporateAction",
            "refdate",
        ]
    ].rename(
        columns={
            "dateApproval": "approved_on",
            "lastDatePriorEx": "last_date_prior_ex",
            "valueCash": "value_cash",
            "corporateAction": "corporate_action_label",
            "typeStock": "stock_type",
            "tradingName": "trading_name",
        }
    )
    cd_["payment_date"] = pd.NaT
    cd_ = pd.merge(cd_, sp[["stock_type", "trading_name", "symbol"]], on=["stock_type", "trading_name"]).drop(
        columns=["stock_type", "trading_name"]
    )

    # company info cash dividends ----
    df = (
        get_dataset(handler.company_info_cash_dividends_dataset)
        .scanner(columns=["isinCode", "refdate"])
        .to_table()
        .to_pandas()
    )
    df = df.groupby(["isinCode"], sort=True).last().reset_index()

    cd1 = get_dataset(handler.company_info_cash_dividends_dataset).to_table().to_pandas()
    cd1 = pd.merge(df, cd1, on=["isinCode", "refdate"], how="inner")

    cd1_ = cd1[["isinCode", "paymentDate", "approvedOn", "lastDatePrior", "rate", "label", "refdate"]].rename(
        columns={
            "approvedOn": "approved_on",
            "lastDatePrior": "last_date_prior_ex",
            "rate": "value_cash",
            "label": "corporate_action_label",
            "paymentDate": "payment_date",
            "isinCode": "isin",
        }
    )
    cd1_ = pd.merge(cd1_, sp[["isin", "symbol"]], on="isin").drop(columns="isin")
    cd1_["ratio"] = pd.NA

    # join: cash dividends + company info cash dividends ----
    cash_dividends = pd.concat([cd_, cd1_]).reset_index(drop=True)

    def ffill_n_remove_duplicates(df):
        if len(df) < 2:
            return df
        ix = ~df["payment_date"].isna()
        if ix.any():
            df["payment_date"] = df.loc[ix.idxmax(), "payment_date"]
        ix = ~df["ratio"].isna()
        if ix.any():
            df["ratio"] = df.loc[ix.idxmax(), "ratio"]
        return df.iloc[[0], :]

    cash_dividends = (
        cash_dividends.groupby(["symbol", "last_date_prior_ex", "value_cash", "corporate_action_label"])
        .apply(ffill_n_remove_duplicates)
        .reset_index(drop=True)
    )

    write_dataset(cash_dividends, handler.template_id)


def create_b3_companies_stock_dividends(handler: MarketDataETL):
    sp = get_dataset(handler.symbols_properties_dataset).scanner(columns=["isin", "symbol"]).to_table().to_pandas()

    # stock dividends ----
    df = (
        get_dataset(handler.company_info_stock_dividends_dataset)
        .scanner(columns=["isinCode", "refdate"])
        .to_table()
        .to_pandas()
    )
    df = df.groupby(["isinCode"], sort=True).last().reset_index()

    sd0 = get_dataset(handler.company_info_stock_dividends_dataset).to_table().to_pandas()
    sd0 = pd.merge(df, sd0, on=["isinCode", "refdate"], how="inner")

    sd0_ = sd0[["isinCode", "label", "lastDatePrior", "approvedOn", "factor", "refdate"]].rename(
        columns={
            "isinCode": "isin",
            "label": "corporate_action_label",
            "lastDatePrior": "last_date_prior_ex",
            "approvedOn": "approved_on",
        }
    )

    sd0_ = pd.merge(sd0_, sp[["isin", "symbol"]], on="isin", how="inner").drop(columns="isin")
    write_dataset(sd0_, handler.template_id)


def create_b3_companies_subscriptions(handler: MarketDataETL):
    sp = get_dataset(handler.symbols_properties_dataset).scanner(columns=["isin", "symbol"]).to_table().to_pandas()

    # stock dividends ----
    df = (
        get_dataset(handler.company_info_subscriptions_dataset)
        .scanner(columns=["isinCode", "refdate"])
        .to_table()
        .to_pandas()
    )
    df = df.groupby(["isinCode"], sort=True).last().reset_index()

    su0 = get_dataset(handler.company_info_subscriptions_dataset).to_table().to_pandas()
    su0 = pd.merge(df, su0, on=["isinCode", "refdate"], how="inner")

    su0_ = su0[
        [
            "isinCode",
            "label",
            "lastDatePrior",
            "approvedOn",
            "subscriptionDate",
            "tradingPeriod",
            "percentage",
            "priceUnit",
            "refdate",
        ]
    ].rename(
        columns={
            "isinCode": "isin",
            "label": "corporate_action_label",
            "lastDatePrior": "last_date_prior_ex",
            "approvedOn": "approved_on",
            "subscriptionDate": "subscription_date",
            "priceUnit": "price_unit",
            "tradingPeriod": "trading_period",
        }
    )

    su0_["trading_period_start"] = pd.to_datetime(su0_["trading_period"].str[:10], format="%d/%m/%Y", errors="coerce")
    su0_["trading_period_end"] = pd.to_datetime(su0_["trading_period"].str[-10:], format="%d/%m/%Y", errors="coerce")
    su0_ = pd.merge(su0_, sp[["isin", "symbol"]], on="isin").drop(columns="isin")

    write_dataset(su0_, handler.template_id)


def create_adjusted_prices(handler: MarketDataETL):
    ds_md = get_dataset(handler.quotes_dataset)
    if handler.quotes_dataset == "b3-bvbg086":
        seq = [
            ac.Declaration("scan", ac.ScanNodeOptions(ds_md)),
            ac.Declaration("filter", ac.FilterNodeOptions(pc.field("refdate") == pc.field("creation_date"))),
            ac.Declaration("aggregate", ac.AggregateNodeOptions([("refdate", "max", None, "max_refdate")])),
        ]
    else:
        seq = [
            ac.Declaration("scan", ac.ScanNodeOptions(ds_md)),
            ac.Declaration("aggregate", ac.AggregateNodeOptions([("refdate", "max", None, "max_refdate")])),
        ]
    t = ac.Declaration.from_sequence(seq).to_table()
    refdate = t.column("max_refdate")[0].as_py()
    symbols = (
        get_dataset(handler.returns_dataset)
        .filter(pc.field("refdate") == refdate)
        .scanner(columns=["symbol"])
        .to_table()
    )
    ohlc = (
        get_dataset(handler.quotes_dataset)
        .filter(pc.field("symbol").isin(symbols.columns[0].to_pylist()))
        .scanner(columns=handler.dataset_names)
        .to_table()
        .to_pandas()
    )
    ohlc = ohlc.set_index(["refdate", "symbol"])
    ohlc.columns = handler.candle_names
    symbols = ohlc.index.get_level_values("symbol").unique()
    rets = get_dataset(handler.returns_dataset).filter(pc.field("symbol").isin(symbols)).to_table().to_pandas()
    all_data = rets.set_index(["refdate", "symbol"]).join(ohlc).reset_index()

    def _(all_data):
        all_data = all_data.set_index("refdate").sort_index(ascending=False)
        rets = all_data["returns"]
        f = np.exp(rets).cumprod().shift()
        f.iloc[0] = 1
        close = all_data[handler.candle_names[-1]].iloc[0].item()
        adj_close = close / f
        fc = adj_close / all_data[handler.candle_names[-1]]
        adj_open = all_data[handler.candle_names[0]] * fc
        adj_high = all_data[handler.candle_names[1]] * fc
        adj_low = all_data[handler.candle_names[2]] * fc
        adj = pd.concat([adj_open, adj_high, adj_low, adj_close], axis=1)
        adj.columns = handler.candle_names

        return adj.reset_index()

    names = ["refdate", "symbol"]
    names.extend(handler.candle_names)
    adj = all_data.groupby("symbol").apply(_).reset_index()[names]
    write_dataset(adj, handler.template_id)
