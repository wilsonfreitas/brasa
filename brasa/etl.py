
from datetime import datetime

from bcb import sgs, PTAX
import pandas as pd
import numpy as np
import pyarrow
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
        df_contracts["adjusted_tax"] = (100_000/df_contracts["settlement_price"] - 1)*(1/t)
    elif handler.compounding == "discrete":
        t = df_contracts["business_days"] / 252
        df_contracts["adjusted_tax"] = (100_000/df_contracts["settlement_price"])**(1/t) - 1

    df_contracts.sort_values(["refdate", "maturity_date"], inplace=True)
    df_contracts = df_contracts[["refdate", "symbol", "maturity_date", "settlement_price", "adjusted_tax",
                                 "business_days", "calendar_days"]]

    write_dataset(df_contracts, handler.template_id)


def create_b3_price_futures(handler: MarketDataETL):
    df = get_dataset(handler.futures_dataset)

    cal = Calendar()
    bcal = Calendar.load("ANBIMA")

    df_contracts = df.filter(pc.field("commodity") == handler.commodity).to_table().to_pandas()
    day = handler.maturity_day
    df_contracts['maturity_date'] = df_contracts['maturity_code'].apply(lambda x: maturity2date(x, cal, day))
    adj_maturity = bcal.following(df_contracts["maturity_date"])
    df_contracts["business_days"] = bcal.bizdays(df_contracts['refdate'], adj_maturity)
    df_contracts["calendar_days"] = cal.bizdays(df_contracts['refdate'], adj_maturity)
    df_contracts.sort_values(["refdate", "maturity_date"], inplace=True)
    df_contracts = df_contracts[["refdate", "symbol", "maturity_date", "settlement_price", "business_days",
                                 "calendar_days"]]

    write_dataset(df_contracts, handler.template_id)


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

    ptax = PTAX()
    ep = ptax.get_endpoint("CotacaoMoedaPeriodo")
    dd = (ep.query()
            .parameters(moeda="USD", dataInicial="1/1/2000", dataFinalCotacao=datetime.today().strftime("%m/%d/%Y"))
            .filter(ep.tipoBoletim == "Fechamento")
            .select(ep.dataHoraCotacao, ep.cotacaoVenda)
            .collect())
    dd_dol = dd
    dd_dol["symbol"] = "BRLUSD"
    dd_dol.columns = ["value", "refdate", "symbol"]
    dd_dol["refdate"] = pd.to_datetime(dd_dol["refdate"])
    dd_dol = dd_dol.loc[:, ["refdate", "value", "symbol"]]

    df_bcb = pd.concat([dd_cdi, dd_selic, dd_seta, dd_ipca, dd_igpm, dd_dol])

    write_dataset(df_bcb, handler.template_id)


def create_b3_curves_di1(handler: MarketDataETL):
    tb_di1 = (get_dataset(handler.futures_dataset)
              .filter(pc.field("business_days") > 0)
              .to_table())
    tb_cdi = (get_dataset(handler.bcb_dataset)
              .filter(pc.field("symbol") == 'CDI')
              .filter(pc.field("refdate").isin(tb_di1.column("refdate").unique().to_pylist()))
              .to_table())
    cal = Calendar.load(handler.calendar)
    tb_v1 = (tb_cdi
                .set_column(1, "adjusted_tax", pc.divide(tb_cdi.column("value"), 100))
                .append_column("maturity_date", pyarrow.array(cal.offset(tb_cdi.column("refdate").to_pylist(), 1)))
                .append_column("business_days", pyarrow.array([1] * tb_cdi.shape[0]))
                .select(["refdate", "symbol", "maturity_date", "business_days", "adjusted_tax"]))
    tb_di1_curve = pyarrow.concat_tables([
        tb_v1, tb_di1.select(["refdate", "symbol", "maturity_date", "business_days", "adjusted_tax"])
    ]).sort_by([("refdate", "ascending"), ("business_days", "ascending")])

    write_dataset(tb_di1_curve.to_pandas(), handler.template_id)


def create_b3_curves(handler: MarketDataETL):
    tb = (get_dataset(handler.futures_dataset)
          .filter(pc.field("business_days") > 0)
          .to_table())
    tb_curve = (tb
                .select(["refdate", "symbol", "maturity_date", "business_days", "adjusted_tax"])
                .sort_by([("refdate", "ascending"), ("business_days", "ascending")]))
    write_dataset(tb_curve.to_pandas(), handler.template_id)


def interp_ff(term, rates, terms):
    log_pu = np.log((1 + rates)**(terms/252))
    pu = np.exp(np.interp(term, terms, log_pu))
    return pu ** (252 / term) - 1


def create_b3_curves_standard_terms(handler: MarketDataETL):
    tb_curve = (get_dataset(handler.curves_dataset)
                    .to_table())
    business_days_standard = np.array(handler.standard_terms)
    symbols_standard = pyarrow.array([f"{handler.symbol_prefix}{d}" for d in business_days_standard])
    cal = Calendar.load(handler.calendar)
    tables = []
    for date in tb_curve.column("refdate").unique():
        rates = tb_curve.filter(pc.field("refdate") == date).column("adjusted_tax").to_numpy()
        terms = tb_curve.filter(pc.field("refdate") == date).column("business_days").to_numpy()
        interp_rates = pyarrow.array(interp_ff(business_days_standard, rates, terms))
        mat_dates = pyarrow.array(cal.offset(date.as_py(), business_days_standard))
        ta = pyarrow.table([
            pyarrow.array([date.as_py()] * len(interp_rates)),
            symbols_standard,
            mat_dates,
            pyarrow.array(business_days_standard),
            interp_rates
        ], names=["refdate", "symbol", "maturity_date", "business_days", "adjusted_tax"])
        tables.append(ta)
    tb_curve_standard = pyarrow.concat_tables(tables).sort_by([
        ("refdate", "ascending"), ("business_days", "ascending")
    ])
    write_dataset(tb_curve_standard.to_pandas(), handler.template_id)


def create_rate_returns(handler: MarketDataETL):
    tb_curve_standard = (get_dataset(handler.curves_dataset)
                    .to_table())
    tables = []
    for symbol in tb_curve_standard.column("symbol").unique():
        rates = tb_curve_standard.filter(pc.field("symbol") == symbol).column("adjusted_tax").to_numpy()
        dates = tb_curve_standard.filter(pc.field("symbol") == symbol).column("refdate")
        symbols = tb_curve_standard.filter(pc.field("symbol") == symbol).column("symbol")
        returns = np.concatenate([np.array([np.nan]), np.diff(rates)])
        ta = pyarrow.table([
            dates,
            symbols,
            pyarrow.array(returns)
        ], names=["refdate", "symbol", "returns"])
        tables.append(ta)

    tb_curve_standard_returns = (pyarrow.concat_tables(tables)
                                 .sort_by([("refdate", "ascending"), ("symbol", "ascending")]))
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

