# Datasets Catalog

A catalog of every queryable dataset (DuckDB view) exposed by brasa, grouped by
related content. Generated against the live database on **2026-06-08**
(87 views total).

## How to use this catalog

Datasets are exposed as DuckDB views created by `create_all_views()`:

```python
from brasa import create_all_views
create_all_views()
```

Then query the DuckDB database at `$BRASA_DATA_PATH/db/brasa.duckdb` (connect with
`read_only=False`). View names follow the pattern `"layer.dataset-name"` and **must
be double-quoted** in SQL because they contain dots and hyphens:

```sql
SELECT * FROM "input.b3-cotahist-daily" LIMIT 5
```

### Layers

- **input** — parsed raw data, closest to the source files.
- **staging** — transformed/enriched/normalized data ready for analysis.
- **custom** — ad-hoc views created on top of the layers above.

### Notes on columns

- `refdate` is the standard reference-date column across most datasets.
- Column names are **case-sensitive**. Most processed tables use `snake_case`, but
  the raw `input.b3-company-*` tables use `camelCase` (e.g. `issuingCompany`,
  `codeCVM`) — this is flagged per-table below.
- "Key columns" lists a representative subset; run `DESCRIBE "layer.dataset-name"`
  to see the full schema.

To regenerate this catalog, list views with
`SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'` and
`DESCRIBE` each one.

---

## Equity Prices & Trading

Daily and historical equity quotations.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `input.b3-cotahist-daily` | Daily stock quotations (COTAHIST) | refdate, symbol, open, high, low, average, close, volume |
| `input.b3-cotahist-yearly` | Yearly historical stock quotations (COTAHIST) | refdate, symbol, open, high, low, average, close, volume |
| `staging.b3-cotahist` | Unified COTAHIST quotations | refdate, symbol, open, high, low, close, volume |
| `staging.b3-equities-spot-market` | Equity spot-market instruments with prices | refdate, symbol, isin, corporation_name, close, open |
| `custom.ibov-daily-prices` | Ad-hoc view: daily prices for IBOV constituents | refdate, symbol, open, high, low, close, volume |

## Returns

Daily return series derived from quotations.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `staging.b3-equities-returns` | Equity daily returns | refdate, symbol, pct_return, log_return |
| `staging.b3-equities-etfs-returns` | ETF daily returns | refdate, symbol, pct_return, log_return |

## Indexes

Index compositions, portfolios, prices, and index/IOPV/BDR reference info.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `input.b3-indexes-composition` | Index compositions (raw) | refdate, indexes, symbol, corporation_name |
| `input.b3-indexes-current-portfolio` | Current index portfolio (raw) | refdate, index, symbol, weight |
| `input.b3-indexes-theoretical-portfolio` | Theoretical index portfolio (raw) | refdate, index, symbol, weight |
| `input.b3-indexes-historical-prices` | Historical index prices, wide by month (raw) | index, year, day, month01…month12 |
| `staging.b3-indexes-composition` | Index compositions (processed) | refdate, indexes, symbol, corporation_name, specification_code |
| `staging.b3-indexes-current-portfolio` | Current index portfolio (processed) | refdate, index, symbol, weight |
| `staging.b3-indexes-theoretical-portfolio` | Theoretical index portfolio (processed) | refdate, index, symbol, weight |
| `staging.b3-indexes-historical-prices` | Historical index prices, long format (processed) | refdate, symbol, value |
| `input.b3-bvbg087-indexes_info` | Index settlement/last prices (BVBG087) | refdate, symbol, settlement_price, close_price, last_price |
| `input.b3-bvbg087-iopv_info` | IOPV (indicative ETF NAV) info (BVBG087) | refdate, symbol, close_price, last_price |
| `input.b3-bvbg087-bdr_info` | BDR reference prices (BVBG087) | refdate, symbol, ref_price |

## Instrument Registries (BVBG028)

Daily instrument-definition files (BVBG028) describing tradable securities, plus
the processed equity/futures registers. These describe instruments, not prices.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `input.b3-bvbg028-equities` | Equity instruments registry | refdate, symbol, isin, corporation_name, close, open, market_capitalisation |
| `input.b3-bvbg028-equity_forwards` | Equity forward instruments | refdate, symbol, isin, underlying_security_id |
| `input.b3-bvbg028-exercise_of_equities` | Equity option exercise instruments | refdate, symbol, isin, delivery_type |
| `input.b3-bvbg028-options_on_equities` | Options-on-equities instruments | refdate, symbol, exercise_price, option_type, maturity_date |
| `input.b3-bvbg028-options_on_spot_and_futures` | Options-on-spot/futures instruments | refdate, symbol, exercise_price, option_type, maturity_date |
| `input.b3-bvbg028-derivatives_option_exercise` | Derivative option exercise instruments | refdate, symbol, isin, settlement_multiplier |
| `input.b3-bvbg028-future_contracts` | Futures contract instruments | refdate, symbol, maturity_date, contract_multiplier |
| `input.b3-bvbg028-fixed_income` | Tradable fixed-income instruments | refdate, symbol, isin, days_to_settlement |
| `input.b3-bvbg028-fixed_income_non_tradable` | Non-tradable fixed income (debentures, etc.) | refdate, symbol, isin, maturity_date, interest_rate, unit_value |
| `input.b3-bvbg028-national_bonds` | National (Treasury) bonds | refdate, isin, selic_code, maturity_date, maturity_value |
| `input.b3-bvbg028-international_bonds` | International bonds | refdate, isin, cusip, issuer_country, maturity_date |
| `input.b3-bvbg028-adrs` | ADRs | refdate, symbol, isin, cusip, program_level, proportion |
| `input.b3-bvbg028-investment_funds` | Investment funds | refdate, fund_name, instrument_asset, currency |
| `input.b3-bvbg028-cash` | Cash-market instrument definitions | refdate, instrument_asset, security_category, currency_code |
| `input.b3-bvbg028-securities_lending` | Securities-lending instruments | refdate, symbol, instrument_asset, fungibility_indicator |
| `input.b3-bvbg028-otc_derivatives` | OTC derivative instruments | refdate, instrument_asset, contract_type |
| `staging.b3-equities-register` | Processed equity register | refdate, symbol, isin, corporation_name |
| `staging.b3-equities-instrument-assets` | Distinct equity instrument assets | refdate, instrument_asset |
| `staging.b3-futures-register` | Processed futures register | refdate, symbol, maturity_date, contract_multiplier |

## Derivatives & Options Market Data

Settlement, open interest, and option pricing/volatility.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `input.b3-bvbg086` | Derivatives market data (settlement, OI, volume) | refdate, symbol, settlement_value, open_interest, volume, close |
| `input.b3-equity-options` | Equity option theoretical prices & implied vol | refdate, symbol, strike, maturity_date, volatility, theoretic_price |
| `input.b3-equities-volatility-surface` | Equity implied-volatility surface | refdate, underlying, delta, volatility, maturity_date |

## Futures & Interest-Rate

Futures settlement prices and interest-rate/inflation futures.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `input.b3-futures-settlement-prices` | Futures settlement prices (raw; frozen — no longer updated, historical data only) | refdate, symbol, commodity, price, settlement_value |
| `staging.b3-futures` | Futures contracts (BVBG028 registry) with prices and settlement rates (BVBG086) | refdate, symbol, commodity, maturity_date, close, adjusted_quote, adjusted_tax, open_interest |
| `staging.b3-futures-settlement-prices` | Futures settlement prices (processed, alt) | refdate, symbol, commodity, price, settlement_value |
| `staging.b3-futures-di1-consolidated` | DI1 (interest-rate) futures, consolidated | refdate, symbol, maturity_code, price, settlement_value |
| `staging.b3-futures-dap` | DAP (inflation) futures with implied tax | refdate, symbol, maturity_date, price, adjusted_tax |

## Macro & FX (BCB / ANBIMA)

Macroeconomic series, FX rates, and fixed-income indices.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `staging.bcb-sgs` | Daily macro series — `symbol` ∈ {CDI, SELIC, IPCA, IGPM, SETA} | refdate, symbol, value |
| `input.bcb-sgs` | Raw BCB/SGS series keyed by numeric `code` | refdate, code, value |
| `input.bcb-currency` | PTAX FX rates — `currency` ∈ {USD, EUR, GBP, JPY, CHF, CAD, AUD} | refdate, currency, bid, ask, parity_bid, parity_ask |
| `staging.b3-economic-indicators` | B3 economic indicators (grouped) | refdate, indicator_group, symbol, value |
| `input.b3-economic-indicators-fwf` | B3 economic indicators (raw fixed-width) | data_geracao_arquivo, cod_indicador, valor_indicador |
| `input.anbima-index-imab` | ANBIMA IMA fixed-income index | refdate, index_name, index_number, duration_du, pmr |

## Corporate Events

Dividends, splits, bonuses, mergers, and subscription rights.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `staging.brasa-corporate-events` | Unified events — `event_family` ∈ {CASH, STOCK, SUBSCRIPTION} | code_cvm, symbol, event_family, event_type, ex_date, value_cash, factor, ratio |
| `staging.b3-cash-dividends-events` | Cash dividends / JCP events | code_cvm, symbol, ex_date, payment_date, value_cash, yield_pct |
| `staging.b3-stock-events` | Stock events (splits, bonus, mergers) | code_cvm, symbol, event_type_raw, factor, ex_date |
| `staging.b3-subscription-events` | Subscription rights | code_cvm, symbol, subscription_price, subscription_date, ex_date |
| `input.b3-cash-dividends` | Raw cash dividends | refdate, trading_name, type_stock, value_cash, last_date_prior_ex |
| `input.b3-company-info-cash_dividends` | Company cash dividends (camelCase) | refdate, issuingCompany, paymentDate, rate, lastDatePrior |
| `input.b3-company-info-stock_dividends` | Company stock dividends (camelCase) | refdate, issuingCompany, factor, approvedOn |
| `input.b3-company-info-subscriptions` | Company subscriptions (camelCase) | refdate, issuingCompany, priceUnit, subscriptionDate |

## Company & Fund Data

Company registries, profiles, sector classification, and listed funds.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `staging.brasa-companies` | Unified company information (no `symbol`; join via `code_cvm`) | code_cvm, company_name, trading_name, sector, subsector, segment |
| `staging.b3-companies-profile` | Company profile | code_cvm, trading_name, company_name, cnpj, segment |
| `staging.b3-companies-symbols` | Symbol ↔ company mapping | symbol, isin, code_cvm, share_class, instrument_type |
| `staging.b3-companies-names` | Company name registry | refdate, code_cvm, trading_name, instrument_asset |
| `staging.brasa-industry-sectors` | Industry/sector classification | sector, subsector, gics_sector, icb_sector |
| `input.cvm-companies-registration` | CVM company registry | code_cvm, cnpj_cia, denom_social, setor_ativ, sit |
| `input.b3-company-details` | Company details (camelCase) | refdate, issuingCompany, tradingName, codeCVM, industryClassification |
| `input.b3-company-info-info` | Company general info (camelCase) | refdate, issuingCompany, codeCVM, segment, totalNumberShares |
| `input.b3-companies-capital` | Company capital structure | refdate, issuing_company, type_capital, total_qty_shares |
| `staging.b3-listed-funds` | Listed funds (ETF, FII, …) | refdate, symbol, fund_name, fund_type |
| `input.b3-listed-funds` | Listed funds registry (raw) | refdate, fund_id, acronym, fund_name, type |

## Intraday & OTC Trades

Tick-level trades and OTC/lending trade information.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `input.b3-trades-intraday` | Intraday trades (tick) | refdate, symbol, traded_price, traded_quantity, trade_time |
| `input.b3-trades-intraday-equities` | Intraday equity trades | refdate, symbol, traded_price, traded_quantity, trade_time |
| `input.b3-trades-intraday-derivatives` | Intraday derivative trades | refdate, symbol, traded_price, traded_quantity, trade_time |
| `staging.b3-trades-intraday` | Intraday trades (processed) | refdate, symbol, traded_price, traded_quantity, trade_time |
| `input.b3-lending-trades` | Securities-lending trades | refdate, symbol, interest_rate_term_trade, trade_quantity, trade_date |
| `input.b3-otc-trade-information` | OTC trade information | refdate, symbol, traded_price, volume, traded_interest_rate |

## Synthetic / Internal Price Series

Internal or synthetic intraday price series (testing / derived). Treat as
non-authoritative.

| Dataset | Description | Key Columns |
|---------|-------------|-------------|
| `input.synthetic-intraday` | Synthetic intraday prices | refdate, symbol, traded_price |
| `input.ti-eq` | Trades-intraday equities (internal) | refdate, symbol, traded_price |
| `input.ti-legacy` | Trades-intraday legacy (internal) | refdate, symbol, traded_price |
| `staging.ti-out` | Trades-intraday output (internal) | refdate, symbol, traded_price |

---

## Known issues — empty & malformed views

These views are registered but currently have no data or are misconfigured.
Querying them raises `IO Error: No files found that match the pattern …`.

| View | Issue |
|------|-------|
| `input.b3-listed-stock-etfs` | No parquet files — use `staging.b3-listed-funds` instead |
| `input.b3-listed-fixed-income-etfs` | No parquet files — use `staging.b3-listed-funds` instead |
| `input.b3-listed-cripto-etfs` | No parquet files — use `staging.b3-listed-funds` instead |
| `input.b3-listed-reits` | No parquet files — use `staging.b3-listed-funds` instead |
| `staging.b3-company-symbols` | Points at a stale `/home/wilson/snap/...` path; use `staging.b3-companies-symbols` |
| `staging/brasa-companies` | Malformed name (slash, not dot) pointing at a stale path; use `staging.brasa-companies` |
| `staging/brasa-industry-sectors` | Malformed name (slash, not dot) pointing at a stale path; use `staging.brasa-industry-sectors` |
