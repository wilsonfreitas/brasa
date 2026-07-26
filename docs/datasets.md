# Datasets Catalog

The authoritative catalog of every queryable dataset (DuckDB view) exposed by
brasa, grouped by related content. Catalog generated against the live database on
**2026-06-08** (87 views total); the layer framing, table-selection guidance, and
the key tables below were re-verified on **2026-07-04**.

> This database is a **work in progress** — views, columns, and coverage change.
> Re-verify the tables you depend on periodically (see *Regenerating* below), and
> treat any "Status" note here as a snapshot, not a guarantee.

## How to use this catalog

Datasets are exposed as DuckDB views created by `create_all_views()`:

```python
from brasa import create_all_views
create_all_views()
```

Then query via the `BrasaDB` helper (it owns a single shared read-write connection,
so you never build the path or hit read-only/read-write conflicts):

```python
from brasa import sql
sql('SELECT * FROM "staging.b3-cotahist" LIMIT 5').df()
```

View names follow the pattern `"layer.dataset-name"` and **must be double-quoted**
in SQL because they contain dots and hyphens.

### Layers — prefer `staging`

- **staging** — transformed/enriched/normalized data ready for analysis. **This is
  the layer end users should use.** Start here.
- **input** — raw parsed data, closest to the source files. This is an **internal
  ingestion layer, not meant for end users**. Reach for an `input.*` table only
  when a dataset has no `staging` equivalent (flagged per-table below).
- **custom** — ad-hoc/research views on top of the layers above. Not part of the
  stable, supported surface; excluded from the selection guidance.

### Table selection

For **which table to pick for a given information need**, the
`brasa-db-explorer` skill carries a "Choosing the right table" routing map derived
from this catalog. Here, each table's **Status** column records the conclusion:

- **canonical** — the table to use for its purpose.
- **raw source** — an `input` table feeding a canonical staging table; use the
  staging one unless you specifically need the raw feed.
- **deprecated** / **outdated** — superseded; a replacement is named.

### Known gaps

- **Adjusted equity OHLC does not exist yet.** `staging.b3-cotahist` prices are
  *not* adjusted for corporate events. Adjusted *returns* exist
  (`staging.b3-equities-returns`), but an adjusted OHLC price series still needs to
  be built — don't assume a table provides it.

### Notes on columns

- `refdate` is the standard reference-date column across most datasets.
- Column names are **case-sensitive**. Most processed tables use `snake_case`, but
  the raw `input.b3-company-*` tables use `camelCase` (e.g. `issuingCompany`,
  `codeCVM`) — this is flagged per-table below.
- "Key columns" lists a representative subset; run `DESCRIBE "layer.dataset-name"`
  to see the full schema.

### Regenerating

List views with
`SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'` and
`DESCRIBE` each one. Re-check row counts and `MIN/MAX(refdate)` for coverage, since
the database is a work in progress.

---

## Equity Prices & Trading

Daily and historical equity/ETF/option quotations. **`staging.b3-cotahist` is the
longest-history source** for stocks, ETFs, and equity options. Stock prices are
**not adjusted** for corporate events; ETF close is fine to use as-is (most
Brazilian ETFs pay no dividends) **except `NDIV11`, `DIVD11`, `SPYI11`**.

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.b3-cotahist` | Unified COTAHIST quotations — deepest history for stocks/ETFs/options; volume, nº de negócios, nº de trades | refdate, symbol, open, high, low, average, close, volume, trade_quantity, traded_contracts, isin | **canonical** (daily stock/ETF/option OHLC & volume) |
| `staging.b3-equities-spot-market` | Which stocks trade on the spot market (non-fractional); a filtered summary of `b3-bvbg028-equities` | refdate, symbol, isin, corporation_name, close, open | canonical (spot-market membership) |
| `input.b3-cotahist-daily` | Daily stock quotations (COTAHIST) | refdate, symbol, open, high, low, average, close, volume | raw source (use `staging.b3-cotahist`) |
| `input.b3-cotahist-yearly` | Yearly historical stock quotations (COTAHIST) | refdate, symbol, open, high, low, average, close, volume | raw source (use `staging.b3-cotahist`) |

## Returns

Daily return series derived from quotations.

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.b3-equities-returns` | **Adjusted** stock daily returns; built from `b3-bvbg086` oscillation. History **starts 2018** | refdate, symbol, pct_return, log_return | **canonical** (adjusted stock returns) |
| `staging.b3-equities-etfs-returns` | ETF daily returns — **longer history**: concatenates `b3-cotahist` (<2018) + `b3-bvbg086` (>2018) | refdate, symbol, pct_return, log_return | **canonical** (ETF returns) |

## Indexes

Index compositions, portfolios, prices, and index/IOPV/BDR reference info.

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.b3-indexes-composition` | Index **membership** — what's in IBOV etc. (no weights) | refdate, indexes, symbol, corporation_name, specification_code | **canonical** (index membership) |
| `staging.b3-indexes-theoretical-portfolio` | **Official quarterly rebalance target weights** (valid for the quarter) | refdate, index, symbol, weight | **canonical** (target weights) |
| `staging.b3-indexes-current-portfolio` | **Live/drifted weights** as of a given day | refdate, index, symbol, weight | **canonical** (live weights) |
| `staging.b3-indexes-historical-prices` | Index **level** over time (long format) | refdate, symbol, value | **canonical** (index level) |
| `input.b3-indexes-composition` | Index compositions (raw) | refdate, indexes, symbol, corporation_name | raw source |
| `input.b3-indexes-current-portfolio` | Current index portfolio (raw) | refdate, index, symbol, weight | raw source |
| `input.b3-indexes-theoretical-portfolio` | Theoretical index portfolio (raw) | refdate, index, symbol, weight | raw source |
| `input.b3-indexes-historical-prices` | Historical index prices, wide by month (raw) | index, year, day, month01…month12 | raw source |
| `input.b3-bvbg087-indexes_info` | Index settlement/last prices (BVBG087) | refdate, symbol, settlement_price, close_price, last_price | raw source |
| `input.b3-bvbg087-iopv_info` | IOPV (indicative ETF NAV) info (BVBG087) | refdate, symbol, close_price, last_price | input-only |
| `input.b3-bvbg087-bdr_info` | BDR reference prices (BVBG087) | refdate, symbol, ref_price | input-only |

## Instrument Registries (BVBG028)

Daily instrument-definition files (BVBG028) describing tradable securities, plus
the processed equity/futures registers. These describe instruments, not prices.

These `input.b3-bvbg028-*` files are the **raw daily registries**. Prefer the
processed registers where they exist — `staging.b3-equities-register`,
`staging.b3-futures-register` — and reach into a specific `input.b3-bvbg028-*` only
for an instrument class with no processed table (e.g. options, bonds, ADRs, funds).
`input.b3-bvbg028-equities` is the canonical answer to *"what stocks/assets exist"*
(daily registry of everything listed), and `input.b3-bvbg028-options_on_equities`
is the options registry (strikes/maturities).

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

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `input.b3-bvbg086` | Market data for **all** B3-traded assets: OHLC, OI, volume, and the daily **`oscillation`** column used to build adjusted stock returns. Settlement is expressed via `adjusted_quote`/`adjusted_tax` — **there is no `settlement_value` column**. Starts **2018** | refdate, symbol, open, high, low, close, average, oscillation, open_interest, volume, traded_contracts, adjusted_quote, adjusted_tax, adjusted_value_contract | raw source (use `staging.b3-futures` for futures; `staging.b3-equities-returns` for returns) |
| `input.b3-equity-options` | Equity option theoretical prices & implied vol | refdate, symbol, strike, maturity_date, volatility, theoretic_price | **canonical** (option theoretical price / implied vol) — input-only |
| `input.b3-equities-volatility-surface` | Equity implied-volatility surface | refdate, underlying, delta, volatility, maturity_date | **canonical** (vol surface) — input-only |

## Futures & Interest-Rate

Futures daily data, settlement, and interest-rate/inflation curves.

Datasets suffixed **`-sp`** derive from the frozen `b3-futures-settlement-prices`
feed (pre-2018 tail; no maturity/OI at the source). They are special-purpose —
reach for them only when the bvbg-based canonical datasets can't cover the need.

**Futures daily data.** For **>2018**, `staging.b3-futures` is canonical — it is the
`b3-bvbg086` prices/OI + `b3-bvbg028-future_contracts` contract metadata already
joined and cleaned (settlement via `adjusted_quote`/`adjusted_tax`, **not**
`settlement_value`). For the **pre-2018** tail, `input.b3-futures-settlement-prices`
is the only source, but it is frozen and incomplete (no maturity/OI) — usable only
for simple standardized contracts like DI1, DOL, DAP.

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.b3-futures` | **Futures daily data (>2018)** — BVBG028 contracts joined to BVBG086 prices/OI/settlement | refdate, symbol, commodity, maturity_date, contract_multiplier, open, high, low, close, average, adjusted_quote, adjusted_tax, open_interest, volume | **canonical** (futures daily data, >2018) |
| `staging.b3-futures-register` | Processed futures contract registry (maturity, multiplier) | refdate, symbol, maturity_date, contract_multiplier | **canonical** (contract registry) |
| `input.b3-futures-settlement-prices` | Raw settlement prices; frozen. Pre-2018 tail only; incomplete (no maturity/OI) | refdate, symbol, commodity, price, settlement_value | raw source (pre-2018 tail; simple contracts only) |
| `staging.b3-futures-settlement-prices` | Processed settlement prices, built on the frozen raw feed | refdate, symbol, commodity, maturity_code, price, settlement_value | ⚠️ **outdated** — ships **2× duplicate rows** (dedup before use); prefer `staging.b3-futures` |

**First-generic (continuous front-contract) series.** One row per refdate, tracking
the front contract for a commodity in `staging.b3-futures`; `ref` holds the original
ticker of the contract backing that day. The front contract rolls the day after
maturity (`maturity_date > refdate`), except DDI which additionally requires at
least 2 ANBIMA business days to maturity (`business_days_to_ignore=[0, 1]`, computed
via the `bizdays` step since `days_to_settlement` is null at the source). History
starts 2016-02-01. Quote/tax nulls at the source pass through unfiltered.

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.b3-futures-win-first-generic` | Mini-index (WIN) front contract, symbol WINT01 | refdate, symbol, ref, maturity_date, adjusted_quote | **canonical** |
| `staging.b3-futures-wdo-first-generic` | Mini-dollar (WDO) front contract, symbol WDOT01 | refdate, symbol, ref, maturity_date, adjusted_quote | **canonical** |
| `staging.b3-futures-dol-first-generic` | Full-dollar (DOL) front contract, symbol DOLT01 | refdate, symbol, ref, maturity_date, adjusted_quote | **canonical** |
| `staging.b3-futures-ind-first-generic` | Full-index (IND) front contract, symbol INDT01 | refdate, symbol, ref, maturity_date, adjusted_quote | **canonical** |
| `staging.b3-futures-ddi-first-generic` | Cupom cambial (DDI) front contract, symbol DDIT01; requires ≥2 business days to maturity | refdate, symbol, ref, maturity_date, adjusted_quote, adjusted_tax | **canonical** |

**Roll-adjusted (back-adjusted continuous) series.** Continuous front-contract price
series for WIN and WDO, back-adjusted by the roll ratio (second contract settlement
÷ front contract settlement on the maturity day) so historical levels are consistent
with today's front contract — unlike the first-generic series above, which simply
splices raw quotes across rolls. History starts 2016-02-01; nulls at the source pass
through (a null front quote produces a null price, and the returns series drops both
that row and its successor via the standard lag filter).

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.b3-futures-win-adjusted` | Mini-index (WIN) back-adjusted continuous price, symbol WINADJ | refdate, symbol, price | **canonical** |
| `staging.b3-futures-wdo-adjusted` | Mini-dollar (WDO) back-adjusted continuous price, symbol WDOADJ | refdate, symbol, price | **canonical** |
| `staging.b3-futures-win-adjusted-returns` | Daily pct/log returns of WINADJ | refdate, symbol, pct_return, log_return | **canonical** |
| `staging.b3-futures-wdo-adjusted-returns` | Daily pct/log returns of WDOADJ | refdate, symbol, pct_return, log_return | **canonical** |

### Curves (nominal DI1 & real DAP)

Vertex-level yield curves. `-standard` variants interpolate the curve to
**standardized fixed-term vertices** (e.g. `DI1T252` = 252 business days; ANBIMA is
only the business-day calendar convention). The rate is `adjusted_tax` (annualized).

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.b3-curves-di1` | DI1 nominal curve at **actual contract maturities** (+ CDI overnight vertex) | refdate, symbol, maturity_date, business_days, adjusted_tax | **canonical** (nominal curve, contract vertices) |
| `staging.b3-curves-di1-standard` | DI1 nominal curve at **standardized fixed-term vertices** (fixed-tenor time series) | refdate, symbol, maturity_date, business_days, adjusted_tax | **canonical** (nominal curve, standard vertices) |
| `staging.b3-curves-di1-standard-returns` | Returns of a fixed-tenor DI1 vertex | refdate, symbol, returns | **canonical** (nominal vertex returns) |
| `staging.b3-curves-dap` / `-standard` / `-standard-returns` | Real-rate curves mirroring the DI1 family, sourced from `staging.b3-futures` (commodity DAP) | *(as DI1)* | canonical |

## Macro & FX (BCB / ANBIMA)

Macroeconomic series, FX rates, and fixed-income indices.

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.bcb-sgs` | Daily **macro** series — `symbol` ∈ {CDI, SELIC, IPCA, IGPM, SETA} | refdate, symbol, value | **canonical** (macro series) |
| `staging.b3-economic-indicators` | B3 economic indicators (grouped) — the pricing companion to use **together with B3 contracts, pricing & curves** (not general macro) | refdate, indicator_group, symbol, value | **canonical** (pricing/contract indicators) |
| `input.bcb-currency` | PTAX FX rates — `currency` ∈ {USD, EUR, GBP, JPY, CHF, CAD, AUD} | refdate, currency, bid, ask, parity_bid, parity_ask | **canonical** (FX/PTAX) — input-only |
| `input.bcb-sgs` | Raw BCB/SGS series keyed by numeric `code` — fallback for series outside the 5 macro symbols | refdate, code, value | raw source (other SGS series) |
| `input.b3-economic-indicators-fwf` | B3 economic indicators (raw fixed-width) | data_geracao_arquivo, cod_indicador, valor_indicador | raw source |
| `input.anbima-index-imab` | ANBIMA IMA fixed-income index | refdate, index_name, index_number, duration_du, pmr | **canonical** (IMA-B index) — input-only |

## Corporate Events

Dividends, splits, bonuses, mergers, and subscription rights.

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.brasa-corporate-events` | **Unified** events — one place for all; `event_family` ∈ {CASH, STOCK, SUBSCRIPTION} | code_cvm, symbol, event_family, event_type, ex_date, value_cash, factor, ratio | **canonical** (all corporate events) |
| `staging.b3-cash-dividends-events` | Cash dividends / JCP events (with yield) | code_cvm, symbol, ex_date, payment_date, value_cash, yield_pct | canonical drill-down (cash/JCP) |
| `staging.b3-stock-events` | Stock events (splits, bonus, mergers) — adjustment `factor` | code_cvm, symbol, event_type_raw, factor, ex_date | canonical drill-down (stock events) |
| `staging.b3-subscription-events` | Subscription rights | code_cvm, symbol, subscription_price, subscription_date, ex_date | canonical drill-down (subscriptions) |
| `input.b3-cash-dividends` | Raw cash dividends | refdate, trading_name, type_stock, value_cash, last_date_prior_ex | raw source |
| `input.b3-company-info-cash_dividends` | Company cash dividends (camelCase) | refdate, issuingCompany, paymentDate, rate, lastDatePrior | raw source |
| `input.b3-company-info-stock_dividends` | Company stock dividends (camelCase) | refdate, issuingCompany, factor, approvedOn | raw source |
| `input.b3-company-info-subscriptions` | Company subscriptions (camelCase) | refdate, issuingCompany, priceUnit, subscriptionDate | raw source |

## Company & Fund Data

Company registries, profiles, sector classification, and listed funds.

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.brasa-companies` | Unified **company-level** information — **no `symbol` column, join via `code_cvm`** | code_cvm, company_name, trading_name, sector, subsector, segment | **canonical** (company info) |
| `staging.b3-companies-symbols` | **Ticker ↔ company** bridge | symbol, isin, code_cvm, share_class, instrument_type | **canonical** (symbol↔company) |
| `staging.brasa-industry-sectors` | Industry/sector classification — **best source** for sector/industry (prefer over the sector columns on `brasa-companies`) | sector, subsector, gics_sector, icb_sector | **canonical** (sector/industry) |
| `staging.b3-companies-profile` | Company profile (adds CNPJ) | code_cvm, trading_name, company_name, cnpj, segment | canonical (CNPJ/profile) |
| `staging.b3-companies-names` | Company name registry | refdate, code_cvm, trading_name, instrument_asset | supporting |
| `staging.b3-listed-funds` | Listed funds (ETF, FII, Fixed Income ETF) | refdate, symbol, fund_name, fund_type | **canonical** (listed funds) |
| `input.cvm-companies-registration` | CVM company registry (CNPJ, situation) | code_cvm, cnpj_cia, denom_social, setor_ativ, sit | raw source |
| `input.b3-company-details` | Company details (camelCase) | refdate, issuingCompany, tradingName, codeCVM, industryClassification | raw source |
| `input.b3-company-info-info` | Company general info (camelCase) | refdate, issuingCompany, codeCVM, segment, totalNumberShares | raw source |
| `input.b3-companies-capital` | Company capital structure | refdate, issuing_company, type_capital, total_qty_shares | raw source |
| `input.b3-listed-funds` | Listed funds registry (raw) | refdate, fund_id, acronym, fund_name, type | raw source |

## Intraday & OTC Trades

Tick-level trades and OTC/lending trade information.

| Dataset | Description | Key Columns | Status |
|---------|-------------|-------------|--------|
| `staging.b3-trades-intraday` | Intraday trades (processed) — shorter coverage (**2023+**) | refdate, symbol, traded_price, traded_quantity, trade_time | canonical when its coverage suffices |
| `input.b3-trades-intraday` | Intraday trades (tick) | refdate, symbol, traded_price, traded_quantity, trade_time | **canonical** (deepest intraday history) — input-only |
| `input.b3-trades-intraday-equities` | Intraday equity trades | refdate, symbol, traded_price, traded_quantity, trade_time | input-only |
| `input.b3-trades-intraday-derivatives` | Intraday derivative trades | refdate, symbol, traded_price, traded_quantity, trade_time | input-only |
| `input.b3-lending-trades` | Securities-lending trades | refdate, symbol, interest_rate_term_trade, trade_quantity, trade_date | input-only |
| `input.b3-otc-trade-information` | OTC trade information | refdate, symbol, traded_price, volume, traded_interest_rate | input-only |

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
