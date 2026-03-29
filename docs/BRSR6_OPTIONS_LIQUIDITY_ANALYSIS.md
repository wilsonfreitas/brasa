# BRSR6 Options Liquidity Analysis

**Date**: 2026-03-14

Investigation into recurring sudden liquidity spikes in BRSR6 (Banrisul) equity options.

## Context

While screening for underlyings with illiquid options that suddenly gain liquidity, BRSR6 stood out with a **R$ 15M option volume spike on March 11, 2026** — roughly 98x its 21-day average. Further investigation revealed this is a recurring quarterly pattern.

## Discovery: Screening for Liquidity Jumps

The starting point was aggregating daily option volume per underlying, computing a 21-day trailing average, and flagging days where volume vastly exceeded the recent baseline.

```sql
WITH option_data AS (
    SELECT
        ooeq.refdate,
        eq.symbol AS underlying_symbol,
        COALESCE(md.volume, 0) AS volume,
        COALESCE(md.traded_quantity, 0) AS traded_quantity,
        COALESCE(md.regular_traded_contracts, 0) AS traded_contracts
    FROM "input.b3-bvbg028-options_on_equities" ooeq
    LEFT JOIN "input.b3-bvbg028-equities" eq ON
        ooeq.underlying_security_id = eq.security_id AND
        ooeq.refdate = eq.refdate
    LEFT JOIN "input.b3-bvbg086" md ON
        ooeq.security_id = md.security_id AND
        ooeq.refdate = md.refdate
    WHERE
        ooeq.security_category = 7
        AND ooeq.instrument_segment = 2
        AND ooeq.refdate >= '2026-01-01'
        AND eq.symbol IS NOT NULL
),
daily_agg AS (
    SELECT
        refdate,
        underlying_symbol,
        SUM(volume) AS total_volume,
        SUM(traded_quantity) AS total_traded_qty,
        SUM(traded_contracts) AS total_contracts,
        COUNT(*) AS num_options
    FROM option_data
    GROUP BY refdate, underlying_symbol
),
with_rolling AS (
    SELECT
        *,
        AVG(total_volume) OVER (
            PARTITION BY underlying_symbol
            ORDER BY refdate
            ROWS BETWEEN 21 PRECEDING AND 1 PRECEDING
        ) AS avg_volume_prev_21d,
        AVG(total_contracts) OVER (
            PARTITION BY underlying_symbol
            ORDER BY refdate
            ROWS BETWEEN 21 PRECEDING AND 1 PRECEDING
        ) AS avg_contracts_prev_21d
    FROM daily_agg
)
SELECT
    refdate,
    underlying_symbol,
    total_volume,
    total_contracts,
    ROUND(avg_volume_prev_21d, 0) AS avg_vol_21d,
    ROUND(avg_contracts_prev_21d, 0) AS avg_contracts_21d,
    CASE WHEN avg_volume_prev_21d > 0
         THEN ROUND(total_volume / avg_volume_prev_21d, 1)
         ELSE NULL END AS volume_ratio,
    CASE WHEN avg_contracts_prev_21d > 0
         THEN ROUND(total_contracts / avg_contracts_prev_21d, 1)
         ELSE NULL END AS contracts_ratio
FROM with_rolling
WHERE refdate >= '2026-02-01'
  AND avg_volume_prev_21d < 500000   -- was illiquid (low avg volume)
  AND total_volume > 1000000          -- suddenly got volume
ORDER BY volume_ratio DESC NULLS LAST
LIMIT 30
```

BRSR6 appeared on **Mar 11** with a 98x volume ratio — not the highest ratio, but the largest absolute spike (R$ 15M, 25M contracts).

## Historical Pattern: Three Spikes in 6 Months

Querying the full option volume history for BRSR6 over the last 6 months revealed two prior spikes of similar nature:

```sql
WITH option_data AS (
    SELECT
        ooeq.refdate,
        COALESCE(md.volume, 0) AS volume,
        COALESCE(md.regular_traded_contracts, 0) AS traded_contracts
    FROM "input.b3-bvbg028-options_on_equities" ooeq
    LEFT JOIN "input.b3-bvbg028-equities" eq ON
        ooeq.underlying_security_id = eq.security_id AND ooeq.refdate = eq.refdate
    LEFT JOIN "input.b3-bvbg086" md ON
        ooeq.security_id = md.security_id AND ooeq.refdate = md.refdate
    WHERE
        ooeq.security_category = 7
        AND ooeq.instrument_segment = 2
        AND ooeq.refdate >= '2025-09-01'
        AND eq.symbol = 'BRSR6'
)
SELECT
    refdate,
    SUM(volume) AS total_volume,
    SUM(traded_contracts) AS total_contracts,
    COUNT(*) AS num_options
FROM option_data
GROUP BY refdate
HAVING SUM(volume) > 0
ORDER BY refdate
```

| Date | Option Volume | Stock Volume | Stock Close | Stock Move |
|---|---|---|---|---|
| **Sep 10, 2025** | R$ 4.5M | 76M (7x normal) | R$ 11.86 | +1.8% |
| **Dec 10, 2025** | R$ 9.9M | 99M (6x normal) | R$ 14.81 | +2.6% |
| **Mar 11, 2026** | R$ 15M | — | ~R$ 17.88 | — |

Each spike coincides with a massive surge in stock volume, suggesting linked equity block trades.

## Mar 11, 2026 Spike: Detailed Breakdown

### Options Traded

```sql
SELECT
    ooeq.symbol,
    ooeq.option_style,
    ooeq.option_type,
    ooeq.exercise_price,
    ooeq.maturity_date,
    COALESCE(md.volume, 0) AS volume,
    COALESCE(md.regular_traded_contracts, 0) AS traded_contracts
FROM "input.b3-bvbg028-options_on_equities" ooeq
LEFT JOIN "input.b3-bvbg028-equities" eq ON
    ooeq.underlying_security_id = eq.security_id AND ooeq.refdate = eq.refdate
LEFT JOIN "input.b3-bvbg086" md ON
    ooeq.security_id = md.security_id AND ooeq.refdate = md.refdate
WHERE
    ooeq.security_category = 7
    AND ooeq.instrument_segment = 2
    AND ooeq.refdate = '2026-03-11'
    AND eq.symbol = 'BRSR6'
    AND COALESCE(md.volume, 0) > 0
ORDER BY volume DESC
```

Top trades on March 11:

| Symbol | Style | Type | Strike | Expiry | Volume | Contracts |
|---|---|---|---|---|---|---|
| BRSRO198 | EURO | PUT | 19.87 | 2026-03-20 | 2,541,150 | 1,500,000 |
| BRSRO193 | EURO | PUT | 19.37 | 2026-03-20 | 2,413,976 | 2,001,100 |
| BRSRO203 | EURO | PUT | 20.37 | 2026-03-20 | 2,231,900 | 1,000,000 |
| BRSRC173 | AMER | CALL | 17.37 | 2026-03-20 | 2,133,846 | 2,000,600 |
| BRSRO188 | EURO | PUT | 18.87 | 2026-03-20 | 1,557,000 | 2,000,000 |
| BRSRC178 | AMER | CALL | 17.87 | 2026-03-20 | 1,397,680 | 2,004,800 |
| BRSRO183 | EURO | PUT | 18.37 | 2026-03-20 | 864,610 | 2,006,700 |
| BRSRC183 | AMER | CALL | 18.37 | 2026-03-20 | 680,240 | 2,000,600 |
| BRSRO178 | EURO | PUT | 17.87 | 2026-03-20 | 383,106 | 2,012,900 |
| BRSRC188 | AMER | CALL | 18.87 | 2026-03-20 | 283,404 | 2,019,700 |
| BRSRO173 | EURO | PUT | 17.37 | 2026-03-20 | 180,076 | 2,000,600 |
| BRSRC193 | AMER | CALL | 19.37 | 2026-03-20 | 110,031 | 2,000,800 |

All major trades expire **Mar 20** — only 9 days out.

### Open Interest Changes (Smoking Gun)

The critical query: comparing OI before and after the spike.

```sql
WITH traded_mar11 AS (
    SELECT ooeq.symbol, ooeq.security_id, ooeq.option_type, ooeq.exercise_price, ooeq.maturity_date
    FROM "input.b3-bvbg028-options_on_equities" ooeq
    JOIN "input.b3-bvbg028-equities" eq ON
        ooeq.underlying_security_id = eq.security_id AND ooeq.refdate = eq.refdate
    JOIN "input.b3-bvbg086" md ON
        ooeq.security_id = md.security_id AND ooeq.refdate = md.refdate
    WHERE ooeq.security_category = 7 AND ooeq.instrument_segment = 2
        AND ooeq.refdate = '2026-03-11' AND eq.symbol = 'BRSR6'
        AND md.volume > 100000
),
before_oi AS (
    SELECT t.symbol, md.open_interest AS oi_before
    FROM traded_mar11 t
    JOIN "input.b3-bvbg086" md ON t.security_id = md.security_id
    WHERE md.refdate = '2026-03-11'
),
after_oi AS (
    SELECT t.symbol, md.open_interest AS oi_after
    FROM traded_mar11 t
    JOIN "input.b3-bvbg086" md ON t.security_id = md.security_id
    WHERE md.refdate = '2026-03-13'
)
SELECT
    t.symbol,
    t.option_type,
    t.exercise_price,
    t.maturity_date,
    b.oi_before,
    a.oi_after,
    a.oi_after - b.oi_before AS oi_change
FROM traded_mar11 t
JOIN before_oi b ON t.symbol = b.symbol
JOIN after_oi a ON t.symbol = a.symbol
ORDER BY t.option_type, t.exercise_price
```

| Strike | Call OI Change | Put OI Change |
|---|---|---|
| 17.37 | +2,013,800 | +1,999,400 |
| 17.87 | +2,009,800 | +2,009,900 |
| 18.37 | +1,997,700 | +1,993,300 |
| 18.87 | +2,010,700 | +2,001,800 |
| 19.37 | +2,000,000 | +2,000,600 |
| 19.87 | — | +1,500,000 |
| 20.37 | — | +1,000,000 |

**Totals**: +10,032,000 new call contracts, +12,505,000 new put contracts.

The symmetric ~2M call+put at each strike is unmistakable.

## Sep 10, 2025 Spike: Prior Occurrence

```sql
SELECT
    ooeq.symbol, ooeq.option_type, ooeq.option_style,
    ooeq.exercise_price, ooeq.maturity_date,
    COALESCE(md.volume, 0) AS volume,
    COALESCE(md.regular_traded_contracts, 0) AS contracts
FROM "input.b3-bvbg028-options_on_equities" ooeq
LEFT JOIN "input.b3-bvbg028-equities" eq ON
    ooeq.underlying_security_id = eq.security_id AND ooeq.refdate = eq.refdate
LEFT JOIN "input.b3-bvbg086" md ON
    ooeq.security_id = md.security_id AND ooeq.refdate = md.refdate
WHERE ooeq.security_category = 7 AND ooeq.instrument_segment = 2
    AND ooeq.refdate = '2025-09-10' AND eq.symbol = 'BRSR6'
    AND COALESCE(md.volume, 0) > 0
ORDER BY volume DESC
```

Same structure: matched 500K-1M call+put pairs at strikes 10.83-13.83 (near spot ~11.86), all expiring Sep 19 (9 days out).

## Dec 10, 2025 Spike: Prior Occurrence

```sql
-- Same query as above with refdate = '2025-12-10'
```

Same structure: matched 1M call+put pairs at strikes 12.79-18.29, all expiring Dec 19 (9 days out).

## Interpretation

### What This Is: Box Spread / Synthetic Financing

The perfectly symmetric ~2M call+put open interest increase at each strike is the hallmark of a **box spread** or **conversion/reversal**. These are synthetic financing trades — not directional bets on the stock.

A box spread at two strikes creates a risk-free payoff equal to the difference between strikes, effectively turning options into a fixed-income instrument. The implied interest rate can differ from the market rate, creating arbitrage or financing opportunities.

### Key Observations

1. **Quarterly cadence**: Sep 10 → Dec 10 → Mar 11, roughly every 3 months — suggests fund rebalancing, dividend/JCP payments, or structured product rolls.
2. **Escalating size**: R$ 4.5M → R$ 9.9M → R$ 15M in volume, scaling up each cycle.
3. **Always near-dated expiry**: 9 calendar days to expiration each time.
4. **Stock spikes too**: Each options event coincides with 6-7x normal stock volume, suggesting the counterparties are also hedging or transacting in the underlying.
5. **Not speculative**: The perfectly matched call+put quantities point to institutional structured trades.

### Likely Participants

This is likely a **state bank (Banrisul) shareholder or large fund** doing periodic structured operations — possibly related to:

- Dividend / JCP (Juros sobre Capital Próprio) tax optimization
- Margin financing through box spreads
- Structured product rolls at quarterly rebalancing dates
- Institutional portfolio insurance or collateral management
