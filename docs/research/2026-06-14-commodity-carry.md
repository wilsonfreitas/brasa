# Commodity Carry on B3 Agricultural Futures

**Date:** 2026-06-14
**Universe:** BGI (live cattle), CCM (corn), ICF (arabica coffee), ETH (hydrous ethanol)
**Sample:** 2018-01-02 → 2025-12-08 (daily settlement; OI available from 2018)
**Verdict:** Carry is a sound *descriptor* of the term structure but is **not a tradeable standalone factor** on this small universe.

---

## 1. The trade idea

**Carry (roll yield)** is the return a futures position earns purely from the passage of time if the
spot price is unchanged, i.e. the slope of the term structure.

- **Backwardation** (near price > deferred price): a long position rolls *up* into cheaper deferred
  contracts → **positive carry**.
- **Contango** (near price < deferred price): a long rolls *down* → **negative carry**.

The classic *carry factor* posits that high-carry (backwardated) commodities outperform low-carry
(contango) ones. We test it two ways:

- **Time-series:** for each commodity, go long when backwardated / short when in contango.
- **Cross-sectional:** each month, long the highest-carry commodities, short the lowest (dollar-neutral).

Carry measure (annualised log slope):

```
carry_ann = ln(P_near / P_far) / (Δmonths / 12)
```

where `P_near` is the active (most-liquid) contract and `P_far` is the nearest deferred liquid contract.

---

## 2. Data foundations

Two data caveats discovered and handled throughout:

1. **Duplicate rows.** Both `input.b3-futures-settlement-prices` and `input.b3-bvbg086` contain exact
   2× duplicate rows. Always dedup (`SELECT DISTINCT` on settlement; `MAX(open_interest) GROUP BY symbol,refdate` on OI).
2. **Open interest starts 2018.** Settlement prices reach back to 2010, but volume/OI in `b3-bvbg086`
   only exist from 2018-01-02. The roll rule needs OI, so the sample begins in 2018.

**Roll rule.** A calendar/nearest-maturity rule fails for B3 agri: liquidity migrates by harvest season
and frequently sits in the *delivery* month (e.g. BGI's most-liquid contract is the nearest only ~40%
of days). So the **active contract = highest open interest each day**, with a **no-rollback ratchet**
(running-max maturity) to suppress flip-flop noise. Daily returns are computed *within* a contract
(roll days contribute no jump) and chained into a **ratio back-adjusted** continuous price
(`adj_price`), anchored so the last value equals the last raw settlement.

Resulting series quality:

| Commodity | Days | Rolls | Median OI | Flat days | Ann. vol |
|-----------|------|-------|-----------|-----------|----------|
| BGI (cattle) | 1936 | 45 | 7,792 | 1.9% | 14.9% |
| CCM (corn)   | 1957 | 29 | 32,728 | 0.8% | 20.5% |
| ICF (coffee) | 1852 | 19 | 4,369 | 1.7% | 32.5% |
| ETH (ethanol)| 1623 | 38 | 1,035 | **49.7%** | 16.9% |

**ETH is thin** (median OI ~1k, ~50% of days have stale/unchanged prices). Treat ETH results as unreliable.

---

## 3. Code

All artefacts are DuckDB views in `$BRASA_DATA_PATH/db/brasa.duckdb`. Create base views first:

```bash
uv run python -c "from brasa import create_all_views; create_all_views()"
```

### 3.1 Rolled continuous series — `custom.commodity-rolled-continuous`

```sql
CREATE OR REPLACE VIEW "custom.commodity-rolled-continuous" AS
WITH px AS (
  SELECT DISTINCT commodity, symbol, maturity_code, refdate, price,
    (2000+CAST(substr(maturity_code,2,2) AS INT))*12 +
    CASE substr(maturity_code,1,1) WHEN 'F' THEN 1 WHEN 'G' THEN 2 WHEN 'H' THEN 3 WHEN 'J' THEN 4
      WHEN 'K' THEN 5 WHEN 'M' THEN 6 WHEN 'N' THEN 7 WHEN 'Q' THEN 8
      WHEN 'U' THEN 9 WHEN 'V' THEN 10 WHEN 'X' THEN 11 WHEN 'Z' THEN 12 END AS mat_ym
  FROM "input.b3-futures-settlement-prices"
  WHERE commodity IN ('BGI','CCM','ICF','ETH') AND price > 0
),
oi AS (  -- dedup bvbg086 (also 2x-duplicated): one OI per symbol/day
  SELECT symbol, refdate, MAX(open_interest) AS open_interest
  FROM "input.b3-bvbg086"
  WHERE substr(symbol,1,3) IN ('BGI','CCM','ICF','ETH') AND open_interest > 0
    AND regexp_matches(symbol,'^[A-Z]{3}[FGHJKMNQUVXZ][0-9]{2}$')
  GROUP BY symbol, refdate
),
joined AS (
  SELECT p.*, o.open_interest FROM px p JOIN oi o ON p.symbol=o.symbol AND p.refdate=o.refdate
),
cand AS (  -- daily max-OI contract maturity
  SELECT commodity, refdate, mat_ym AS cand_mat_ym,
    ROW_NUMBER() OVER (PARTITION BY commodity,refdate ORDER BY open_interest DESC, mat_ym) rn
  FROM joined QUALIFY rn=1
),
ratchet AS (  -- monotone non-decreasing target maturity (no rollback)
  SELECT commodity, refdate,
    MAX(cand_mat_ym) OVER (PARTITION BY commodity ORDER BY refdate ROWS UNBOUNDED PRECEDING) AS roll_mat_ym
  FROM cand
),
active AS (  -- the contract sitting at the ratcheted maturity each day
  SELECT j.commodity, j.refdate, j.symbol AS active_symbol, j.maturity_code, j.open_interest, j.price AS raw_price
  FROM ratchet r JOIN joined j
    ON j.commodity=r.commodity AND j.refdate=r.refdate AND j.mat_ym=r.roll_mat_ym
  QUALIFY ROW_NUMBER() OVER (PARTITION BY j.commodity,j.refdate ORDER BY j.open_interest DESC)=1
),
seq AS (
  SELECT *,
    LAG(active_symbol) OVER (PARTITION BY commodity ORDER BY refdate) prev_symbol,
    LAG(raw_price)     OVER (PARTITION BY commodity ORDER BY refdate) prev_price
  FROM active
),
ret AS (
  SELECT *, CASE WHEN prev_symbol=active_symbol AND prev_price>0 THEN raw_price/prev_price-1 ELSE 0 END AS ret
  FROM seq
),
cum AS (
  SELECT *, SUM(LN(1+ret)) OVER (PARTITION BY commodity ORDER BY refdate) cum_logret FROM ret
)
SELECT commodity, refdate, active_symbol, maturity_code, open_interest, raw_price,
  (active_symbol<>prev_symbol OR prev_symbol IS NULL) AS is_roll, ret,
  EXP(cum_logret)
    / LAST_VALUE(EXP(cum_logret)) OVER (PARTITION BY commodity ORDER BY refdate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
    * LAST_VALUE(raw_price)       OVER (PARTITION BY commodity ORDER BY refdate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS adj_price
FROM cum;
```

### 3.2 Carry — `custom.commodity-carry`

```sql
CREATE OR REPLACE VIEW "custom.commodity-carry" AS
WITH near AS (
  SELECT commodity, refdate, active_symbol AS near_symbol, raw_price AS near_price, adj_price,
    (2000+CAST(substr(maturity_code,2,2) AS INT))*12 +
    CASE substr(maturity_code,1,1) WHEN 'F' THEN 1 WHEN 'G' THEN 2 WHEN 'H' THEN 3 WHEN 'J' THEN 4
      WHEN 'K' THEN 5 WHEN 'M' THEN 6 WHEN 'N' THEN 7 WHEN 'Q' THEN 8
      WHEN 'U' THEN 9 WHEN 'V' THEN 10 WHEN 'X' THEN 11 WHEN 'Z' THEN 12 END AS near_mat
  FROM "custom.commodity-rolled-continuous"
),
px AS (
  SELECT DISTINCT commodity, symbol, refdate, price,
    (2000+CAST(substr(maturity_code,2,2) AS INT))*12 +
    CASE substr(maturity_code,1,1) WHEN 'F' THEN 1 WHEN 'G' THEN 2 WHEN 'H' THEN 3 WHEN 'J' THEN 4
      WHEN 'K' THEN 5 WHEN 'M' THEN 6 WHEN 'N' THEN 7 WHEN 'Q' THEN 8
      WHEN 'U' THEN 9 WHEN 'V' THEN 10 WHEN 'X' THEN 11 WHEN 'Z' THEN 12 END mat
  FROM "input.b3-futures-settlement-prices"
  WHERE commodity IN ('BGI','CCM','ICF','ETH') AND price>0
),
oi AS (SELECT symbol, refdate, MAX(open_interest) oi FROM "input.b3-bvbg086"
  WHERE open_interest>0 AND regexp_matches(symbol,'^[A-Z]{3}[FGHJKMNQUVXZ][0-9]{2}$') GROUP BY 1,2),
far AS (  -- nearest deferred liquid contract beyond the active one
  SELECT n.commodity, n.refdate, f.symbol AS far_symbol, f.price AS far_price, f.mat AS far_mat
  FROM near n JOIN LATERAL (
    SELECT p.symbol, p.price, p.mat
    FROM px p JOIN oi o ON p.symbol=o.symbol AND p.refdate=o.refdate
    WHERE p.commodity=n.commodity AND p.refdate=n.refdate AND p.mat>n.near_mat
    ORDER BY p.mat LIMIT 1
  ) f ON TRUE
)
SELECT n.commodity, n.refdate, n.near_symbol, n.near_price, f.far_symbol, f.far_price,
  (f.far_mat-n.near_mat) AS months_gap, n.adj_price,
  n.near_price/f.far_price-1 AS carry_raw,
  LN(n.near_price/f.far_price) / ((f.far_mat-n.near_mat)/12.0) AS carry_ann
FROM near n JOIN far f USING(commodity,refdate);
```

### 3.3 Backtest (Python)

```python
import duckdb, os, numpy as np, pandas as pd
con = duckdb.connect(os.path.join(os.environ['BRASA_DATA_PATH'], 'db', 'brasa.duckdb'), read_only=False)

# month-end carry + adj_price per commodity, plus forward 1-month return
df = con.sql('''
WITH m AS (
  SELECT commodity, refdate, carry_ann, adj_price, date_trunc('month', refdate) ym,
    ROW_NUMBER() OVER (PARTITION BY commodity, date_trunc('month',refdate) ORDER BY refdate DESC) rn
  FROM "custom.commodity-carry")
SELECT commodity, ym, carry_ann, adj_price FROM m WHERE rn=1
''').df()
df = df.sort_values(['commodity','ym'])
df['fwd_ret'] = df.groupby('commodity')['adj_price'].shift(-1)/df['adj_price'] - 1
d = df.dropna(subset=['carry_ann','fwd_ret']).copy()

def spearman(a,b):  # scipy not installed; rank then Pearson
    return np.corrcoef(pd.Series(a).rank(), pd.Series(b).rank())[0,1]

def perf(r):
    r = r.dropna(); ann = r.mean()*12; vol = r.std()*np.sqrt(12)
    return ann, vol, (ann/vol if vol>0 else np.nan)

# --- Information coefficient ---
print('Pooled rank IC =', round(spearman(d['carry_ann'], d['fwd_ret']), 3))

# --- Time-series carry: position = sign(carry) ---
for label, sub in [('all 4', d), ('ex-ETH', d[d.commodity!='ETH'])]:
    ls = sub.assign(pnl=np.sign(sub['carry_ann'])*sub['fwd_ret']).groupby('ym')['pnl'].mean()
    print(label, 'TS carry  ', [round(x,2) for x in perf(ls)])
    bh = sub.groupby('ym')['fwd_ret'].mean()
    print(label, 'long-only ', [round(x,2) for x in perf(bh)])

# --- Cross-sectional carry: dollar-neutral, weight by demeaned carry ---
def xs_backtest(data, scheme):
    rows = []
    for ym, g in data.groupby('ym'):
        if len(g) < 3: continue
        c = g['carry_ann'].values; r = g['fwd_ret'].values
        w = (pd.Series(c).rank()-(len(c)+1)/2).values if scheme=='rank' else c-c.mean()
        w = w/np.abs(w).sum() if np.abs(w).sum()>0 else w
        rows.append((ym, (w*r).sum()))
    return pd.Series(dict(rows))

for label, sub in [('all 4', d), ('ex-ETH', d[d.commodity!='ETH'])]:
    for scheme in ['rank','demeaned']:
        print(label, 'XS', scheme, [round(x,2) for x in perf(xs_backtest(sub, scheme))])
```

---

## 4. Results

### Descriptive carry (works — matches commodity economics)

| Commodity | Avg carry_ann | % days backwardated | Reading |
|-----------|---------------|---------------------|---------|
| ETH | +4.3% | 34% | unreliable (thin/stale) |
| ICF (coffee) | −1.4% | 42% | ~flat; currently +18.8% (tight supply) |
| BGI (cattle) | −6.3% | 32% | mild contango |
| CCM (corn) | −16.9% | 9% | structural contango (storable, harvest) |

### Predictive / tradeable carry (does **not** work)

| Test | Result |
|------|--------|
| Pooled next-month rank IC | **+0.031** (≈ 0) |
| Cross-sectional monthly rank IC | **−0.022** (≈ 0) |
| Time-series L/S (long backwardated) | Sharpe **−0.07** (all 4), **−0.23** ex-ETH |
| Cross-sectional L/S (rank / demeaned) | Sharpe +0.10 / +0.18 all 4; **−0.14 / −0.13 ex-ETH** |
| Long-only commodities (benchmark) | Sharpe **+0.77** (~+11%/yr) |

The only positive carry numbers depend on including ETH, whose series is data-compromised — almost
certainly artefacts, not edge.

---

## 5. Conclusion & next steps

- **Carry is a good descriptor**, not a standalone alpha source on this universe.
- **Why it fails:** only 4 assets × 8 years (tiny breadth / low statistical power), and B3 agri carry is
  dominated by *predictable seasonality* (corn is structurally contango around harvest) that is already
  priced — not a harvestable risk premium.
- **Use carry combined with seasonality** (the structural contango is seasonal) or as a filter.
- **Next baseline:** time-series momentum on `adj_price` — the strong long-only Sharpe (0.77) suggests
  directional/trend structure worth capturing.

### Reproduce

```bash
uv run python -c "from brasa import create_all_views; create_all_views()"
# then run the SQL in §3.1 and §3.2 to (re)create the views, and the Python in §3.3 for the backtest.
```
