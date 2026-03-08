

# B3 Instrument Classification Reference

This document is the authoritative reference for classifying instruments traded on B3 (Brasil, Bolsa, Balcão). It consolidates the classification codes used across B3's data systems — the Instruments API, the COTAHIST daily file, and ISIN encoding — to enable rich instrument classification in the data lake.

---

## ISIN Code Structure

The **ISIN (International Securities Identification Number)**, defined by ISO 6166, is a 12-character alphanumeric code that uniquely identifies a security globally. For Brazilian instruments the country code is always `BR`.

### Format

```
BR AAAA TT NNN C
│   │    │   │  └── check digit (1 char, Luhn algorithm)
│   │    │   └───── national series / class identifier (3 chars)
│   │    └────────── instrument type code (2 chars)
│   └─────────────── issuer code — typically the 4-char ticker base
└─────────────────── country code (ISO 3166-1 alpha-2)
```

**Example**: `BRPETRACNPR3` → Petrobras ON (PETR3)
- `BR` — Brazil
- `PETR` — issuer code
- `AC` — Ação (equity/share)
- `NPR` — national series
- `3` — check digit

### Instrument Type Codes Embedded in the ISIN

The 2-character type code embedded in the NSIN (national identifier portion) directly encodes the broad instrument category. Cross-referencing it with `security_category` allows unambiguous classification.

| Type Code | Category | Typical `security_category` | Notes |
|-----------|----------|-----------------------------|-------|
| `AC` | Ação (Equity / Share) | 11 (SHARES) | NSIN suffix `G` → fractional/special class; suffix `L` → IPO / follow-on (security_category 25, 26) |
| `BD` | BDR (Brazilian Depositary Receipt) | 1 (BDR) | Same suffix rules as `AC`: `G` = fractional, `L` = IPO/follow-on |
| `CT` | Fundo (Fund) | 3 (ETF EQUITIES), 6 (FUNDS/FII), 21 (ETF FOREIGN INDEX) | ETF domestic equity → 3; Fundo Imobiliário → 6; ETF tracking foreign indices → 21 |
| `IN` | Índice (Index) | 16 (INDEX) | Always filter with `security_category = 16` to avoid non-index instruments with this prefix |
| `CD` | Certificado / Unit | 13 (UNIT) | Also identifiable via `instrument_market IN (10, 20)` |
| `UN` | Unit | 13 (UNIT) | Also identifiable via `instrument_market IN (10, 20)` |

### Classification Recipes Using ISIN + Other Fields

| Instrument Class | ISIN Type | `security_category` | `instrument_market` | `instrument_segment` |
|-----------------|-----------|---------------------|---------------------|---------------------|
| Common shares (ON) | `AC` | 11 | 1 (SPOT) | 1 (EQUITY-CASH) |
| Preferred shares (PN) | `AC` | 11 | 1 (SPOT) | 1 (EQUITY-CASH) |
| IPO / Follow-on shares | `AC` | 25, 26 | — | 1 (EQUITY-CASH) |
| BDR | `BD` | 1 | 1 (SPOT) | 1 (EQUITY-CASH) |
| ETF (domestic equity) | `CT` | 3 | 8 (ETF PRIMARY MARKET) | 1 (EQUITY-CASH) |
| ETF (foreign index) | `CT` | 21 | 8 (ETF PRIMARY MARKET) | 1 (EQUITY-CASH) |
| ETF (fixed income) | `CT` | 22, 55 | 8 (ETF PRIMARY MARKET) | 1 (EQUITY-CASH) |
| Fundo Imobiliário (FII) | `CT` | 6 | 1 (SPOT) | 1 (EQUITY-CASH) |
| Unit | `CD`, `UN` | 13 | 10, 20 | 1 (EQUITY-CASH) |
| Index (reference only) | `IN` | 16 | — | 11 (INDICATORS) |

---

## `instrument_market`

Identifies the trading modality / market type. Values appear in both the B3 Instruments API and the COTAHIST file. The Portuguese names in parentheses are the COTAHIST equivalents.

| Code | Name | PT-BR Name (COTAHIST) |
|------|------|-----------------------|
| 0 | UNDEFINED | — |
| 1 | SPOT | Mercado Disponível |
| 2 | FUTURE | Mercado Futuro |
| 3 | OPTIONS ON SPOT | Opções sobre Disponível |
| 4 | OPTIONS ON FUTURE | Opções sobre Futuro |
| 5 | FORWARD | Mercado a Termo |
| 8 | ETF PRIMARY MARKET | — |
| 9 | PORTFOLIO | — |
| 10 | CASH | Vista |
| 12 | OPTION EXERCISE (CALL) | Exercício de Opções de Compra |
| 13 | OPTIONS EXERCISE (PUT) | Exercício de Opções de Venda |
| 17 | AUCTION | Leilão |
| 20 | ODD LOT | Fracionário |
| 30 | EQUITY FORWARD | Termo de Ações |
| 70 | EQUITY CALL | OPC — Opção de Compra de Ações |
| 80 | EQUITY PUT | OPV — Opção de Venda de Ações |
| 81 | SWAP | — |
| 82 | FLEXIBLE PUT OPTION | — |
| 83 | FLEXIBLE CALL OPTION | — |
| 84 | FORWARD | — |
| 85 | INDICATORS | — |
| 86 | CURVES | — |
| 87 | SURFACES | — |
| 91 | SECURITY LENDING OTC | — |
| 92 | SECURITY LENDING T0 | — |
| 93 | SECURITY LENDING T1 | — |
| 94 | SECURITY LENDING GOV. BOND | — |
| 95 | GOV. BOND REPO — SPECIFIC COLLATERAL | — |
| 96 | GOV. BOND REPO — GENERAL COLLATERAL | — |

---

## `instrument_segment`

Broad market segment the instrument belongs to. The Portuguese names in parentheses are the COTAHIST equivalents.

| Code | Name | PT-BR Name (COTAHIST) |
|------|------|-----------------------|
| 1 | EQUITY-CASH | Ações — Vista |
| 2 | EQUITY-DERIVATIVE | Ações — Derivativos |
| 3 | FIXED INCOME | Renda Fixa Privada |
| 4 | AGRIBUSINESS | Agronegócio |
| 5 | FINANCIAL | Financeiro (e.g., Futuros de Dólar) |
| 6 | METAL | Metais |
| 7 | ENERGY | Energia Elétrica |
| 8 | GOV. BONDS | Títulos Públicos |
| 9 | FX | Câmbio |
| 10 | OTC | — |
| 11 | INDICATORS | — |
| 12 | OTC TRADED SECURITIES LENDING | — |

---

## `security_category`

The most granular classification field. Groups are provided below for readability.

### Equities & Equity-Like

| Code | Name |
|------|------|
| 1 | BDR |
| 2 | COMMON EQUITIES FORWARD |
| 6 | FUNDS |
| 9 | RECEIPTS |
| 11 | SHARES |
| 12 | RIGHTS |
| 13 | UNIT |
| 14 | UNIT RECEIPTS |
| 23 | WARRANT |
| 25 | IPO — FOLLOW ON |
| 26 | AUCTIONS |
| 39 | ADR |
| 60 | FLEXIBLE EQUITIES FORWARD |
| 80 | STOCK FUTURE |
| 81 | STOCK ROLLOVER |
| 90 | DIGITAL |

### ETFs & Funds

| Code | Name |
|------|------|
| 3 | ETF EQUITIES |
| 21 | ETF FOREIGN INDEX |
| 22 | ETF FIXED INCOME |
| 35 | FIC |
| 55 | ETF GOVT BONDS |
| 56 | ETF PRIMARY MARKET — GROSS SETTLEMENT |
| 57 | ETF PRIMARY MARKET — NET SETTLEMENT |

### Options & Derivatives

| Code | Name |
|------|------|
| 4 | FORWARD POINTS |
| 5 | FORWARD RATE AGREEMENT |
| 7 | OPTION ON EQUITIES |
| 8 | OPTION ON INDEX |
| 10 | ROLLOVER |
| 15 | VOLATILITY |
| 17 | EXERCISE |
| 18 | FORW/FUT GOLD |
| 20 | FX SWAP |
| 34 | PURE GOLD |

### Fixed Income

| Code | Name |
|------|------|
| 30 | BANK CD |
| 31 | LETTER OF CREDIT |
| 32 | CPR |
| 33 | BONDS |
| 36 | INTERNATIONAL BONDS |
| 37 | CASH |
| 38 | DEBENTURES |
| 40 | CRI (Real Estate Receivable Certificates) |
| 41 | CRA (Agribusiness Receivable Certificates) |
| 42 | LETRAS FINANCEIRAS |
| 43 | PROMISSORY NOTES |
| 70 | FIXED INCOME TRADABLE INSTRUMENT T0 |
| 71 | FIXED INCOME TRADABLE INSTRUMENT T1 |

### Securities Lending

| Code | Name |
|------|------|
| 53 | TRADABLE SECURITIES LENDING |
| 54 | SECURITIES LENDING OTC |

### Reference / Analytical

| Code | Name |
|------|------|
| 16 | INDEX |
| 50 | ECONOMIC INDICATORS |
| 51 | PRICING CURVES |
| 52 | PRICING SURFACES |

### Undefined

| Code | Name |
|------|------|
| 0 | UNDEFINED |

---

## Data Quality Filters

When querying the instruments table, apply these filters to exclude synthetic, inactive, or non-tradable entries:

- `trading_start_date <> '9999-12-31'` — excludes instruments that have never started trading (placeholder entries).
- `instrument_asset <> 'TAXA'` — excludes rate/curve reference instruments that are not directly tradable.
