# Plan: Add All Instrument Types to b3-bvbg028 Template

## Context

The BVBG028 file from B3 contains a daily register of **all** listed instruments.
Currently the template only extracts 3 of 17 instrument types (equities, options on equities, futures).
The reader code (`B3ReadBVBG028XmlStep` in `brasa/engine/pipeline/steps/b3_steps.py`) is already generic — it dynamically maps XML tags to datasets via the `tag` field in the YAML config. **No code changes are needed** — only YAML template additions.

Source file analyzed: `BVBG.028.02_BV000327202603060327151039412501386.xml.gz` (2026-03-06)

## Instrument Types Overview

| # | Dataset Name | XML Tag | Records | Description |
|---|---|---|---|---|
| 1 | `equities` | `EqtyInf` | 15,603 | **Already exists** |
| 2 | `options_on_equities` | `OptnOnEqtsInf` | 86,383 | **Already exists** |
| 3 | `future_contracts` | `FutrCtrctsInf` | 691 | **Already exists** |
| 4 | `exercise_of_equities` | `ExrcEqtsInf` | 15,378 | Option exercise instruments on equities |
| 5 | `options_on_spot_and_futures` | `OptnOnSpotAndFutrsInf` | 5,671 | Options on spot/futures (IDI, commodities) |
| 6 | `derivatives_option_exercise` | `DrvsOptnExrcInf` | 5,671 | Exercise instruments for derivatives options |
| 7 | `strategies` | `StrtgyInf` | 1,061 | Strategy instruments (spreads, combos) |
| 8 | `equity_forwards` | `EqtyFwdInf` | 677 | Equity forward contracts (termo) |
| 9 | `international_bonds` | `IntlBdInf` | 627 | International government bonds |
| 10 | `fixed_income` | `FxdIncmInf` | 376 | Tradable fixed income (ETFs, debentures) |
| 11 | `national_bonds` | `NtlBdInf` | 367 | Brazilian federal government bonds |
| 12 | `fixed_income_non_tradable` | `FxdIncmNonTrdblInf` | 128 | Non-tradable fixed income (underlying FI) |
| 13 | `adrs` | `ADRInf` | 31 | American Depositary Receipts |
| 14 | `securities_lending` | `BTCInf` | 7 | Securities lending (BTC) |
| 15 | `otc_derivatives` | `OTCInf` | 6 | OTC derivatives (swaps, flex options) |
| 16 | `cash` | `CshInf` | 3 | Cash/collateral instruments |
| 17 | `investment_funds` | `FICInf` | 2 | Investment fund of funds (FIC) |

---

## Schemas for Each New Dataset

All datasets share **9 common header fields** (same as existing datasets). The common header comes from the outer `<Instrm>` element:

```xml
<!-- Common header structure (present in ALL instrument types) -->
<Instrm>
  <RptParams>
    <RptDtAndTm>
      <Dt>2026-03-06</Dt>           <!-- refdate -->
    </RptDtAndTm>
  </RptParams>
  <FinInstrmId>
    <OthrId>
      <Id>200000072072</Id>          <!-- security_id -->
      <Tp>
        <Prtry>8</Prtry>            <!-- security_proprietary -->
      </Tp>
    </OthrId>
    <PlcOfListg>
      <MktIdrCd>BVMF</MktIdrCd>     <!-- security_market -->
    </PlcOfListg>
  </FinInstrmId>
  <FinInstrmAttrCmon>
    <Asst>JPPV</Asst>               <!-- instrument_asset -->
    <AsstDesc>JPPV</AsstDesc>       <!-- instrument_asset_description -->
    <Mkt>10</Mkt>                   <!-- instrument_market -->
    <Sgmt>1</Sgmt>                  <!-- instrument_segment -->
    <Desc>FII JPPVALUECI</Desc>     <!-- instrument_description -->
  </FinInstrmAttrCmon>
  <InstrmInf>
    <!-- instrument-type-specific content here -->
  </InstrmInf>
</Instrm>
```

```yaml
# Common header fields (present in ALL datasets)
- name: refdate
  description: Reference date
  tag: RptParams/RptDtAndTm/Dt
  type: date
- name: security_id
  description: Security ID
  tag: FinInstrmId/OthrId/Id
  type: string
- name: security_proprietary
  description: Security proprietary code
  tag: FinInstrmId/OthrId/Tp/Prtry
  type: string
- name: security_market
  description: Security market code
  tag: FinInstrmId/PlcOfListg/MktIdrCd
  type: string
- name: instrument_asset
  description: Instrument asset code
  tag: FinInstrmAttrCmon/Asst
  type: string
- name: instrument_asset_description
  description: Instrument asset description
  tag: FinInstrmAttrCmon/AsstDesc
  type: string
- name: instrument_market
  description: Instrument market code
  tag: FinInstrmAttrCmon/Mkt
  type: string
- name: instrument_segment
  description: Instrument segment code
  tag: FinInstrmAttrCmon/Sgmt
  type: string
- name: instrument_description
  description: Instrument description
  tag: FinInstrmAttrCmon/Desc
  type: string
```

Below are the **specific fields** for each new dataset (header fields omitted for brevity — they will be included in the actual YAML). Each section shows the raw XML sample first, then the corresponding schema mapping.

---

### 4. `exercise_of_equities` (ExrcEqtsInf) — 15,378 records
Exercise instruments for equity options.

**XML Sample:**
```xml
<ExrcEqtsInf>
  <SctyCtgy>17</SctyCtgy>
  <TckrSymb>SUZBH63E</TckrSymb>
  <ISIN>BRSUZB3H0E55</ISIN>
  <TradgCcy>BRL</TradgCcy>
  <TradgStartDt>2025-02-04</TradgStartDt>
  <TradgEndDt>2026-08-21</TradgEndDt>
  <DlvryTp>1</DlvryTp>
  <OptnExrcInstrmId>
    <OthrId>
      <Id>200001860914</Id>
      <Tp><Prtry>8</Prtry></Tp>
    </OthrId>
    <PlcOfListg><MktIdrCd>BVMF</MktIdrCd></PlcOfListg>
  </OptnExrcInstrmId>
</ExrcEqtsInf>
```

**Schema:**
```yaml
tag: ExrcEqtsInf
fields:
  # ... common header fields ...
  - name: security_category         # SctyCtgy = "17"
    tag: InstrmInf/ExrcEqtsInf/SctyCtgy
    type: string
  - name: symbol                    # TckrSymb = "SUZBH63E"
    tag: InstrmInf/ExrcEqtsInf/TckrSymb
    type: string
  - name: isin                      # ISIN = "BRSUZB3H0E55"
    tag: InstrmInf/ExrcEqtsInf/ISIN
    type: string
  - name: trading_currency          # TradgCcy = "BRL"
    tag: InstrmInf/ExrcEqtsInf/TradgCcy
    type: string
  - name: trading_start_date        # TradgStartDt = "2025-02-04"
    tag: InstrmInf/ExrcEqtsInf/TradgStartDt
    type: date
  - name: trading_end_date          # TradgEndDt = "2026-08-21"
    tag: InstrmInf/ExrcEqtsInf/TradgEndDt
    type: date
  - name: delivery_type             # DlvryTp = "1"
    tag: InstrmInf/ExrcEqtsInf/DlvryTp
    type: string
  - name: option_exercise_security_id         # OptnExrcInstrmId/OthrId/Id
    tag: InstrmInf/ExrcEqtsInf/OptnExrcInstrmId/OthrId/Id
    type: string
  - name: option_exercise_security_proprietary # OptnExrcInstrmId/OthrId/Tp/Prtry
    tag: InstrmInf/ExrcEqtsInf/OptnExrcInstrmId/OthrId/Tp/Prtry
    type: string
  - name: option_exercise_security_market     # OptnExrcInstrmId/PlcOfListg/MktIdrCd
    tag: InstrmInf/ExrcEqtsInf/OptnExrcInstrmId/PlcOfListg/MktIdrCd
    type: string
```

---

### 5. `options_on_spot_and_futures` (OptnOnSpotAndFutrsInf) — 5,671 records
Options on spot and futures contracts (IDI, commodities like soy).

**XML Sample:**
```xml
<OptnOnSpotAndFutrsInf>
  <ISIN>BRBMEFSJ1H35</ISIN>
  <TckrSymb>SJCK26P002350</TckrSymb>
  <ExrcPric>23.5</ExrcPric>
  <ExrcStyle>AMER</ExrcStyle>
  <XprtnDt>2026-04-24</XprtnDt>
  <XprtnCd>KVM0</XprtnCd>
  <OptnTp>PUTT</OptnTp>
  <CtrctMltplr>450.000000000</CtrctMltplr>
  <AsstQtnQty>1.000000000</AsstQtnQty>
  <PmtTp>0</PmtTp>
  <AllcnRndLot>1</AllcnRndLot>
  <CFICd>OPATCS</CFICd>
  <UndrlygInstrmId>
    <OthrId>
      <Id>100000229417</Id>
      <Tp><Prtry>8</Prtry></Tp>
    </OthrId>
    <PlcOfListg><MktIdrCd>BVMF</MktIdrCd></PlcOfListg>
  </UndrlygInstrmId>
  <PrmUpfrntInd>true</PrmUpfrntInd>
  <TradgStartDt>2025-02-13</TradgStartDt>
  <TradgEndDt>2026-04-24</TradgEndDt>
  <OpngPosLmtDt>2026-04-23</OpngPosLmtDt>
  <TradgCcy>USD</TradgCcy>
  <AsstSttlmInd>
    <OthrId>
      <Id>9800508</Id>
      <Tp><Prtry>8</Prtry></Tp>
    </OthrId>
    <PlcOfListg><MktIdrCd>BVMF</MktIdrCd></PlcOfListg>
  </AsstSttlmInd>
  <WdrwlDays>33</WdrwlDays>
  <WrkgDays>33</WrkgDays>
  <ClnrDays>49</ClnrDays>
</OptnOnSpotAndFutrsInf>
```

**Schema:**
```yaml
tag: OptnOnSpotAndFutrsInf
fields:
  # ... common header fields ...
  - name: isin                      # ISIN
    tag: InstrmInf/OptnOnSpotAndFutrsInf/ISIN
    type: string
  - name: symbol                    # TckrSymb = "SJCK26P002350"
    tag: InstrmInf/OptnOnSpotAndFutrsInf/TckrSymb
    type: string
  - name: exercise_price            # ExrcPric = "23.5"
    tag: InstrmInf/OptnOnSpotAndFutrsInf/ExrcPric
    type: numeric
  - name: exercise_style            # ExrcStyle = "AMER" | "EURO"
    tag: InstrmInf/OptnOnSpotAndFutrsInf/ExrcStyle
    type: string
  - name: maturity_date             # XprtnDt
    tag: InstrmInf/OptnOnSpotAndFutrsInf/XprtnDt
    type: date
  - name: expiration_code           # XprtnCd = "KVM0"
    tag: InstrmInf/OptnOnSpotAndFutrsInf/XprtnCd
    type: string
  - name: option_type               # OptnTp = "CALL" | "PUTT"
    tag: InstrmInf/OptnOnSpotAndFutrsInf/OptnTp
    type: string
  - name: contract_multiplier       # CtrctMltplr = "450.000000000"
    tag: InstrmInf/OptnOnSpotAndFutrsInf/CtrctMltplr
    type: numeric
  - name: asset_quotation_quantity  # AsstQtnQty
    tag: InstrmInf/OptnOnSpotAndFutrsInf/AsstQtnQty
    type: numeric
  - name: payment_type              # PmtTp = "0"
    tag: InstrmInf/OptnOnSpotAndFutrsInf/PmtTp
    type: string
  - name: allocation_lot_size       # AllcnRndLot = "1"
    tag: InstrmInf/OptnOnSpotAndFutrsInf/AllcnRndLot
    type: integer
  - name: cfi_code                  # CFICd
    tag: InstrmInf/OptnOnSpotAndFutrsInf/CFICd
    type: string
  - name: underlying_security_id    # UndrlygInstrmId/OthrId/Id
    tag: InstrmInf/OptnOnSpotAndFutrsInf/UndrlygInstrmId/OthrId/Id
    type: string
  - name: underlying_security_proprietary
    tag: InstrmInf/OptnOnSpotAndFutrsInf/UndrlygInstrmId/OthrId/Tp/Prtry
    type: string
  - name: underlying_security_market
    tag: InstrmInf/OptnOnSpotAndFutrsInf/UndrlygInstrmId/PlcOfListg/MktIdrCd
    type: string
  - name: premium_upfront_indicator # PrmUpfrntInd = "true"
    tag: InstrmInf/OptnOnSpotAndFutrsInf/PrmUpfrntInd
    type: string
  - name: trading_start_date
    tag: InstrmInf/OptnOnSpotAndFutrsInf/TradgStartDt
    type: date
  - name: trading_end_date
    tag: InstrmInf/OptnOnSpotAndFutrsInf/TradgEndDt
    type: date
  - name: opening_position_limit_date # OpngPosLmtDt
    tag: InstrmInf/OptnOnSpotAndFutrsInf/OpngPosLmtDt
    type: date
  - name: trading_currency
    tag: InstrmInf/OptnOnSpotAndFutrsInf/TradgCcy
    type: string
  - name: asset_settlement_security_id  # AsstSttlmInd/OthrId/Id
    tag: InstrmInf/OptnOnSpotAndFutrsInf/AsstSttlmInd/OthrId/Id
    type: string
  - name: asset_settlement_security_proprietary
    tag: InstrmInf/OptnOnSpotAndFutrsInf/AsstSttlmInd/OthrId/Tp/Prtry
    type: string
  - name: asset_settlement_security_market
    tag: InstrmInf/OptnOnSpotAndFutrsInf/AsstSttlmInd/PlcOfListg/MktIdrCd
    type: string
  - name: withdrawal_days           # WdrwlDays
    tag: InstrmInf/OptnOnSpotAndFutrsInf/WdrwlDays
    type: integer
  - name: working_days              # WrkgDays
    tag: InstrmInf/OptnOnSpotAndFutrsInf/WrkgDays
    type: integer
  - name: calendar_days             # ClnrDays
    tag: InstrmInf/OptnOnSpotAndFutrsInf/ClnrDays
    type: integer
```

---

### 6. `derivatives_option_exercise` (DrvsOptnExrcInf) — 5,671 records
Exercise instruments for derivatives options (paired 1:1 with OptnOnSpotAndFutrsInf).

**XML Sample:**
```xml
<DrvsOptnExrcInf>
  <SctyCtgy>17</SctyCtgy>
  <TckrSymb>SJCK26P002350E</TckrSymb>
  <ISIN>BRBMEFSJ1H35</ISIN>
  <DerivOptnExrcInstrmId>
    <OthrId>
      <Id>200001875038</Id>
      <Tp><Prtry>8</Prtry></Tp>
    </OthrId>
    <PlcOfListg><MktIdrCd>BVMF</MktIdrCd></PlcOfListg>
  </DerivOptnExrcInstrmId>
  <WdrwlDays>33</WdrwlDays>
  <WrkgDays>33</WrkgDays>
  <ClnrDays>49</ClnrDays>
</DrvsOptnExrcInf>
```

> Note: Some records also contain `OptnDlvryTp`, `SttlmIndMltplr`, and `AsstSttlmInd` (nested). The sample above is minimal; the schema captures all observed fields.

**Schema:**
```yaml
tag: DrvsOptnExrcInf
fields:
  # ... common header fields ...
  - name: security_category         # SctyCtgy = "17"
    tag: InstrmInf/DrvsOptnExrcInf/SctyCtgy
    type: string
  - name: symbol                    # TckrSymb = "SJCK26P002350E"
    tag: InstrmInf/DrvsOptnExrcInf/TckrSymb
    type: string
  - name: isin                      # ISIN
    tag: InstrmInf/DrvsOptnExrcInf/ISIN
    type: string
  - name: option_delivery_type      # OptnDlvryTp = "0" | "1"
    tag: InstrmInf/DrvsOptnExrcInf/OptnDlvryTp
    type: string
  - name: settlement_multiplier     # SttlmIndMltplr = "1" | "1000"
    tag: InstrmInf/DrvsOptnExrcInf/SttlmIndMltplr
    type: numeric
  - name: derivative_exercise_security_id     # DerivOptnExrcInstrmId/OthrId/Id
    tag: InstrmInf/DrvsOptnExrcInf/DerivOptnExrcInstrmId/OthrId/Id
    type: string
  - name: derivative_exercise_security_proprietary
    tag: InstrmInf/DrvsOptnExrcInf/DerivOptnExrcInstrmId/OthrId/Tp/Prtry
    type: string
  - name: derivative_exercise_security_market
    tag: InstrmInf/DrvsOptnExrcInf/DerivOptnExrcInstrmId/PlcOfListg/MktIdrCd
    type: string
  - name: asset_settlement_security_id  # AsstSttlmInd/OthrId/Id
    tag: InstrmInf/DrvsOptnExrcInf/AsstSttlmInd/OthrId/Id
    type: string
  - name: asset_settlement_security_proprietary
    tag: InstrmInf/DrvsOptnExrcInf/AsstSttlmInd/OthrId/Tp/Prtry
    type: string
  - name: asset_settlement_security_market
    tag: InstrmInf/DrvsOptnExrcInf/AsstSttlmInd/PlcOfListg/MktIdrCd
    type: string
  - name: withdrawal_days
    tag: InstrmInf/DrvsOptnExrcInf/WdrwlDays
    type: integer
  - name: working_days
    tag: InstrmInf/DrvsOptnExrcInf/WrkgDays
    type: integer
  - name: calendar_days
    tag: InstrmInf/DrvsOptnExrcInf/ClnrDays
    type: integer
```

---

### 7. `strategies` (StrtgyInf) — 1,061 records
Strategy instruments (spreads, calendar spreads, EDS, TAS).

**XML Sample:**
```xml
<StrtgyInf>
  <SctyCtgy>93</SctyCtgy>
  <XprtnDt>2040-08-15</XprtnDt>
  <TckrSymb>DAIQ40Q50</TckrSymb>
  <XprtnCd>Q0Q0</XprtnCd>
  <TradgStartDt>2025-03-05</TradgStartDt>
  <TradgEndDt>2040-08-14</TradgEndDt>
  <ValTpCd>0</ValTpCd>
  <ISIN>BRBMEFDAI0X8</ISIN>
  <CFICd>KFXXXX</CFICd>
  <AllcnRndLot>5</AllcnRndLot>
  <TradgCcy>BRL</TradgCcy>
  <PrtlGvUpAllwncInd>false</PrtlGvUpAllwncInd>
  <StrtgyLegList>                        <!-- REPEATED element (leg 1) -->
    <LegId>1</LegId>
    <SdTpCd>BUYI</SdTpCd>
    <UndrlygInstrmId>
      <OthrId>
        <Id>100000139430</Id>
        <Tp><Prtry>8</Prtry></Tp>
      </OthrId>
      <PlcOfListg><MktIdrCd>BVMF</MktIdrCd></PlcOfListg>
    </UndrlygInstrmId>
  </StrtgyLegList>
  <StrtgyLegList>                        <!-- REPEATED element (leg 2) -->
    <LegId>2</LegId>
    <SdTpCd>SELL</SdTpCd>
    <UndrlygInstrmId>
      <OthrId>
        <Id>100000139428</Id>
        <Tp><Prtry>8</Prtry></Tp>
      </OthrId>
      <PlcOfListg><MktIdrCd>BVMF</MktIdrCd></PlcOfListg>
    </UndrlygInstrmId>
  </StrtgyLegList>
</StrtgyInf>
```

> **Limitation:** `StrtgyLegList` is a repeated element (multiple legs per strategy). The reader's `_smart_find` will extract only the **first** leg's values. Full leg extraction would require a code change to support list-type fields.

**Schema:**
```yaml
tag: StrtgyInf
fields:
  # ... common header fields ...
  - name: security_category         # SctyCtgy
    tag: InstrmInf/StrtgyInf/SctyCtgy
    type: string
  - name: maturity_date             # XprtnDt
    tag: InstrmInf/StrtgyInf/XprtnDt
    type: date
  - name: symbol                    # TckrSymb
    tag: InstrmInf/StrtgyInf/TckrSymb
    type: string
  - name: expiration_code           # XprtnCd
    tag: InstrmInf/StrtgyInf/XprtnCd
    type: string
  - name: trading_start_date
    tag: InstrmInf/StrtgyInf/TradgStartDt
    type: date
  - name: trading_end_date
    tag: InstrmInf/StrtgyInf/TradgEndDt
    type: date
  - name: value_type_code           # ValTpCd
    tag: InstrmInf/StrtgyInf/ValTpCd
    type: string
  - name: isin
    tag: InstrmInf/StrtgyInf/ISIN
    type: string
  - name: cfi_code
    tag: InstrmInf/StrtgyInf/CFICd
    type: string
  - name: allocation_lot_size
    tag: InstrmInf/StrtgyInf/AllcnRndLot
    type: integer
  - name: trading_currency
    tag: InstrmInf/StrtgyInf/TradgCcy
    type: string
  - name: partial_give_up_indicator # PrtlGvUpAllwncInd
    tag: InstrmInf/StrtgyInf/PrtlGvUpAllwncInd
    type: string
  - name: opening_future_position_day # OpngFutrPosDay
    tag: InstrmInf/StrtgyInf/OpngFutrPosDay
    type: string
  - name: rollover_base_price_code  # RlvrBasePricCd
    tag: InstrmInf/StrtgyInf/RlvrBasePricCd
    type: string
  - name: settlement_multiplier     # SttlmIndMltplr
    tag: InstrmInf/StrtgyInf/SttlmIndMltplr
    type: numeric
  - name: asset_settlement_security_id
    tag: InstrmInf/StrtgyInf/AsstSttlmInd/OthrId/Id
    type: string
  - name: asset_settlement_security_proprietary
    tag: InstrmInf/StrtgyInf/AsstSttlmInd/OthrId/Tp/Prtry
    type: string
  - name: asset_settlement_security_market
    tag: InstrmInf/StrtgyInf/AsstSttlmInd/PlcOfListg/MktIdrCd
    type: string
  # First strategy leg only (limitation of _smart_find)
  - name: leg1_id                   # StrtgyLegList/LegId (first)
    tag: InstrmInf/StrtgyInf/StrtgyLegList/LegId
    type: string
  - name: leg1_side                 # StrtgyLegList/SdTpCd (first)
    tag: InstrmInf/StrtgyInf/StrtgyLegList/SdTpCd
    type: string
  - name: leg1_underlying_security_id
    tag: InstrmInf/StrtgyInf/StrtgyLegList/UndrlygInstrmId/OthrId/Id
    type: string
  - name: leg1_underlying_security_proprietary
    tag: InstrmInf/StrtgyInf/StrtgyLegList/UndrlygInstrmId/OthrId/Tp/Prtry
    type: string
  - name: leg1_underlying_security_market
    tag: InstrmInf/StrtgyInf/StrtgyLegList/UndrlygInstrmId/PlcOfListg/MktIdrCd
    type: string
```

---

### 8. `equity_forwards` (EqtyFwdInf) — 677 records
Forward contracts on equities (termo).

**XML Sample:**
```xml
<EqtyFwdInf>
  <SctyCtgy>2</SctyCtgy>
  <TckrSymb>MOVI3T</TckrSymb>
  <ISIN>BRMOVITNO002</ISIN>
  <DstrbtnId>126</DstrbtnId>
  <CFICd>EMXXXR</CFICd>
  <PmtTp>0</PmtTp>
  <AllcnRndLot>1</AllcnRndLot>
  <PricFctr>1</PricFctr>
  <TradgStartDt>2017-02-08</TradgStartDt>
  <TradgEndDt>9999-12-31</TradgEndDt>
  <CtdyTrtmntTp>1</CtdyTrtmntTp>
  <TradgCcy>BRL</TradgCcy>
  <UndrlygInstrmId>
    <OthrId>
      <Id>200002489436</Id>
      <Tp><Prtry>8</Prtry></Tp>
    </OthrId>
    <PlcOfListg><MktIdrCd>BVMF</MktIdrCd></PlcOfListg>
  </UndrlygInstrmId>
</EqtyFwdInf>
```

**Schema:**
```yaml
tag: EqtyFwdInf
fields:
  # ... common header fields ...
  - name: security_category         # SctyCtgy = "2"
    tag: InstrmInf/EqtyFwdInf/SctyCtgy
    type: string
  - name: symbol                    # TckrSymb = "MOVI3T"
    tag: InstrmInf/EqtyFwdInf/TckrSymb
    type: string
  - name: isin
    tag: InstrmInf/EqtyFwdInf/ISIN
    type: string
  - name: distribution_id           # DstrbtnId
    tag: InstrmInf/EqtyFwdInf/DstrbtnId
    type: integer
  - name: cfi_code
    tag: InstrmInf/EqtyFwdInf/CFICd
    type: string
  - name: payment_type
    tag: InstrmInf/EqtyFwdInf/PmtTp
    type: string
  - name: allocation_lot_size
    tag: InstrmInf/EqtyFwdInf/AllcnRndLot
    type: integer
  - name: price_factor
    tag: InstrmInf/EqtyFwdInf/PricFctr
    type: numeric
  - name: trading_start_date
    tag: InstrmInf/EqtyFwdInf/TradgStartDt
    type: date
  - name: trading_end_date
    tag: InstrmInf/EqtyFwdInf/TradgEndDt
    type: date
  - name: custody_treatment_type
    tag: InstrmInf/EqtyFwdInf/CtdyTrtmntTp
    type: string
  - name: trading_currency
    tag: InstrmInf/EqtyFwdInf/TradgCcy
    type: string
  - name: underlying_security_id
    tag: InstrmInf/EqtyFwdInf/UndrlygInstrmId/OthrId/Id
    type: string
  - name: underlying_security_proprietary
    tag: InstrmInf/EqtyFwdInf/UndrlygInstrmId/OthrId/Tp/Prtry
    type: string
  - name: underlying_security_market
    tag: InstrmInf/EqtyFwdInf/UndrlygInstrmId/PlcOfListg/MktIdrCd
    type: string
```

---

### 9. `international_bonds` (IntlBdInf) — 627 records
International government bonds used as collateral.

**XML Sample:**
```xml
<IntlBdInf>
  <SctyCtgy>36</SctyCtgy>
  <ISIN>DE0001102408</ISIN>
  <CUSIP>LW7430652</CUSIP>
  <IssrCtry>276</IssrCtry>
  <Tp>GERMANY TREASURY BONDS</Tp>
  <IssePric>100.48000000</IssePric>
  <IsseDt>2016-07-15</IsseDt>
  <MtrtyDt>2026-08-15</MtrtyDt>
  <Ccy>EUR</Ccy>
</IntlBdInf>
```

**Schema:**
```yaml
tag: IntlBdInf
fields:
  # ... common header fields ...
  - name: security_category         # SctyCtgy = "36"
    tag: InstrmInf/IntlBdInf/SctyCtgy
    type: string
  - name: isin                      # ISIN = "DE0001102408"
    tag: InstrmInf/IntlBdInf/ISIN
    type: string
  - name: cusip                     # CUSIP = "LW7430652"
    tag: InstrmInf/IntlBdInf/CUSIP
    type: string
  - name: issuer_country            # IssrCtry = "276" (ISO numeric)
    tag: InstrmInf/IntlBdInf/IssrCtry
    type: string
  - name: bond_type                 # Tp = "GERMANY TREASURY BONDS"
    tag: InstrmInf/IntlBdInf/Tp
    type: string
  - name: issue_price               # IssePric
    tag: InstrmInf/IntlBdInf/IssePric
    type: numeric
  - name: issue_date                # IsseDt
    tag: InstrmInf/IntlBdInf/IsseDt
    type: date
  - name: maturity_date             # MtrtyDt
    tag: InstrmInf/IntlBdInf/MtrtyDt
    type: date
  - name: currency                  # Ccy = "EUR"
    tag: InstrmInf/IntlBdInf/Ccy
    type: string
```

---

### 10. `fixed_income` (FxdIncmInf) — 376 records
Tradable fixed income instruments (ETFs, debentures on the secondary market).

**XML Sample:**
```xml
<FxdIncmInf>
  <SctyCtgy>71</SctyCtgy>
  <ISIN>BRLFTSCTF006</ISIN>
  <TckrSymb>LFTS11</TckrSymb>
  <TradgStartDt>2022-11-08</TradgStartDt>
  <TradgEndDt>9999-12-31</TradgEndDt>
  <TradgCcy>BRL</TradgCcy>
  <PmtTp>0</PmtTp>
  <DaysToSttlm>1</DaysToSttlm>
  <AllcnRndLot>1</AllcnRndLot>
  <PricFctr>1</PricFctr>
  <UndrlygInstrmId>
    <OthrId>
      <Id>200000977475</Id>
      <Tp><Prtry>8</Prtry></Tp>
    </OthrId>
    <PlcOfListg><MktIdrCd>BVMF</MktIdrCd></PlcOfListg>
  </UndrlygInstrmId>
</FxdIncmInf>
```

**Schema:**
```yaml
tag: FxdIncmInf
fields:
  # ... common header fields ...
  - name: security_category
    tag: InstrmInf/FxdIncmInf/SctyCtgy
    type: string
  - name: isin
    tag: InstrmInf/FxdIncmInf/ISIN
    type: string
  - name: symbol
    tag: InstrmInf/FxdIncmInf/TckrSymb
    type: string
  - name: trading_start_date
    tag: InstrmInf/FxdIncmInf/TradgStartDt
    type: date
  - name: trading_end_date
    tag: InstrmInf/FxdIncmInf/TradgEndDt
    type: date
  - name: trading_currency
    tag: InstrmInf/FxdIncmInf/TradgCcy
    type: string
  - name: payment_type
    tag: InstrmInf/FxdIncmInf/PmtTp
    type: string
  - name: days_to_settlement
    tag: InstrmInf/FxdIncmInf/DaysToSttlm
    type: integer
  - name: allocation_lot_size
    tag: InstrmInf/FxdIncmInf/AllcnRndLot
    type: integer
  - name: price_factor
    tag: InstrmInf/FxdIncmInf/PricFctr
    type: numeric
  - name: underlying_security_id
    tag: InstrmInf/FxdIncmInf/UndrlygInstrmId/OthrId/Id
    type: string
  - name: underlying_security_proprietary
    tag: InstrmInf/FxdIncmInf/UndrlygInstrmId/OthrId/Tp/Prtry
    type: string
  - name: underlying_security_market
    tag: InstrmInf/FxdIncmInf/UndrlygInstrmId/PlcOfListg/MktIdrCd
    type: string
```

---

### 11. `national_bonds` (NtlBdInf) — 367 records
Brazilian federal government bonds (Tesouro Nacional).

**XML Sample:**
```xml
<NtlBdInf>
  <SctyCtgy>33</SctyCtgy>
  <ISIN>BRSTNCNI1951</ISIN>
  <SelicCd>891300</SelicCd>
  <BaseDt>2000-07-01</BaseDt>
  <MtrtyVal>0</MtrtyVal>
  <BaseDtPric>1</BaseDtPric>
  <IsseDt>2017-02-24</IsseDt>
  <MtrtyDt>2028-12-15</MtrtyDt>
  <BrzlnFdrlGovntBdTpCd>28</BrzlnFdrlGovntBdTpCd>
  <SctyLndgGovntBdInd>false</SctyLndgGovntBdInd>
  <GovntBdRepoSpcfcInd>false</GovntBdRepoSpcfcInd>
  <GovntBdRepoGnlInd>false</GovntBdRepoGnlInd>
</NtlBdInf>
```

**Schema:**
```yaml
tag: NtlBdInf
fields:
  # ... common header fields ...
  - name: security_category         # SctyCtgy = "33"
    tag: InstrmInf/NtlBdInf/SctyCtgy
    type: string
  - name: isin
    tag: InstrmInf/NtlBdInf/ISIN
    type: string
  - name: selic_code                # SelicCd = "891300"
    tag: InstrmInf/NtlBdInf/SelicCd
    type: string
  - name: base_date                 # BaseDt = "2000-07-01"
    tag: InstrmInf/NtlBdInf/BaseDt
    type: date
  - name: maturity_value            # MtrtyVal
    tag: InstrmInf/NtlBdInf/MtrtyVal
    type: numeric
  - name: base_date_price           # BaseDtPric
    tag: InstrmInf/NtlBdInf/BaseDtPric
    type: numeric
  - name: issue_date                # IsseDt
    tag: InstrmInf/NtlBdInf/IsseDt
    type: date
  - name: maturity_date             # MtrtyDt
    tag: InstrmInf/NtlBdInf/MtrtyDt
    type: date
  - name: bond_type_code            # BrzlnFdrlGovntBdTpCd
    tag: InstrmInf/NtlBdInf/BrzlnFdrlGovntBdTpCd
    type: string
  - name: security_lending_indicator # SctyLndgGovntBdInd
    tag: InstrmInf/NtlBdInf/SctyLndgGovntBdInd
    type: string
  - name: repo_specific_indicator   # GovntBdRepoSpcfcInd
    tag: InstrmInf/NtlBdInf/GovntBdRepoSpcfcInd
    type: string
  - name: repo_general_indicator    # GovntBdRepoGnlInd
    tag: InstrmInf/NtlBdInf/GovntBdRepoGnlInd
    type: string
```

---

### 12. `fixed_income_non_tradable` (FxdIncmNonTrdblInf) — 128 records
Non-tradable fixed income (underlying instruments for FI ETFs, debentures).

**XML Sample:**
```xml
<FxdIncmNonTrdblInf>
  <SctyCtgy>22</SctyCtgy>
  <ISIN>BRLFTSCTF006</ISIN>
  <DstrbtnId>100</DstrbtnId>
  <EXDstrbtnNb>0</EXDstrbtnNb>
  <CtdyTrtmntTp>0</CtdyTrtmntTp>
  <CFICd>CEOGBU</CFICd>
  <SpcfctnCd>F11</SpcfctnCd>
  <CrpnNm>INVESTO TEVA TESOURO SELIC ETF – FDO INV INDICE</CrpnNm>
  <AsstRegnDt>2022-10-25</AsstRegnDt>
  <IsseCd>13418</IsseCd>
  <SrsNb>1</SrsNb>
  <AsstCollTp>0</AsstCollTp>
  <AsstAddtlCollTp>0</AsstAddtlCollTp>
  <AsstSubrdntdTp>2</AsstSubrdntdTp>
  <DbnrConvtbltTp>0</DbnrConvtbltTp>
  <DbnrTaxBnft>
    <Artl1Ind>false</Artl1Ind>
    <Artl2Ind>false</Artl2Ind>
  </DbnrTaxBnft>
  <PerptlDbnrInd>false</PerptlDbnrInd>
  <IsseDt>2022-11-08</IsseDt>
  <BaseDt>2050-08-15</BaseDt>
  <IntrstRate>0.0000</IntrstRate>
  <IntrstRateCrrctnTp>2</IntrstRateCrrctnTp>
  <IntrstRateCrrctnTmBase>252</IntrstRateCrrctnTmBase>
  <EarlyRedInd>false</EarlyRedInd>
  <TtlSrsIsseVal>5000000.00000000</TtlSrsIsseVal>
  <UnitVal>100</UnitVal>
  <MktCptlstn>50000</MktCptlstn>
  <XprtnDt>2050-08-15</XprtnDt>
  <TradgStartDt>2022-11-08</TradgStartDt>
  <TradgEndDt>9999-12-31</TradgEndDt>
  <TckrSymb>LFTSETF11</TckrSymb>
  <RskRatg>C</RskRatg>
  <TradgCcy>BRL</TradgCcy>
  <FrstPric>100.0000000</FrstPric>
  <LastPric>148.9900000</LastPric>
</FxdIncmNonTrdblInf>
```

> Note: Some records also contain `AsstInd` (nested, with OthrId) and `IndxPctg`. These are optional fields present only in some instruments.

**Schema:**
```yaml
tag: FxdIncmNonTrdblInf
fields:
  # ... common header fields ...
  - name: security_category
    tag: InstrmInf/FxdIncmNonTrdblInf/SctyCtgy
    type: string
  - name: isin
    tag: InstrmInf/FxdIncmNonTrdblInf/ISIN
    type: string
  - name: distribution_id
    tag: InstrmInf/FxdIncmNonTrdblInf/DstrbtnId
    type: integer
  - name: ex_distribution_number
    tag: InstrmInf/FxdIncmNonTrdblInf/EXDstrbtnNb
    type: integer
  - name: custody_treatment_type
    tag: InstrmInf/FxdIncmNonTrdblInf/CtdyTrtmntTp
    type: string
  - name: cfi_code
    tag: InstrmInf/FxdIncmNonTrdblInf/CFICd
    type: string
  - name: specification_code
    tag: InstrmInf/FxdIncmNonTrdblInf/SpcfctnCd
    type: string
  - name: corporation_name
    tag: InstrmInf/FxdIncmNonTrdblInf/CrpnNm
    type: string
  - name: asset_registration_date
    tag: InstrmInf/FxdIncmNonTrdblInf/AsstRegnDt
    type: date
  - name: issue_code
    tag: InstrmInf/FxdIncmNonTrdblInf/IsseCd
    type: string
  - name: series_number
    tag: InstrmInf/FxdIncmNonTrdblInf/SrsNb
    type: string
  - name: asset_collateral_type
    tag: InstrmInf/FxdIncmNonTrdblInf/AsstCollTp
    type: string
  - name: asset_additional_collateral_type
    tag: InstrmInf/FxdIncmNonTrdblInf/AsstAddtlCollTp
    type: string
  - name: asset_subordinated_type
    tag: InstrmInf/FxdIncmNonTrdblInf/AsstSubrdntdTp
    type: string
  - name: debenture_convertibility_type
    tag: InstrmInf/FxdIncmNonTrdblInf/DbnrConvtbltTp
    type: string
  - name: debenture_tax_benefit_article1
    tag: InstrmInf/FxdIncmNonTrdblInf/DbnrTaxBnft/Artl1Ind
    type: string
  - name: debenture_tax_benefit_article2
    tag: InstrmInf/FxdIncmNonTrdblInf/DbnrTaxBnft/Artl2Ind
    type: string
  - name: perpetual_debenture_indicator
    tag: InstrmInf/FxdIncmNonTrdblInf/PerptlDbnrInd
    type: string
  - name: issue_date
    tag: InstrmInf/FxdIncmNonTrdblInf/IsseDt
    type: date
  - name: base_date
    tag: InstrmInf/FxdIncmNonTrdblInf/BaseDt
    type: date
  - name: interest_rate
    tag: InstrmInf/FxdIncmNonTrdblInf/IntrstRate
    type: numeric
  - name: interest_rate_correction_type
    tag: InstrmInf/FxdIncmNonTrdblInf/IntrstRateCrrctnTp
    type: string
  - name: interest_rate_correction_time_base
    tag: InstrmInf/FxdIncmNonTrdblInf/IntrstRateCrrctnTmBase
    type: string
  - name: index_percentage          # IndxPctg (only present in some records)
    tag: InstrmInf/FxdIncmNonTrdblInf/IndxPctg
    type: numeric
  - name: early_redemption_indicator
    tag: InstrmInf/FxdIncmNonTrdblInf/EarlyRedInd
    type: string
  - name: total_series_issue_value
    tag: InstrmInf/FxdIncmNonTrdblInf/TtlSrsIsseVal
    type: numeric
  - name: unit_value
    tag: InstrmInf/FxdIncmNonTrdblInf/UnitVal
    type: numeric
  - name: market_capitalisation
    tag: InstrmInf/FxdIncmNonTrdblInf/MktCptlstn
    type: numeric
  - name: maturity_date
    tag: InstrmInf/FxdIncmNonTrdblInf/XprtnDt
    type: date
  - name: trading_start_date
    tag: InstrmInf/FxdIncmNonTrdblInf/TradgStartDt
    type: date
  - name: trading_end_date
    tag: InstrmInf/FxdIncmNonTrdblInf/TradgEndDt
    type: date
  - name: symbol
    tag: InstrmInf/FxdIncmNonTrdblInf/TckrSymb
    type: string
  - name: risk_rating
    tag: InstrmInf/FxdIncmNonTrdblInf/RskRtg
    type: string
  - name: trading_currency
    tag: InstrmInf/FxdIncmNonTrdblInf/TradgCcy
    type: string
  - name: open
    tag: InstrmInf/FxdIncmNonTrdblInf/FrstPric
    type: numeric
  - name: close
    tag: InstrmInf/FxdIncmNonTrdblInf/LastPric
    type: numeric
  - name: asset_indicator_security_id  # AsstInd/OthrId/Id (only in some)
    tag: InstrmInf/FxdIncmNonTrdblInf/AsstInd/OthrId/Id
    type: string
  - name: asset_indicator_security_proprietary
    tag: InstrmInf/FxdIncmNonTrdblInf/AsstInd/OthrId/Tp/Prtry
    type: string
  - name: asset_indicator_security_market
    tag: InstrmInf/FxdIncmNonTrdblInf/AsstInd/PlcOfListg/MktIdrCd
    type: string
```

---

### 13. `adrs` (ADRInf) — 31 records
American Depositary Receipts.

**XML Sample:**
```xml
<ADRInf>
  <SctyCtgy>39</SctyCtgy>
  <TckrSymb>GFSA3 BZ</TckrSymb>
  <ISIN>US3626074005</ISIN>
  <CFICd>MMXXXX</CFICd>
  <CUSIP>362607400</CUSIP>
  <PrgmLvl>3</PrgmLvl>
  <Ppsn>2</Ppsn>
  <TradgCcy>USD</TradgCcy>
</ADRInf>
```

**Schema:**
```yaml
tag: ADRInf
fields:
  # ... common header fields ...
  - name: security_category         # SctyCtgy = "39"
    tag: InstrmInf/ADRInf/SctyCtgy
    type: string
  - name: symbol                    # TckrSymb = "GFSA3 BZ"
    tag: InstrmInf/ADRInf/TckrSymb
    type: string
  - name: isin                      # ISIN = "US3626074005"
    tag: InstrmInf/ADRInf/ISIN
    type: string
  - name: cfi_code                  # CFICd = "MMXXXX"
    tag: InstrmInf/ADRInf/CFICd
    type: string
  - name: cusip                     # CUSIP = "362607400"
    tag: InstrmInf/ADRInf/CUSIP
    type: string
  - name: program_level             # PrgmLvl = "3"
    tag: InstrmInf/ADRInf/PrgmLvl
    type: string
  - name: proportion                # Ppsn = "2"
    tag: InstrmInf/ADRInf/Ppsn
    type: string
  - name: trading_currency          # TradgCcy = "USD"
    tag: InstrmInf/ADRInf/TradgCcy
    type: string
```

---

### 14. `securities_lending` (BTCInf) — 7 records
Securities lending (BTC) instruments.

**XML Sample:**
```xml
<BTCInf>
  <SctyCtgy>54</SctyCtgy>
  <TckrSymb>OTCSECLEND</TckrSymb>
  <FngbInd>true</FngbInd>
  <PmtTp>0</PmtTp>
</BTCInf>
```

**Schema:**
```yaml
tag: BTCInf
fields:
  # ... common header fields ...
  - name: security_category         # SctyCtgy = "54"
    tag: InstrmInf/BTCInf/SctyCtgy
    type: string
  - name: symbol                    # TckrSymb = "OTCSECLEND"
    tag: InstrmInf/BTCInf/TckrSymb
    type: string
  - name: fungibility_indicator     # FngbInd = "true"
    tag: InstrmInf/BTCInf/FngbInd
    type: string
  - name: payment_type              # PmtTp = "0"
    tag: InstrmInf/BTCInf/PmtTp
    type: string
```

---

### 15. `otc_derivatives` (OTCInf) — 6 records
OTC derivatives (swaps, flexible options).

**XML Sample:**
```xml
<OTCInf>
  <CtrctTp>1</CtrctTp>
  <TradOrgnCd>2</TradOrgnCd>
  <FngbInd>false</FngbInd>
</OTCInf>
```

**Schema:**
```yaml
tag: OTCInf
fields:
  # ... common header fields ...
  - name: contract_type             # CtrctTp (0=swap, 1=flex call, etc.)
    tag: InstrmInf/OTCInf/CtrctTp
    type: string
  - name: trade_origin_code         # TradOrgnCd
    tag: InstrmInf/OTCInf/TradOrgnCd
    type: string
  - name: fungibility_indicator     # FngbInd
    tag: InstrmInf/OTCInf/FngbInd
    type: string
```

---

### 16. `cash` (CshInf) — 3 records
Cash/collateral instruments.

**XML Sample:**
```xml
<CshInf>
  <SctyCtgy>37</SctyCtgy>
  <CFICd>MMXXXX</CFICd>
  <CcyCd>BRL</CcyCd>
</CshInf>
```

**Schema:**
```yaml
tag: CshInf
fields:
  # ... common header fields ...
  - name: security_category         # SctyCtgy = "37"
    tag: InstrmInf/CshInf/SctyCtgy
    type: string
  - name: cfi_code                  # CFICd = "MMXXXX"
    tag: InstrmInf/CshInf/CFICd
    type: string
  - name: currency_code             # CcyCd = "BRL" | "EUR" | "USD"
    tag: InstrmInf/CshInf/CcyCd
    type: string
```

---

### 17. `investment_funds` (FICInf) — 2 records
Investment fund of funds (FIC).

**XML Sample:**
```xml
<FICInf>
  <SctyCtgy>35</SctyCtgy>
  <FndNm>FIC BBVMBOVESPA</FndNm>
  <Ccy>BRL</Ccy>
</FICInf>
```

**Schema:**
```yaml
tag: FICInf
fields:
  # ... common header fields ...
  - name: security_category         # SctyCtgy = "35"
    tag: InstrmInf/FICInf/SctyCtgy
    type: string
  - name: fund_name                 # FndNm = "FIC BBVMBOVESPA"
    tag: InstrmInf/FICInf/FndNm
    type: string
  - name: currency                  # Ccy = "BRL"
    tag: InstrmInf/FICInf/Ccy
    type: string
```

---

## Implementation

### Files to modify
- `templates/b3/raw/b3-bvbg028.yaml` — add 14 new dataset blocks (YAML only)

### No code changes needed
The reader (`B3ReadBVBG028XmlStep`) already handles arbitrary datasets generically via the `tag` → dataset mapping.

### Known limitation
`StrtgyInf` has repeated `StrtgyLegList` elements. The `_smart_find` method returns only the first match, so we capture `leg1_*` fields only. Full leg extraction would require a code enhancement (out of scope for this task).

## Verification

```bash
# 1. Process the existing file through the template
uv run python -c "
from brasa import process_marketdata
import datetime
process_marketdata('b3-bvbg028', refdate=datetime.date(2026, 3, 6))
"

# 2. Verify all 17 datasets were created with correct record counts
uv run python -c "
from brasa import get_marketdata
import datetime
rd = datetime.date(2026, 3, 6)
for ds in ['equities', 'options_on_equities', 'future_contracts',
           'exercise_of_equities', 'options_on_spot_and_futures',
           'derivatives_option_exercise', 'strategies', 'equity_forwards',
           'international_bonds', 'fixed_income', 'national_bonds',
           'fixed_income_non_tradable', 'adrs', 'securities_lending',
           'otc_derivatives', 'cash', 'investment_funds']:
    df = get_marketdata(f'b3-bvbg028-{ds}', refdate=rd)
    print(f'{ds}: {len(df)} rows, {len(df.columns)} cols')
"

# 3. Run existing tests
uv run pytest tests/ -v

# 4. Lint
uv run ruff check . && uv run ruff format --check .
```
