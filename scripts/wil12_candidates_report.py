"""WIL-12 — visual report of every hard-stop -> sudden-start candidate pair.

Reads scripts/out/symbol_spans_analysis.csv (produced by
scripts/wil12_symbol_changes.py) and pairs EVERY hard-stop with EVERY
sudden-start on the next B3 business day — no ISIN-class, price, or suffix
filter. Writes a self-contained sortable/filterable HTML table to
scripts/out/symbol_changes_candidates.html for visual investigation.

Each pair carries a returns cross-check from staging.b3-equities-returns
(threaded through the spans CSV as `event_pct_return`): `dest_pct_return` is the
sudden-start's adjusted first-day return, `close_change_pct` is the raw
close-to-close change across the pair, and `ret_match` is True when they agree
within RET_MATCH_TOL. For a genuine rename the adjusted return equals the close
change, so `ret_match` is the strongest continuity signal available.

Verdict per pair (row color only; nothing is hidden):
- MATCHED: the pair is in scripts/out/symbol_changes.csv (strict rules)
- NEAR:    same ISIN class and price diff <= 25%, but not matched
- WEAK:    everything else (class mismatch or large price gap)

Usage:
    uv run python scripts/wil12_candidates_report.py
"""

import json
from pathlib import Path

import pandas as pd
from bizdays import Calendar

OUT_DIR = Path("scripts/out")
SPANS_CSV = OUT_DIR / "symbol_spans_analysis.csv"
CHANGES_CSV = OUT_DIR / "symbol_changes.csv"
REPORT_HTML = OUT_DIR / "symbol_changes_candidates.html"

NEAR_TOL = 0.25  # same-class pairs up to this price diff are flagged NEAR
RET_MATCH_TOL = 0.005  # |dest_pct_return - close_change_pct| to corroborate


def build_pairs() -> pd.DataFrame:
    spans = pd.read_csv(SPANS_CSV, parse_dates=["event_date"])
    cal = Calendar.load("B3")

    hs = spans[spans["event_type"] == "HARD_STOP"].copy()
    ss = spans[spans["event_type"] == "SUDDEN_START"].copy()
    hs["next_bd"] = [cal.offset(d.date(), 1) for d in hs["event_date"]]
    hs["next_bd"] = pd.to_datetime(hs["next_bd"])

    pairs = hs.merge(
        ss, left_on="next_bd", right_on="event_date", suffixes=("_src", "_dest")
    )
    pairs["price_diff_pct"] = (
        (pairs["close_dest"] - pairs["close_src"]).abs() / pairs["close_src"]
    ).where(pairs["close_src"] > 0)
    pairs["same_class"] = pairs["isin_class_src"] == pairs["isin_class_dest"]
    pairs["same_suffix"] = pairs["symbol_src"].str[4:] == pairs["symbol_dest"].str[4:]

    # share-class continuity (from the ISIN-derived ON/PN class): ON->ON and
    # PN->PN always continue, PN may be discontinued and reappear as a new ON
    # (PN->ON), but ON never becomes PN. Incompatible pairs cannot be a rename.
    sc_src, sc_dest = pairs["share_class_src"], pairs["share_class_dest"]
    pairs["same_share_class"] = sc_src == sc_dest
    pairs["class_ok"] = pairs["same_share_class"] | (
        (sc_src == "PN") & (sc_dest == "ON")
    )

    # adjusted-return cross-check (staging.b3-equities-returns): the dest's
    # first-day return should equal the signed close-to-close change.
    pairs["close_change_pct"] = (
        (pairs["close_dest"] - pairs["close_src"]) / pairs["close_src"]
    ).where(pairs["close_src"] > 0)
    pairs["dest_pct_return"] = pairs.get("event_pct_return_dest")
    ret_diff = (pairs["dest_pct_return"] - pairs["close_change_pct"]).abs()
    # no returns coverage (pre-2016 or absent) -> unknown (None), not a False
    pairs["ret_match"] = (
        (ret_diff <= RET_MATCH_TOL)
        .astype(object)
        .where(pairs["dest_pct_return"].notna(), None)
    )

    matched_pairs: set[tuple[str, str]] = set()
    if CHANGES_CSV.exists():
        changes = pd.read_csv(CHANGES_CSV)
        matched_pairs = set(
            zip(changes["src_symbol"], changes["dest_symbol"], strict=True)
        )

    def verdict(row) -> str:
        if (row["symbol_src"], row["symbol_dest"]) in matched_pairs:
            return "MATCHED"
        # adjusted returns confirm continuity even outside the strict rules
        if row["ret_match"] is True:
            return "CONFIRMED"
        # an incompatible share-class transition (e.g. ON->PN) can't be a rename
        if (
            row["class_ok"]
            and pd.notna(row["price_diff_pct"])
            and row["price_diff_pct"] <= NEAR_TOL
        ):
            return "NEAR"
        return "WEAK"

    pairs["verdict"] = pairs.apply(verdict, axis=1)
    pairs = pairs.sort_values(["event_date_dest", "symbol_src"])

    return pd.DataFrame(
        {
            "src_symbol": pairs["symbol_src"],
            "dest_symbol": pairs["symbol_dest"],
            "src_last_date": pairs["event_date_src"].dt.strftime("%Y-%m-%d"),
            "change_date": pairs["event_date_dest"].dt.strftime("%Y-%m-%d"),
            "src_close": pairs["close_src"].round(2),
            "dest_close": pairs["close_dest"].round(2),
            "price_diff_pct": (pairs["price_diff_pct"] * 100).round(1),
            "close_change_pct": (pairs["close_change_pct"] * 100).round(2),
            "dest_pct_return": (pairs["dest_pct_return"] * 100).round(2),
            "ret_match": pairs["ret_match"],
            "src_class": pairs["isin_class_src"],
            "dest_class": pairs["isin_class_dest"],
            "src_share_class": pairs["share_class_src"],
            "dest_share_class": pairs["share_class_dest"],
            "same_share_class": pairs["same_share_class"],
            "class_ok": pairs["class_ok"],
            "same_class": pairs["same_class"],
            "same_suffix": pairs["same_suffix"],
            "src_corp": pairs["corporation_name_src"],
            "dest_corp": pairs["corporation_name_dest"],
            "verdict": pairs["verdict"],
        }
    )


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>WIL-12 — symbol-change candidates</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 1.5rem; background: #fafafa; }
  h1 { font-size: 1.2rem; }
  .summary span { margin-right: 1.2rem; font-weight: 600; }
  .controls { margin: .8rem 0; display: flex; gap: 1rem; align-items: center; }
  input[type=text] { padding: .35rem .6rem; width: 240px; }
  table { border-collapse: collapse; width: 100%; font-size: .82rem; background: #fff; }
  th, td { border: 1px solid #ddd; padding: .25rem .5rem; text-align: left; white-space: nowrap; }
  th { background: #333; color: #fff; cursor: pointer; position: sticky; top: 0; user-select: none; }
  th .arrow { font-size: .7em; margin-left: .25em; }
  tr.MATCHED   { background: #e3f6e3; }
  tr.CONFIRMED { background: #cdeffd; }
  tr.NEAR      { background: #fff7d6; }
  tr.WEAK      { background: #f2f2f2; color: #777; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .legend span { padding: .15rem .55rem; margin-right: .6rem; border-radius: 4px; font-size: .8rem; }
  .lg-MATCHED { background: #e3f6e3; } .lg-CONFIRMED { background: #cdeffd; }
  .lg-NEAR { background: #fff7d6; } .lg-WEAK { background: #f2f2f2; }
</style>
</head>
<body>
<h1>WIL-12 — hard-stop &rarr; next-business-day sudden-start candidates</h1>
<div class="summary" id="summary"></div>
<div class="legend">
  <span class="lg-MATCHED">MATCHED — in strict-rule output</span>
  <span class="lg-CONFIRMED">CONFIRMED — adjusted return matches close change</span>
  <span class="lg-NEAR">NEAR — same class, &Delta; &le; 25%</span>
  <span class="lg-WEAK">WEAK — class mismatch or large gap</span>
</div>
<div class="controls">
  <input type="text" id="filter" placeholder="filter symbol / corp / date...">
  <label><input type="checkbox" id="hideMatched"> hide matched</label>
  <label><input type="checkbox" id="hideWeak"> hide weak</label>
  <label><input type="checkbox" id="hideDiffClass"> hide different share class</label>
  <span id="count"></span>
</div>
<table id="tbl"><thead></thead><tbody></tbody></table>
<script>
const COLUMNS = __COLUMNS__;
const NUMERIC = new Set(["src_close", "dest_close", "price_diff_pct", "close_change_pct", "dest_pct_return"]);
const DATA = __DATA__;
let sortCol = "change_date", sortAsc = true;

function render() {
  const q = document.getElementById("filter").value.toLowerCase();
  const hideM = document.getElementById("hideMatched").checked;
  const hideW = document.getElementById("hideWeak").checked;
  const hideDC = document.getElementById("hideDiffClass").checked;
  let rows = DATA.filter(r =>
    !(hideM && r.verdict === "MATCHED") &&
    !(hideW && r.verdict === "WEAK") &&
    !(hideDC && r.same_share_class === false) &&
    (!q || COLUMNS.some(c => String(r[c]).toLowerCase().includes(q))));
  rows.sort((a, b) => {
    let x = a[sortCol], y = b[sortCol];
    if (NUMERIC.has(sortCol)) { x = x === null ? -Infinity : x; y = y === null ? -Infinity : y; }
    const cmp = x < y ? -1 : x > y ? 1 : 0;
    return sortAsc ? cmp : -cmp;
  });
  const thead = document.querySelector("#tbl thead");
  thead.innerHTML = "<tr>" + COLUMNS.map(c =>
    `<th data-col="${c}">${c}<span class="arrow">${c === sortCol ? (sortAsc ? "\\u25b2" : "\\u25bc") : ""}</span></th>`
  ).join("") + "</tr>";
  thead.querySelectorAll("th").forEach(th => th.onclick = () => {
    const c = th.dataset.col;
    if (sortCol === c) sortAsc = !sortAsc; else { sortCol = c; sortAsc = true; }
    render();
  });
  document.querySelector("#tbl tbody").innerHTML = rows.map(r =>
    `<tr class="${r.verdict}">` + COLUMNS.map(c =>
      `<td class="${NUMERIC.has(c) ? "num" : ""}">${r[c] === null ? "" : r[c]}</td>`
    ).join("") + "</tr>"
  ).join("");
  document.getElementById("count").textContent = rows.length + " / " + DATA.length + " pairs";
  const n = v => DATA.filter(r => r.verdict === v).length;
  document.getElementById("summary").innerHTML =
    `<span>${DATA.length} candidate pairs</span>` +
    `<span>${n("MATCHED")} matched</span><span>${n("CONFIRMED")} confirmed</span>` +
    `<span>${n("NEAR")} near</span><span>${n("WEAK")} weak</span>`;
}
["filter", "hideMatched", "hideWeak", "hideDiffClass"].forEach(id =>
  document.getElementById(id).addEventListener("input", render));
render();
</script>
</body>
</html>
"""


def main() -> None:
    pairs = build_pairs()
    records = json.loads(pairs.to_json(orient="records"))
    html = HTML_TEMPLATE.replace("__COLUMNS__", json.dumps(list(pairs.columns)))
    html = html.replace("__DATA__", json.dumps(records))
    REPORT_HTML.write_text(html, encoding="utf-8")

    counts = pairs["verdict"].value_counts().to_dict()
    print(f"pairs: {len(pairs)}")
    print(f"verdicts: {counts}")
    print(f"report written to {REPORT_HTML}")


if __name__ == "__main__":
    main()
