# Data Validations (`brasa validate`)

`brasa validate` checks the **data** stored in your datalake against
declarative rules — e.g. "this dataset has a row on every B3 business day" or
"no duplicate `(refdate, symbol)` keys". It does not check template files
(those are checked by tests).

**Put a rule on the dataset that makes the promise.** A raw input such as
`input.b3-bvbg086` mirrors its source, gaps included, so it only gets
structural rules (`no-duplicates`, `not-null`). An ETL output that patches
known source gaps is where a completeness rule belongs.

## Where rules live

Rules come from three places, merged together (nothing overrides anything):

1. **Inline, in templates** — rules that are true for every user of the
   dataset. The dataset is implicit:

   ```yaml
   # single-dataset and ETL templates: top level
   id: b3-equities-returns
   ...
   validations:
     - rule: calendar-completeness   # a row on every B3 business day
       calendar: B3
     - rule: no-duplicates
       key: [refdate, symbol]
   ```

   ```yaml
   # multi-dataset templates: inside each datasets entry
   datasets:
     indexes_info:
       tag: IndxInf
       fields: [...]
       validations:
         - rule: not-null
           columns: [refdate, symbol]
   ```

2. **Rule files in template folders** — every `validations/*.yaml` in each
   [template folder](TEMPLATES.md#template-folders). Typical use: personal
   rules in your own workspace folder. Format: a mapping of `layer.dataset` to
   a list of rules:

   ```yaml
   # ~/my-workspace/validations/mine.yaml
   staging.b3-cotahist:
     - rule: calendar-completeness
       group_column: symbol
       series:
         BOVA11: { calendar: B3 }
         IVVB11: { calendar: B3 }
   ```

3. **`--rules FILE`** — an extra one-off file in the same format.

## Usage

```bash
brasa validate                                      # every dataset that has rules
brasa validate staging.b3-equities-returns          # one dataset
brasa validate staging.b3-cotahist input.b3-bvbg086 # several
brasa validate --rules my.yaml staging.b3-cotahist  # plus an extra rules file
```

What you see:

- `staging.x: 3 rule(s) checked` (info) for every dataset that was checked,
  followed by any rule failures (errors).
- `no rules for staging.x` (info) when you ask for a dataset without rules.
- `skipped: staging.x/USD — series 'USD' not found` or
  `skipped: staging.x — dataset not in the datalake` (info) when a rule finds
  nothing to check. Skips are shown so a typo in a series name is visible, but
  they never fail the run — a partial datalake passes.
- A rules-file key that no template produces is an **error**
  (`unknown dataset 'staging.x' (no template produces it)`).

### Exit codes

| Situation | Exit |
| --- | --- |
| no rule failures (skips and notes are fine) | 0 |
| rule failures, or invalid rule definitions | 1 |
| a dataset argument that no template produces, or `--rules` file not found | 2 |

> **Changed in 0.5:** validations used to run as `brasa doctor --category
> validations --validations-file FILE`. That category and flag are gone;
> use `brasa validate` (with `--rules FILE` for the old file).

## Rule format

A rule is a mapping with a `rule:` discriminator. Rule files map each
`"layer.dataset"` to a list of rules; an inline `validations:` block is the
same list without the key.

```yaml
staging.bcb-sgs:
  - rule: calendar-completeness
    date_column: refdate        # optional, default: refdate
    group_column: symbol        # column that separates series
    series:
      CDI: { calendar: ANBIMA }
      SELIC: { calendar: ANBIMA }
```

Per-series (grouped) or rule-level (single-series) optional keys:

- `calendar` — bizdays calendar name. Default `ANBIMA`.
- `start` — ISO date. Default: first observed date for the series.
- `end` — ISO date, or `today`. Default: last observed date.

Additionally, `calendar-completeness` supports a per-series `frequency`:

- `frequency` — `daily` (default), `monthly`, or `quarterly`.
  - `daily` checks business-day completeness against the bizdays `calendar`.
  - `monthly` / `quarterly` are **calendar-agnostic** period-existence checks:
    a period is "present" if it holds at least one observation. The `calendar`
    field is ignored. Missing periods are reported as `YYYY-MM` (monthly) or
    `YYYY-Qn` (quarterly).

Like `calendar`/`start`/`end`, `frequency` is placed per-series in the grouped
(`series:`) shape or rule-level in the single-series shape; there is no
rule-level → series inheritance.

### Rule: `no-unexpected-observations`

The inverse of `calendar-completeness`: it flags rows that fall on a
**non-business day** (weekend or holiday) of the configured `calendar`. Same
config shape as `calendar-completeness` (`date_column`, `group_column`,
`series`, per-series `calendar`). Daily-only: series with `frequency: monthly`
or `frequency: quarterly` are skipped. Findings are `error` severity and
read-only (no auto-fix). Dates outside the calendar's coverage are skipped.

```yaml
input.b3-bvbg086:
  - rule: no-unexpected-observations
    date_column: refdate
    group_column: symbol
    series:
      PETR4: { calendar: B3 }
```

### Content rules (dataset-wide)

These rules check columns across the whole dataset — no `series:` map. All are
`error` severity and read-only.

- `value-range` — a numeric `column` must lie within `[min, max]` (at least one
  bound required; inclusive; nulls skipped).
- `not-null` — every column in `columns` must have no null values.
- `no-duplicates` — no duplicate rows for the `key` column set.

```yaml
staging.b3-cotahist:
  - rule: value-range
    column: close
    min: 0
  - rule: not-null
    columns: [refdate, symbol, close]
  - rule: no-duplicates
    key: [refdate, symbol]
```
