# Data Validations (`brasa doctor --category validations`)

`brasa doctor` can validate the **content** of stored datasets against a
declarative spec file — currently the `calendar-completeness` rule, which
checks that each configured series has a row for every business day of a
calendar across its observed range.

Validations are **spec-driven**: you pass the spec explicitly with
`--validations-file`. There is no bundled default.

## Usage

```bash
# Run only the validations category against your spec
brasa doctor --category validations --validations-file my-validations.yaml

# Run every category; validations uses the given spec
brasa doctor --validations-file my-validations.yaml
```

### Behavior when no file is given

| Invocation | Result |
| --- | --- |
| `doctor --category validations` (no file) | error, exit 2 |
| `doctor` (all categories, no file) | validations **skipped** with a note; other categories run; exit unaffected |
| any run with `--validations-file` | validations run against that file |

> **Behavior change:** bare `brasa doctor` no longer runs validations by
> default. Earlier versions shipped a packaged `validations.yaml`; that file is
> now an example only (see below), and validations run solely against an
> explicit `--validations-file`.

### Exit codes

| Situation | Exit |
| --- | --- |
| `--category validations`, no file | 2 (usage error) |
| bad / missing / empty `--validations-file` | 1 (config-error finding) |
| bare `doctor`, no file | 0 (validations skipped) |
| valid file, gaps found | 1 |
| valid file, clean | 0 |

## Spec format

The file is a mapping keyed by `"layer.dataset"`; each entry is a list of
rules. Each rule has a `rule:` discriminator.

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

A full, working example lives at
[`docs/examples/validations.yaml`](examples/validations.yaml).

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

## Roadmap

Additional rule types — monthly/quarterly frequency completeness, non-business-day
observation flags, and value/null/duplicate rules with auto-fix — are tracked
under WIL-85, WIL-92, and WIL-93.
