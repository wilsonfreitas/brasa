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

A full, working example lives at
[`docs/examples/validations.yaml`](examples/validations.yaml).

## Roadmap

Additional rule types — monthly/quarterly frequency completeness, non-business-day
observation flags, and value/null/duplicate rules with auto-fix — are tracked
under WIL-85, WIL-92, and WIL-93.
