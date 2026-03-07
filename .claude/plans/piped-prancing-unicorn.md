# Plan: Compact CLI Summary Output

## Context

The current CLI output prints a wide `console.rule("[bold]SUMMARY[/bold]")` horizontal separator between the progress line and the summary text. This is visually noisy when multiple templates are executed in sequence. The goal is to remove the rule and blank lines, leaving only the compact summary text.

**Current output:**
```
Process b3-indexes-composition SSSSSSSSSSSS [50/81]
                               SSSSS....... [81/81] (0.1s)

─────────────────────────────── SUMMARY ───────────────────────────────

b3-indexes-composition process: 9 passed, 72 skipped in 0.1s

```

**Target output (multiple sequential runs):**
```
Process template1 SSSSSSSS..... [81/81] (0.1s)
template1 process: 9 passed, 72 skipped in 0.1s
Process template2 SSSSSSSS..... [81/81] (0.1s)
template2 process: 10 passed, 71 skipped in 0.1s
```

## Change

**File:** `brasa/engine/reporting.py`, method `TaskReport._print_summary()` (lines 603–654)

Remove the blank line and `console.rule` before the summary text, and remove the trailing blank line after it.

**From:**
```python
self.console.print()
self.console.rule("[bold]SUMMARY[/bold]")

# ... build parts and time_str ...

self.console.print(summary)
self.console.print()
```

**To:**
```python
# ... build parts and time_str (unchanged) ...

self.console.print(summary)
```

That's the entire change — 3 lines removed, nothing else altered.

## Verification

```bash
uv run python -m brasa.cli run b3-indexes-composition-consolidated
uv run pytest --no-integration
uv run ruff check . && uv run ruff format --check .
```
