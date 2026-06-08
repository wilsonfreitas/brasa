# Design: `brasa run-all`

**Date:** 2026-06-08
**Status:** Approved (design)

## Summary

A new CLI command, `brasa run-all`, that brings the **entire pipeline** to a
fresh state in a **single topological pass**. It is the global sibling of
`brasa run <template>`: where `run` resolves and executes one template's
ancestor chain, `run-all` sweeps every template in the dependency graph and
executes whatever is stale, leaving the whole pipeline up to date.

It pairs with `brasa map`: `map` *shows* what is stale, `run-all` *fixes* it.

## Scope

- **Process + ETL only.** `run-all` never downloads new raw data (downloads
  require refdates and are an explicit, separate step). "Stale" means exactly
  what `brasa map` / `brasa run` already mean:
  - a **download** template with unprocessed (downloaded-but-not-parsed) files, or
  - an **ETL** template whose output is missing or older than its inputs.
- No `--template` scoping: `brasa run <template>` already covers running a single
  template with dependency resolution.

## Behavior & Algorithm

Walk every template **sources-first** (global topological order). For each node,
re-check staleness **live** — after its upstreams have already run this pass — and
execute it if needed. Because upstreams always run before downstreams, a single
pass converges everything; no iterative re-scan loop is required.

```
graph = TemplateDependencyGraph()
nodes = graph.global_topological_order()      # all templates, sources first

failed  = set()    # raised an exception during execution
blocked = set()    # cannot run (e.g. download template with no downloaded data)

for tid in nodes:
    upstreams = graph.get_upstream(tid)
    if any(u in failed or u in blocked for u in upstreams):
        record SKIPPED(tid, reason="upstream <u> failed/blocked")
        # tid is now effectively unsatisfied for its own descendants:
        blocked.add(tid)
        continue

    action, reason = staleness_check(tid)      # live re-check
    if action == "skip":
        continue
    if action == "blocked":                    # e.g. download never-run, no data
        record BLOCKED(tid, reason); blocked.add(tid); continue

    report = orchestrator.execute_step(tid, action, verbosity)
    record EXECUTED(tid, report)
    if not report.success:
        failed.add(tid)
```

`staleness_check(tid)` reuses the **exact predicates** `brasa run` already uses:

- **download:** `_check_download_template_staleness` →
  - unprocessed files present → `("process", "unprocessed downloads detected")`
  - `never-run` (no downloaded data) → `("blocked", "no downloaded data")`
  - otherwise → `("skip", "all downloads already processed")`
- **etl:** `_check_etl_template_staleness` →
  - output missing/outdated → `("etl", "output missing or outdated")`
  - otherwise → `("skip", "output is up to date")`

This keeps `run-all` consistent with `map`, `run`, and `plan`.

### Failure handling

Per design decision: when a template **fails** (or is **blocked**), its
**descendants are skipped** (their inputs are now broken/stale/absent), but
**independent branches keep running**. All failures and blocks are reported at the
end.

### `--dry-run`

`--dry-run` cannot execute, so it predicts the run via **forward-closure**: the
execution set = currently-stale templates ∪ all of their descendants, printed in
topological order with reasons (`stale` vs `downstream of <X>`). Download
templates that are `never-run` (no data) are shown as blocked, and their
descendants as skipped, matching real-run behavior.

## Code Changes (reuse-first)

### `engine/dependency_graph.py`
- Extract `pipeline_map._global_topological_order` into a public method
  `global_topological_order() -> list[str]` on `TemplateDependencyGraph`.
- Refactor `pipeline_map.py` to call the new method (no duplicated topo logic).

### `engine/orchestrator.py`
- Add `execute_all(dry_run: bool = False, verbosity: Verbosity = ...) -> RunAllReport`,
  reusing the existing `_execute_step`.
- Add a `RunAllReport` dataclass mirroring `OrchestratorReport`:
  - per-template lines with status (executed / skipped / failed / blocked) and reason
  - counts, `.success` property, `.summary()` method, `.dry_run` flag.

### `cli.py`
- Add the `run-all` subparser:
  - `--dry-run` — print the predicted execution plan without executing.
  - `-v` / `-vv` verbosity via `add_verbosity_args` (matching `run`).
- Handler: call `orchestrator.execute_all(...)`, print `report.summary()`,
  exit non-zero on any failure.

## Exit Codes & Edge Cases

- Nothing stale → `"Everything is up to date."`, **exit 0**.
- A template raises during execution → **failure**; its descendants skipped;
  **exit 1**.
- A download template with no downloaded data (`never-run`) → **blocked**
  (not a failure); its descendants skipped with a clear
  `"upstream has no downloaded data"` note; **exit 0** with a warning.
  (Downloading is out of scope.)

## Testing

Mirror the existing suites:
- **Unit:** `global_topological_order` ordering; `execute_all` ordering and
  skip-propagation (failed/blocked → descendants skipped) on a small synthetic
  graph — no network.
- **CLI:** `tests/test_cli_run_all.py` for `--dry-run` output and exit codes,
  following `tests/test_pipeline_map.py` / `tests/test_cli_map.py`.

## Definition of Done

`uv run pytest`, `uv run ruff check . && uv run ruff format --check .`, and
`uv run pre-commit run --all-files` all pass.
