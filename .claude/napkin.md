# Napkin Runbook — brasa

## Curation Rules
- Re-prioritize on every read.
- Keep recurring, high-value notes only.
- Max 10 items per category.
- Each item includes date + "Do instead".

## Execution & Validation (Highest Priority)

1. **[2026-04-20] Always use `uv run` prefix for Python commands**
   Do instead: Never use bare `python`, `pytest`, `ruff`, or `mypy`. Always prefix with `uv run`. (See CLAUDE.md)

2. **[2026-04-20] Definition of Done is mandatory—never skip quality gates**
   Do instead: After any implementation, run all three: `uv run pytest`, `uv run ruff check . && uv run ruff format --check .`, `uv run pre-commit run --all-files`. No task is complete until all three pass.

3. **[2026-04-20] b3-bvbg028 dataset pattern: 9 common header fields + specific fields**
   Do instead: When adding a dataset block to `templates/b3/raw/b3-bvbg028.yaml`, always include: refdate, security_id, security_proprietary, security_market, instrument_asset, instrument_asset_description, instrument_market, instrument_segment, instrument_description. Then add dataset-specific fields.

4. **[2026-04-20] DatasetCase spot_check must match fixture data from 2021-04-23**
   Do instead: When appending a DatasetCase to `tests/test_bvbg028_datasets.py`, verify all spot_check values against `data/IN210423.zip`. Use security_id as the lookup key and validate 18–25 key fields.

5. **[2026-04-20] Linear MCP tools are primary—use `mcp__plugin_linear_linear__*` for all issue work**
   Do instead: Use Linear MCP functions (get_issue, save_issue, list_issues, save_comment) instead of gh CLI. Always pass plain text for markdown content—no escape sequences.

## Shell & Command Reliability

1. **[2026-04-20] Git commits require staged changes—use `git add` before `git commit`**
   Do instead: Always stage relevant files before commit. Use `git commit -m "$(cat <<'EOF'...EOF)"` for multi-line messages to preserve formatting.

2. **[2026-04-20] Avoid destructive git operations without explicit user request**
   Do instead: Use safe alternatives: cherry-pick instead of reset, revert instead of amend for published commits. Only reset/force-push if explicitly authorized.

## Domain Behavior Guardrails

1. **[2026-04-20] YAML field types: date(), numeric(), string, integer, character, category**
   Do instead: Use `type: date` for dates without format parameter, `type: date(format='%Y%m%d')` for yyyymmdd format. Use `type: numeric` for decimals; specify `decimal=","` or `thousands="."` if needed. See `brasa/fieldsets/` for type adapters.

2. **[2026-04-20] XML nested paths use forward slashes—handle optional and nested structures**
   Do instead: For nested XML like `<UndrlygInstrmId><OthrId><Id>`, use tag path `InstrmInf/DatasetName/UndrlygInstrmId/OthrId/Id`. Optional fields (may be null in some records) should still be declared in schema.

3. **[2026-04-20] Parquet files are partitioned by refdate in `.brasa-cache/db/`**
   Do instead: Query via PyArrow datasets or DuckDB views (`input.dataset-name`, `staging.dataset-name`). Always create views with `from brasa import create_all_views; create_all_views()` before querying.

## User Directives

1. **[2026-04-20] When planning b3-bvbg028 datasets, use linear-brasa-plan skill**
   Do instead: Invoke `/linear-brasa-plan WIL-X` to generate implementation plans directly to Linear issues. Do not create separate plan documents unless explicitly requested.

2. **[2026-04-20] b3-bvbg028 sub-issues follow a consistent template: Design → Implementation Plan → Execution**
   Do instead: Use the established pattern: fetch issue, extract Design section, invoke superpowers:writing-plans, save plan back to issue as ## Implementation Plan. Then use linear-brasa-executor to execute.
