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

3. **[2026-04-20] Linear MCP tools are primary—use `mcp__plugin_linear_linear__*` for all issue work**
   Do instead: Use Linear MCP functions (get_issue, save_issue, list_issues, save_comment) instead of gh CLI. Always pass plain text for markdown content—no escape sequences. If only `authenticate`/`complete_authentication` show up (2026-10-31), call `authenticate`, show the user the URL right away, and explore code while waiting.

4. **[2026-04-20] b3-bvbg028 dataset pattern: 9 common header fields + specific fields**
   Do instead: When adding a dataset block to `templates/b3/raw/b3-bvbg028.yaml`, always include: refdate, security_id, security_proprietary, security_market, instrument_asset, instrument_asset_description, instrument_market, instrument_segment, instrument_description. Then add dataset-specific fields.

5. **[2026-04-20] DatasetCase spot_check must match fixture data from 2021-04-23**
   Do instead: When appending a DatasetCase to `tests/test_bvbg028_datasets.py`, verify all spot_check values against `data/IN210423.zip`. Use security_id as the lookup key and validate 18–25 key fields.

## Shell & Command Reliability

1. **[2026-04-20] Git commits require staged changes—use `git add` before `git commit`**
   Do instead: Always stage relevant files before commit. Use `git commit -m "$(cat <<'EOF'...EOF)"` for multi-line messages to preserve formatting.

2. **[2026-04-20] Avoid destructive git operations without explicit user request**
   Do instead: Use safe alternatives: cherry-pick instead of reset, revert instead of amend for published commits. Only reset/force-push if explicitly authorized.

3. **[2026-10-31] Linear docs: auto-links and silent no-op patches**
   Do instead: Keep domain-like file names (`steps.py`, `x.md`) out of Linear document headings — they become links even in backticks. A `save_document` patch whose anchor touches auto-linked text or an issue mention (`WIL-123`) can no-op silently: check `updatedAt` changed, or resend the full content.

4. **[2026-10-31] zsh does not word-split unquoted variables**
   Do instead: Don't pass a file list as `$F` to ruff/pytest (it becomes ONE argument); list paths inline or use an array (`files=(a b); cmd $files`).

## Domain Behavior Guardrails

1. **[2026-04-20] YAML field types: date(), numeric(), string, integer, character, category**
   Do instead: Use `type: date` for dates without format parameter, `type: date(format='%Y%m%d')` for yyyymmdd format. Use `type: numeric` for decimals; specify `decimal=","` or `thousands="."` if needed. See `brasa/fieldsets/` for type adapters.

2. **[2026-06-12] `legacy/` folders are excluded from template discovery**
   Do instead: To retire a template, `git mv` it into `templates/legacy/` — `_discover_templates()` in `brasa/engine/template.py` skips any path with a `legacy` part (hatch also drops `**/legacy/**` from the wheel). Check test/template references to the id first (e.g., `tests/test_pipeline.py` uses real templates as loading examples).

3. **[2026-07-17] Template discovery is multi-root (WIL-84)**
   Do instead: `BRASA_TEMPLATE_PATH` roots are searched first, bundled `brasa/files/templates` last; first root wins on a name clash (`brasa list-templates` shows source + shadows). Use it for user/extra templates instead of copying into the package.

4. **[2026-04-20] XML nested paths use forward slashes—handle optional and nested structures**
   Do instead: For nested XML like `<UndrlygInstrmId><OthrId><Id>`, use tag path `InstrmInf/DatasetName/UndrlygInstrmId/OthrId/Id`. Optional fields (may be null in some records) should still be declared in schema.

5. **[2026-04-20] Parquet files are partitioned by refdate in `<data_path>/db/`**
   Do instead: Query via PyArrow datasets or DuckDB views (`input.dataset-name`, `staging.dataset-name`). Always create views with `from brasa import create_all_views; create_all_views()` before querying.

6. **[2026-10-31] CLI dates use `--arg refdate=@YYYY-MM-DD`**
   Do instead: There is no `-d/--date` flag on `brasa download` (DOWNLOAD_PLAN.md still shows one — stale). Use `--arg refdate=@2026-10-30`, ranges `@2026-01-01:2026-03-31`, calendars `@2026-01~ANBIMA`.

## User Directives

1. **[2026-04-20] Linear issue flow: Design Document → Implementation Plan Document → Execution**
   Do instead: Use `linear-brasa-brainstorm` (Design Document, → Refined), `linear-brasa-plan` (Implementation Plan Document, → Planned), `linear-brasa-executor` (branch per issue, merge to main). Plans/designs live in issue Documents, not in description sections or separate repo files unless asked.

2. **[2026-10-31] Verify by running software, not by document review**
   Do instead: Keep design → plan → execute with every document in Linear as evidence, but don't wait for document approvals. End each issue with a short "try it" scenario Wilson runs on the branch (real datalake for read-only commands; scratch `BRASA_DATA_PATH` + `XDG_CONFIG_HOME` for anything that writes), then merge after confirmation.
