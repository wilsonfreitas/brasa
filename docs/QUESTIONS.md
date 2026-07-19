# QUESTIONS.md — Full Codebase Review (2026-07-19)

Comprehensive code review of **brasa** at commit `ff98d10` (branch `review/2026-07-full-audit`).

> **Post-merge note (2026-07-19):** WIL-95 and WIL-97 merged after this review was written. Questions affected by that merge carry a `Post-merge` note (Q2.1 resolved, Q2.7 partially resolved). This file supersedes the 2025 review that previously lived at `docs/QUESTIONS.md`.

> **Quick-wins pass (2026-07-19, branch `fix/audit-quick-wins`):** 24 S-effort items implemented with TDD — answers recorded inline below. Still open from the quick-win candidates (need a decision): Q7.1, Q2.2, Q2.3, Q11.6, Q13.1.

Every finding below was verified against the current code (file:line references checked). Findings from the two previous reviews (`docs/QUESTIONS.md`, 2025, and `~/dev/python/brasa_QUESTIONS.md`) were re-verified: still-valid ones are carried forward with current line numbers; resolved/stale ones were dropped (e.g., `requests` is now a declared dependency, pytest is no longer pinned `<8`, SSL verification now defaults to on).

**How to answer**: fill each **Answer** with one of
`fix — <how>` · `intended — <why>` · `won't fix — <why>` · `discuss`.
Themes below map 1:1 to candidate Linear sub-issues.

## Summary

| # | Theme | Questions | Max severity |
|---|-------|-----------|--------------|
| 1 | Security & trust model | 7 | high |
| 2 | HTTP download hardening | 8 | high |
| 3 | Cache & metadata store | 8 | high |
| 4 | Template system | 10 | high |
| 5 | Processing & pipeline engine | 7 | high |
| 6 | ETL handlers (`etl.py`) | 7 | high |
| 7 | Query layer (`queries.py`) | 10 | high |
| 8 | Readers & parsers correctness | 8 | high |
| 9 | Dependency resolution & orchestration | 5 | high |
| 10 | Concurrency model | 4 | medium |
| 11 | Testing | 9 | high |
| 12 | CLI & public API | 8 | medium |
| 13 | Repo hygiene, packaging & docs | 8 | medium |

Total: **99 questions**

---

## Theme 1 — Security & trust model

### Q1.1: `exec()` / `eval()` of template-supplied strings in pipeline steps
- **File:** `brasa/engine/pipeline/steps/custom_steps.py:103` (eval), `:139` (exec)
- **Severity:** high
- **Type:** security
- **Effort (est.):** M
- **Issue:** The `apply_lambda` step runs `eval(f"lambda x: {expression}")` and the `exec_code` step runs `exec(code, namespace)` with strings taken directly from template YAML. Three bundled templates use them (`b3-registered-contracts`, `b3-listed-funds-consolidated`, `b3-derivatives-daily`). Since `BRASA_TEMPLATE_PATH` lets users add template roots, any YAML file on that path is arbitrary code execution. The docstring acknowledges this ("WARNING: Only use this for trusted code").
- **Proposed fix:** Either (a) accept the trust model explicitly and document "templates are code" in `docs/TEMPLATES.md` + README security note, or (b) migrate the 3 usages to dedicated registered steps and remove/deprecate `exec_code`/`apply_lambda`.
- **Question(s):** Are templates permanently considered trusted code (same trust level as Python source)? Should `exec_code`/`apply_lambda` be kept, restricted, or removed?
- **Answer:**

### Q1.2: Dynamic function loading from templates (`load_function_by_name`)
- **File:** `brasa/engine/core.py:37-49`, also duplicated at `brasa/engine/pipeline/steps/custom_steps.py:17-21`
- **Severity:** medium
- **Type:** security
- **Effort (est.):** S
- **Issue:** `__import__` of any fully-qualified name from template `function:` fields — same trust question as Q1.1, plus the helper is duplicated in two modules.
- **Proposed fix:** Deduplicate into one helper; optionally restrict to a `brasa.*` module allowlist if templates are ever semi-trusted.
- **Question(s):** Same trust decision as Q1.1 — restrict to `brasa.*` or leave open?
- **Answer:**

### Q1.3: ZipSlip — no path validation in `unzip_file_to()`
- **File:** `brasa/util.py:188-195`
- **Severity:** medium
- **Type:** security
- **Effort (est.):** S
- **Issue:** `zf.extract(name, dest)` extracts every member without checking that the resolved path stays inside `dest`. Zips come from external endpoints (B3, ANBIMA); a crafted archive with `../../` members writes outside the destination. Also: no `with` statement (leaks handle on exception).
- **Proposed fix:** Validate `Path(dest, name).resolve().is_relative_to(Path(dest).resolve())` per member (or use Python 3.12's `filter="data"` semantics), and use a context manager.
- **Question(s):** Agree to add path validation + context manager?
- **Answer:** fix — implemented (branch `fix/audit-quick-wins`, 62f4a6f): members resolving outside the destination raise `ValueError` before extraction; zip handles now use context managers.

### Q1.4: `unzip_recursive()` — unbounded depth, shared temp dir, no cleanup
- **File:** `brasa/util.py:203-211`, consumed by `brasa/engine/download.py:50-58`
- **Severity:** medium
- **Type:** security
- **Effort (est.):** M
- **Issue:** Recursive extraction into `gettempdir()` with no depth cap (the *checksum* path got a cap of 8 in `_hash_zip_contents`, extraction did not), no cleanup of intermediate files, and a shared system temp dir — two concurrent brasa processes extracting archives with identical member names will collide/overwrite. `download.py` then flattens member paths with `Path(filename).name`, so two members with the same basename in different subdirs silently overwrite each other.
- **Proposed fix:** Extract into a per-download `tempfile.mkdtemp()` (or directly into the cache download folder), apply the same depth cap as the checksum path, clean up intermediates, and preserve (or detect collisions of) member basenames.
- **Question(s):** OK to change extraction to a private per-download temp dir with depth cap? Is the basename-flattening behavior relied upon anywhere?
- **Answer:**

### Q1.5: SSL verification disabled in 6 templates
- **File:** `brasa/files/templates/...` — `cvm-companies-registration`, `anbima-index-imab`, `b3-indexes-composition`, `b3-futures-settlement-prices`, `b3-cotahist-yearly`, `b3-cotahist-daily` (`verify_ssl: false`); mechanism at `brasa/engine/template.py:366`, `brasa/downloaders/downloaders.py:47`
- **Severity:** medium
- **Type:** security
- **Effort (est.):** S
- **Issue:** The default is now `verify_ssl: true` (good — previous review's global-disable finding is resolved), but 6 templates still opt out, meaning MITM-able downloads of financial data for the most-used datasets (cotahist!).
- **Proposed fix:** Re-test each endpoint today; remove `verify_ssl: false` where the cert chain now validates; where it genuinely fails, pin the CA bundle (`certifi` + custom CA) instead of disabling, and log a warning at download time.
- **Question(s):** Which of these endpoints still actually fail TLS verification? Acceptable to add a runtime warning when `verify_ssl: false` is active?
- **Answer:**

### Q1.6: SQL from template YAML executed verbatim
- **File:** `brasa/engine/dependency_resolver.py:95-138` (`_run_sql`), `brasa/etl.py:1229-1233` (`execute_query`)
- **Severity:** low
- **Type:** security
- **Effort (est.):** S
- **Issue:** Dependency `query:` blocks and ETL `query:` handlers run raw SQL against DuckDB. Under the "templates are trusted" model this is fine, but it's another spot where the trust boundary should be stated, and `execute_query` runs against the shared read-write `BrasaDB` connection (a template query could `DROP` views).
- **Proposed fix:** Document the trust model; optionally run template SQL on a read-only or scoped connection where feasible.
- **Question(s):** Confirm intended trust model; should `execute_query` get its own connection?
- **Answer:**

### Q1.7: Checksums via `pickle.dumps` + MD5 — cross-version stability
- **File:** `brasa/util.py:113-133`
- **Severity:** medium
- **Type:** correctness/security
- **Effort (est.):** M
- **Issue:** Cache identity (`CacheMetadata.id`) is `md5(pickle.dumps((template, sorted_args, extra_key)))`. `DownloadArgs` canonicalization (WIL-34) made the *values* stable, but pickle's byte output can still change with the default protocol across Python versions — a Python upgrade could invalidate every cache id again (that's exactly why `scripts/migrate_cache_ids.py` had to exist). MD5 itself is fine for dedup but pointless to keep if touching this.
- **Proposed fix:** Hash a deterministic JSON encoding (`json.dumps(..., sort_keys=True)`) with SHA-256, with a one-time migration script (pattern already exists in `scripts/migrate_cache_ids.py`).
- **Question(s):** Worth the migration now, or defer until the next forced cache migration?
- **Answer:**

---

## Theme 2 — HTTP download hardening

### Q2.1: No timeout on any `requests` call
- **File:** `brasa/downloaders/downloaders.py:47, 149, 171, 234` (only the `bcb` client gets `_CLIENT.timeout = 60` at line 17)
- **Severity:** high
- **Type:** bug
- **Effort (est.):** S
- **Issue:** Every `requests.get`/`requests.post` in the downloaders is issued without `timeout=`. A hung B3/ANBIMA endpoint blocks a batch download forever — retry logic never even engages because the first attempt never returns.
- **Proposed fix:** Add a configurable timeout (template-level `timeout:` with a default of e.g. 60s) threaded through `SimpleDownloader` and subclasses.
- **Question(s):** Default timeout value? Template-configurable or global env var?

> **Post-merge (WIL-97):** Resolved — `_DEFAULT_DOWNLOAD_TIMEOUT` with per-class `(connect, read)` overrides now applied in `downloaders.py` (e.g. lines 22-24, 65, 86, 118, 206).

- **Answer:**

### Q2.2: `VnaAnbimaURLDownloader` is broken dead code
- **File:** `brasa/downloaders/downloaders.py:219-255`
- **Severity:** medium
- **Type:** bug
- **Effort (est.):** S
- **Issue:** References `self.attrs`, `self.now`, and `self.get_fname()` — none of which exist on the class or its parents — and returns a 4-tuple instead of the `IO` contract every other downloader follows. Any call path would raise `AttributeError`.
- **Proposed fix:** Delete it (or rewrite against the current contract if a VNA template is planned).
- **Question(s):** Is VNA data still on the roadmap, or can this be deleted?
- **Answer:** fix — implemented: class deleted (no template, export, or test referenced it; any call raised AttributeError).

### Q2.3: BCB downloaders swallow all exceptions and return `None`
- **File:** `brasa/downloaders/downloaders.py:184-193` (`BCBSGSDownloader`), `:200-216` (`BCBCurrencyDownloader`), also `B3FilesURLDownloader.download` returns `None` on non-200 at `:173-174`
- **Severity:** medium
- **Type:** error-handling
- **Effort (est.):** S
- **Issue:** `except Exception: return None` hides the real failure (bad JSON, network error, API change). The engine then raises a generic "null file pointer" `DownloadException` — the root cause is unrecoverable from logs, and `_extract_http_status` can't extract a status. This also defeats retry classification (no status code → falls back to `retry_on_download_exception`).
- **Proposed fix:** Let exceptions propagate (or wrap in `DownloadException` with the original as `__cause__` and status where known); make `B3FilesURLDownloader` raise on non-200 like its siblings.
- **Question(s):** Any known flaky-BCB reason for the swallow, or safe to propagate?
- **Answer:** fix — implemented: BCBSGSDownloader/BCBCurrencyDownloader wrap failures in DownloadException with the original as __cause__; B3FilesURLDownloader raises on non-200 using the `status_code = N` message convention. Tests updated from the returns-None contract.

### Q2.4: Retry backoff has no jitter
- **File:** `brasa/engine/template.py:471-541`
- **Severity:** low
- **Type:** resilience
- **Effort (est.):** S
- **Issue:** Exponential backoff with identical delays across a batch means N failed downloads all retry simultaneously (thundering herd against B3 rate limits — precisely the 429 case the retry codes list).
- **Proposed fix:** `delay * random.uniform(0.5, 1.5)` per attempt.
- **Answer:** fix — implemented (4eec58c): each retry sleep is scaled by `random.uniform(0.5, 1.5)`; logged delay remains the base sequence, and TEST-003 pins the base sequence with jitter factored out.

### Q2.5: Retry telemetry lost when all attempts fail
- **File:** `brasa/engine/template.py:520-523`
- **Severity:** low
- **Type:** observability
- **Effort (est.):** S
- **Issue:** On final failure the original exception is raised without the attempts/status history (only intermediate trials in SQLite carry it). `DownloadResult` has `retry_*` fields but they're `None` on failure paths.
- **Proposed fix:** Attach `retry_info` to the raised `DownloadException` (custom attribute) and populate `DownloadResult.retry_*` on failure too.
- **Answer:**

### Q2.6: HTTP status extracted by regex-parsing exception *strings*, in two places
- **File:** `brasa/engine/cache.py:69-84` and `brasa/engine/template.py:257-277`
- **Severity:** medium
- **Type:** refactor
- **Effort (est.):** M
- **Issue:** The status code travels through the system embedded in an exception message (`"status_code = 404 url = ..."`) and is recovered by regex in two near-duplicate helpers. Any wording change silently breaks retry classification and trial recording.
- **Proposed fix:** Give `DownloadException` a structured `status_code: int | None` attribute set at raise time (`downloaders.py:50-52`), delete both regex helpers.
- **Question(s):** OK to change `DownloadException`'s constructor signature?
- **Answer:**

### Q2.7: Generic `raise Exception(...)` instead of the custom hierarchy
- **File:** `brasa/engine/download.py:52`, `brasa/downloaders/helpers.py:105,111,115`, `brasa/parsers/b3/bvbg087.py:78`, `bvbg028.py:112`, `bvbg086.py:26`
- **Severity:** low
- **Type:** error-handling
- **Effort (est.):** S
- **Issue:** Empty-zip and empty-file conditions raise bare `Exception`, bypassing `InvalidContentException`/`CorruptedContentException` — so they're classified as unexpected `E` errors instead of expected `I` outcomes, and the retry logic treats them as non-classifiable.
- **Proposed fix:** Replace with `InvalidContentException` (content problems) / `CorruptedContentException` (parse problems) consistently.

> **Post-merge (WIL-97):** Partially resolved — the empty-zip site now raises typed `NoDataException` (`engine/download.py:53`, mapped to the new `NO_DATA` status). Still bare `Exception`: `downloaders/helpers.py:145,151,155` and `parsers/b3/bvbg087.py:78`, `bvbg028.py:112`, `bvbg086.py:26`.

- **Answer:** fix — implemented (730a869): empty-file/JSON validators raise `InvalidContentException`; bvbg028/086/087 parsers raise `CorruptedContentException` on malformed XML. (Empty-zip site was already fixed by WIL-97 with `NoDataException`.)

### Q2.8: `B3PagedURLEncodedDownloader` mutates its args and hardcodes page size
- **File:** `brasa/downloaders/downloaders.py:102-133`
- **Severity:** low
- **Type:** refactor
- **Effort (est.):** S
- **Issue:** `url` property mutates `self.args` (side effect in a getter); `pageSize = 100` is hardcoded; only the *first* page raises `InvalidContentException` on empty results — an empty later page is silently fine (probably correct, but undocumented).
- **Proposed fix:** Build the paged params locally in `download()`, make page size a template arg.
- **Answer:**

---

## Theme 3 — Cache & metadata store

### Q3.1: Custom `Singleton` base — testing pain, no thread safety
- **File:** `brasa/engine/core.py:52-73`; used by `CacheManager`, `DatasetCatalog`
- **Severity:** medium
- **Type:** architecture
- **Effort (est.):** L
- **Issue:** Tests must poke `CacheManager.__it__ = None` (duplicated across ≥4 test files); two threads can race `__new__` and double-init; a mid-init failure leaves a half-built singleton cached. This is the single biggest testability tax in the codebase.
- **Proposed fix:** Module-level lazily-created instance behind `get_cache_manager()` with an explicit `reset_for_tests()`, or dependency injection at the API layer. Big refactor — worth its own issue.
- **Question(s):** Appetite for this refactor now, or keep singleton and just centralize the test reset in `conftest.py`?
- **Answer:**

### Q3.2: `select *` + positional row indexing in metadata loads
- **File:** `brasa/engine/cache.py:441-472` (`_load_meta_dict_by_id`)
- **Severity:** medium
- **Type:** correctness
- **Effort (est.):** S
- **Issue:** Row fields are read as `meta_row[1]`…`meta_row[11]` from `select *`. Any column added in the middle of the table (as the migrations in this same file do with `ALTER TABLE ADD COLUMN`, which appends — but a future `create-meta-db.sql` reorder wouldn't) silently shifts every field. The `len(meta_row) > 10` guards show this has already bitten once.
- **Proposed fix:** Name the columns in the SELECT, or use `sqlite3.Row` and access by name.
- **Answer:**

### Q3.3: `save_meta` is SELECT-then-INSERT/UPDATE, not an upsert
- **File:** `brasa/engine/cache.py:474-520`
- **Severity:** medium
- **Type:** concurrency
- **Effort (est.):** S
- **Issue:** Two threads/processes saving the same id race between the existence check and the insert (`process_marketdata` runs 4 workers; the `db_lock` protects in-process but not multi-process runs). SQLite supports `INSERT ... ON CONFLICT(id) DO UPDATE`.
- **Proposed fix:** Single upsert statement.
- **Answer:**

### Q3.4: New SQLite connection per operation; no WAL, no `busy_timeout`
- **File:** `brasa/engine/cache.py:344-349` (`meta_db_connection`)
- **Severity:** medium
- **Type:** performance/concurrency
- **Effort (est.):** M
- **Issue:** Every cache call opens a fresh connection with default journal mode and no busy timeout. Under the 4-worker processing pool, concurrent readers + a writer can throw `sqlite3.OperationalError: database is locked` instead of waiting.
- **Proposed fix:** `PRAGMA journal_mode=WAL` once at DB creation + `PRAGMA busy_timeout=5000` per connection (or a thread-local connection cache).
- **Answer:**

### Q3.5: Migration backfill compares `downloaded = '1'` (string) on a boolean/int column
- **File:** `brasa/engine/cache.py:306-314`; inserts store Python `True`/`False` (`:616`, `save_trial`)
- **Severity:** medium
- **Type:** bug
- **Effort (est.):** S
- **Issue:** `save_trial` binds Python bools (stored as integers 1/0), but `_migrate_download_trials` backfills with `WHERE ... downloaded = '1'`. With SQLite type affinity this only matches if the column has TEXT/NUMERIC affinity in `create-meta-db.sql`; if it's INTEGER, `1 = '1'` is false and legacy rows keep NULL status. `has_successful_trial` (`:666`) uses `downloaded == 1` (int) — the two disagree.
- **Proposed fix:** Verify the DDL affinity; normalize both queries to integer comparison and add a test with a legacy DB fixture.
- **Question(s):** Do you have production `meta.db` files old enough to hit this backfill?
- **Answer:** no defect — verified empirically (dee480c): the column has TEXT affinity, so bound Python bools store as text '1'/'0' and *both* comparison spellings match. Normalized `has_successful_trial` to `= '1'` anyway so the queries cannot diverge if the DDL changes.

### Q3.6: `_should_download` never re-downloads when raw files were deleted but a successful trial exists
- **File:** `brasa/engine/api.py:39-89`
- **Severity:** medium
- **Type:** correctness
- **Effort (est.):** M
- **Issue:** The missing-raw-files guard (REQ-012) only applies when the last status is `D`. If the last status is `.` (PASSED) and the raw folder was manually cleaned, `has_successful_trial` is true and the function returns `False` — the entry is skipped forever unless `--force`. Meanwhile `doctor check_missing_raw` detects exactly this state, so the two disagree about what should happen.
- **Proposed fix:** Extend the file-existence guard to the PASSED path (meta exists but `downloaded_files` missing on disk → re-download), or document that `doctor --fix` is the recovery path.
- **Question(s):** Which behavior is intended?
- **Answer:**

### Q3.7: `clean_meta_db_folder` is a documented no-op inside `remove_meta`
- **File:** `brasa/engine/cache.py:531-538`
- **Severity:** low
- **Type:** refactor
- **Effort (est.):** S
- **Issue:** Now carries a comment explaining why (partitioned outputs are shared; doctor's `check_orphan_db` covers it) — better than before, but the dead call in `remove_meta` (`:561`) still suggests per-entry cleanup happens when it doesn't. Processed parquet rows for a dropped entry are left behind by `brasa cache drop`.
- **Proposed fix:** Remove the no-op method and its call; document in `cache drop` help that db-layer data is reclaimed via `doctor`.
- **Answer:** fix — implemented (2c4beb0): method removed; the partitioned-dataset rationale moved to `remove_meta`'s docstring.

### Q3.8: `load_marketdata` returns the *whole* materialized dataset, not the entry's rows
- **File:** `brasa/engine/cache.py:718-739`
- **Severity:** low
- **Type:** api-design
- **Effort (est.):** M
- **Issue:** Documented in the docstring, but surprising: `get_marketdata("x", refdate=d)` returns every refdate ever processed for the template. For big datasets this is also a memory hit.
- **Proposed fix:** Filter by the entry's `download_args` partition values when partitioning is on `refdate` (the common case), or rename/document loudly.
- **Question(s):** Is the whole-dataset return intended API behavior?
- **Answer:**

---

## Theme 4 — Template system

### Q4.1: YAML keys injected straight into `__dict__` can shadow methods/attributes
- **File:** `brasa/engine/template.py:358-363` (`MarketDataDownloader`), `:59-62` (`MarketDataETL`), `:203-205` (`MarketDataWriter`)
- **Severity:** high
- **Type:** bug
- **Effort (est.):** M
- **Issue:** `for n, v in downloader.items(): self.__dict__[n] = v` means a template key named `download:`, `validate:`, `url` (already special-cased), or `is_pipeline` overwrites the method/property of the same name — instance dict wins over non-data descriptors. This is both a foot-gun and the reason typos are silently accepted (see Q4.2).
- **Proposed fix:** Replace blanket injection with explicit known attributes + a `self.extra: dict` for the rest (readers already do this with `self.attributes`).
- **Answer:**

### Q4.2: No template schema validation — typos are silently ignored
- **File:** `brasa/engine/template.py` (all `reader.get(...)` defaults), 93 bundled templates
- **Severity:** high
- **Type:** correctness
- **Effort (est.):** L
- **Issue:** `reader: {encodng: latin1}` silently falls back to utf-8; unknown top-level sections are absorbed by Q4.1's injection. With 93 templates and 3 field-definition dialects, there is no machine check that a template is well-formed until it fails at runtime (or worse, produces wrong data).
- **Proposed fix:** A Pydantic/JSON-Schema model for templates, validated at load; a `brasa doctor --category templates` check already exists as a natural home; CI check that loads all bundled templates.
- **Question(s):** Pydantic (new dependency) or hand-rolled validation? Strict (reject unknown keys) or warn-only first?
- **Answer:**

### Q4.3: Template discovery re-scans the filesystem (`rglob`) on every cache miss
- **File:** `brasa/engine/template.py:757-780` (`_discover_templates`), called from `retrieve_template` (`:845`), `list_templates`, and `TemplateDependencyGraph._load_templates`
- **Severity:** medium
- **Type:** performance
- **Effort (est.):** S
- **Issue:** Each miss walks every template root recursively. `TemplateDependencyGraph` construction loads *all* templates and is itself rebuilt from scratch by `cli deps/plan/graph/map`, `resolve_dependencies`, and the orchestrator (see Q9.3) — multiplying the scans.
- **Proposed fix:** Cache the discovery map (invalidate via `clear_template_cache()` which already exists), and/or memoize per-process.
- **Answer:**

### Q4.4: Module-level `_template_cache` ignores `BRASA_TEMPLATE_PATH`/`BRASA_DATA_PATH` changes
- **File:** `brasa/engine/template.py:707`
- **Severity:** low
- **Type:** architecture
- **Effort (est.):** S
- **Issue:** Cache is process-global with no invalidation on env change — mostly a test-isolation hazard (tests must call `clear_template_cache()` manually).
- **Proposed fix:** Key the cache by the resolved root list, or clear it in the shared conftest fixture.
- **Answer:**

### Q4.5: `extra-key: date` is frozen at template load time
- **File:** `brasa/engine/template.py:373-379`
- **Severity:** medium
- **Type:** correctness
- **Effort (est.):** S
- **Issue:** `extra_key` is computed with `datetime.now()` in `MarketDataDownloader.__init__`, and templates are cached for the process lifetime. A long-running process (scheduler, notebook kernel) that crosses midnight keeps yesterday's snapshot key — downloads that should create a new daily snapshot are treated as duplicates.
- **Proposed fix:** Make `extra_key` a property evaluated at download time.
- **Question(s):** Any intentional reliance on per-process freezing?
- **Answer:**

### Q4.6: Duplicate template stems within one root are silently resolved by `rglob` order
- **File:** `brasa/engine/template.py:767-780`
- **Severity:** low
- **Type:** correctness
- **Effort (est.):** S
- **Issue:** The `shadows` flag only fires across *different* roots. Two files named `foo.yaml` in different subdirectories of the same root: first `rglob` hit wins, no warning.
- **Proposed fix:** Detect same-root duplicates and raise (they're always a mistake, since id must equal stem).
- **Answer:**

### Q4.7: Template ID validated only after full construction
- **File:** `brasa/engine/template.py:856-863`
- **Severity:** low
- **Type:** performance
- **Effort (est.):** S
- **Issue:** The whole YAML is parsed, pipelines constructed, and functions imported before the id/filename mismatch check. Minor waste; mostly it means side effects (dynamic imports) happen for invalid templates.
- **Proposed fix:** Check `template.get("id")` right after `yaml.safe_load`.
- **Answer:**

### Q4.8: Three field-definition dialects coexist
- **File:** templates (legacy `handler:` blocks vs modern `type:` strings vs mixed); `brasa/fieldsets/`, `brasa/readers/csv.py` (a third, separate Field class hierarchy), `brasa/parsers/fwf.py` (a fourth)
- **Severity:** medium
- **Type:** architecture
- **Effort (est.):** L
- **Issue:** Field parsing logic exists in at least three independent implementations (`fieldsets/`, `readers/csv.py`, `parsers/fwf.py` row templates). Every fix to null handling/decimal parsing must be found and repeated.
- **Proposed fix:** Converge on `fieldsets` as the single type system; port the FWF/CSV row templates to consume it; migrate remaining `handler:`-style templates.
- **Question(s):** Which legacy templates still block this? Is there a migration order you prefer?
- **Answer:**

### Q4.9: 14 legacy templates under `templates/**/legacy/` — retirement plan?
- **File:** `brasa/files/templates/` (93 yaml, 14 in `legacy/` excluded from discovery)
- **Severity:** low
- **Type:** architecture
- **Effort (est.):** M
- **Issue:** Legacy paths are excluded from discovery (good, per napkin note), but the function-based reader/ETL code paths they exercised still live throughout `template.py`/`processing.py`/`etl.py`. If nothing discoverable uses `reader.function` anymore, that whole branch may be removable.
- **Proposed fix:** Audit which non-legacy templates still use `function:`-style readers/ETL; delete the legacy branches when count reaches zero.
- **Question(s):** Timeline for deleting `templates/legacy/` and the function-based code paths?
- **Answer:**

### Q4.10: Implicit writer defaults (`layer`, auto-created writer)
- **File:** `brasa/engine/template.py:678-693`
- **Severity:** low
- **Type:** api-design
- **Effort (est.):** S
- **Issue:** Missing `writer:` silently gets defaults (INPUT layer, no partitioning); ETL templates silently default to STAGING via `_layer` poking. Reasonable defaults, but undocumented, and unpartitioned-by-default output is a footgun for large datasets.
- **Proposed fix:** Document defaults in `TEMPLATES.md`; consider a doctor/template-validation warning when a non-trivial template has no partitioning.
- **Answer:**

---

## Theme 5 — Processing & pipeline engine

### Q5.1: Engine hardcodes specific template IDs in `get_fname_part`
- **File:** `brasa/engine/processing.py:19-51`
- **Severity:** medium
- **Type:** architecture
- **Effort (est.):** M
- **Issue:** `b3-company-info`, `b3-company-details`, `b3-cash-dividends`, `b3-indexes-theoretical-portfolio` are special-cased by name inside the generic engine to build filenames from specific download args. Template-specific knowledge belongs in the template.
- **Proposed fix:** Add an `output-filename-args: [issuingCompany]`-style reader/writer option and drive the generic path from it; also guard `df["refdate"].iloc[0]` against empty frames (IndexError today).
- **Answer:**

### Q5.2: Multi-dataset outputs that are all empty never get marked processed
- **File:** `brasa/engine/processing.py:157-197`
- **Severity:** medium
- **Type:** bug
- **Effort (est.):** S
- **Issue:** `mark_as_processed()` is only called inside `save_partitioned_parquet_file`, and empty frames (`dx.shape[0] > 0`) are skipped. A download whose every dataset is legitimately empty stays `is_processed=False` forever and is re-processed on every run.
- **Proposed fix:** Mark processed after the loop regardless (an empty result is still a processed result), or record it as INVALID explicitly.
- **Question(s):** Is "all datasets empty" a valid outcome for any current template?
- **Answer:**

### Q5.3: Concurrent `pq.write_to_dataset(..., existing_data_behavior="delete_matching")` from 4 workers
- **File:** `brasa/engine/processing.py:91-107` + `brasa/engine/api.py:721` (ThreadPoolExecutor)
- **Severity:** high
- **Type:** concurrency
- **Effort (est.):** M
- **Issue:** Multiple worker threads write into the same dataset root concurrently with `delete_matching`. For distinct refdate partitions this is *probably* safe, but two entries that map to the same partition (reprocessing, extra-key snapshots, non-refdate-partitioned templates) can interleave delete/write and lose data. There's a `debug/overwrite-partitioned-data` branch in the repo suggesting this has been investigated before.
- **Proposed fix:** Decide and enforce the invariant: either serialize writes per dataset (per-dataset lock), or guarantee one entry ↔ one partition and assert it.
- **Question(s):** What did the `debug/overwrite-partitioned-data` investigation conclude?
- **Answer:**

### Q5.4: `_get_schema_from_fields` swallows all errors and returns `None` silently
- **File:** `brasa/engine/processing.py:125-140`
- **Severity:** medium
- **Type:** error-handling
- **Effort (est.):** S
- **Issue:** If schema generation fails, data is written *without* schema enforcement and nobody is told — type drift then surfaces much later in doctor's schema-drift check or in queries.
- **Proposed fix:** `logger.warning` with the exception; consider failing hard when the template explicitly declares fields.
- **Answer:** fix — implemented (1c9d68d): failures now log a warning with the exception before returning None.

### Q5.5: Pipeline executor re-wraps all step failures as `RuntimeError`
- **File:** `brasa/engine/pipeline/executor.py:120-128`
- **Severity:** low
- **Type:** error-handling
- **Effort (est.):** S
- **Issue:** Typed exceptions (e.g. `InvalidContentException` from a read step) become `RuntimeError`, so upstream classification (expected vs unexpected in `DownloadResult`/TaskReport) can't see them. `__cause__` is preserved, but callers match on type.
- **Proposed fix:** Re-raise brasa-domain exceptions unwrapped; wrap only unknown ones.
- **Answer:** fix — implemented (349b979): added `DOMAIN_EXCEPTIONS` registry in `engine.exceptions`; both executors re-raise those unwrapped and wrap only unknown exceptions in RuntimeError.

### Q5.6: ETL pipelines have no checkpoint/transaction semantics
- **File:** `brasa/engine/pipeline/etl_executor.py:132-175`
- **Severity:** low
- **Type:** architecture
- **Effort (est.):** L
- **Issue:** A failure during the final write loses the whole computation, and a partial `write_to_dataset` can leave a mixed old/new dataset. For current dataset sizes this may be acceptable.
- **Proposed fix:** Write to a temp dir + atomic rename, or accept and document.
- **Question(s):** Have partial-write corruptions actually happened?
- **Answer:**

### Q5.7: Shared mutable `PipelineContext` across steps — by design?
- **File:** `brasa/engine/pipeline/context.py`, `executor.py`
- **Severity:** low
- **Type:** architecture
- **Effort (est.):** M
- **Issue:** Steps can mutate `intermediate_results` and see each other's state. Flexible, but makes steps order-dependent in non-obvious ways and hard to unit test in isolation (see Q11.5).
- **Proposed fix:** Document the contract (which keys are stable API for steps) — full isolation is probably not worth it.
- **Answer:**

---

## Theme 6 — ETL handlers (`etl.py`)

### Q6.1: `create_cotahist_dataset` discards the result of `sort_by` — data written unsorted
- **File:** `brasa/etl.py:392-397`
- **Severity:** high
- **Type:** bug
- **Effort (est.):** S
- **Issue:** ```python
  tb_cotahist = pyarrow.concat_tables([...])
  tb_cotahist.sort_by([("refdate", "ascending")])   # return value dropped
  write_dataset(tb_cotahist.to_pandas(), ...)
  ```
  `Table.sort_by` returns a new table; the dataset is written unsorted. Anything downstream assuming refdate order (e.g. `.iloc[0]`/`.iloc[-1]` patterns, `ffill`) may be silently wrong.
- **Proposed fix:** `tb_cotahist = tb_cotahist.sort_by(...)`; audit other handlers for the same pattern.
- **Answer:** fix — implemented (1b1289c): `tb_cotahist = tb_cotahist.sort_by(...)`; regression test asserts the written frame is sorted. Audited the rest of etl.py — this was the only dropped-return `sort_by`.

### Q6.2: Hardcoded 2021-06-10 data patch runs inside `create_equities_returns` forever
- **File:** `brasa/etl.py:479-509`
- **Severity:** medium
- **Type:** architecture
- **Effort (est.):** M
- **Issue:** A one-off correction for a missing B3 date (2021-06-10) is inlined in the handler — 30 lines of groupby/apply that execute on every run, with the "why" only in a terse comment.
- **Proposed fix:** Move data patches into a declarative corrections mechanism (e.g. a `corrections/` dataset or template-level patch list), or at minimum extract to a named function with documentation.
- **Answer:**

### Q6.3: `etl.py` is a 1233-line God module of ~30 handlers — migration plan?
- **File:** `brasa/etl.py`
- **Severity:** medium
- **Type:** architecture
- **Effort (est.):** L
- **Issue:** The pipeline-based ETL system exists and works (several templates already use it, and `create_b3_listed_funds` was already migrated). The remaining function handlers duplicate the load→transform→write skeleton with hardcoded schemas (`:146-153`, `:229-241`, `:279-287`, …).
- **Proposed fix:** Rank handlers by migration difficulty; migrate the trivial ones (concat/copy/rename/returns) to pipeline steps; keep genuinely complex ones (adjusted prices) as `custom` steps.
- **Question(s):** Which handlers are actively used today (vs orphaned)? Priority order?
- **Answer:**

### Q6.4: `ffill_n_remove_duplicates` — first-non-null, not forward-fill
- **File:** `brasa/etl.py:1008-1017`
- **Severity:** medium
- **Type:** correctness
- **Effort (est.):** S
- **Issue:** `ix.idxmax()` on the boolean non-null mask picks the *first* non-null `payment_date`/`ratio` within a duplicate group, and `df.iloc[[0], :]` keeps the first row. If two sources disagree, "first wins" — the name says ffill, and whether first-wins is correct for dividend data needs a decision.
- **Proposed fix:** Confirm intended precedence (company-info source vs cash-dividends source), implement explicitly, rename.
- **Answer:**

### Q6.5: Deprecated `create_b3_listed_funds` still fully implemented
- **File:** `brasa/etl.py:878-915`
- **Severity:** low
- **Type:** cleanup
- **Effort (est.):** S
- **Issue:** Deprecation warning added and the docstring even says the columns it expects no longer exist — i.e. it can only fail. Dead weight.
- **Proposed fix:** Delete it (git history keeps it).
- **Answer:** fix — implemented (e932f69): function deleted; no template or test referenced it and it expected column names that no longer exist.

### Q6.6: Row-wise `.apply(lambda ...)` where vectorized ops exist
- **File:** `brasa/etl.py:489, 925, 934, 938`; `brasa/readers/helpers.py:635`
- **Severity:** low
- **Type:** performance
- **Effort (est.):** S
- **Issue:** `df[col].apply(lambda x: re.sub(...))` → `df[col].str.replace(regex=True)`; `groupby().apply(lambda x: x.shape[0])` → `groupby().size()`. Straightforward wins on large frames.
- **Proposed fix:** Mechanical replacement + spot-check outputs.
- **Answer:** fix — implemented (e4a50f1): `groupby().apply(...)` → `size()`/`nunique()`; regex scrubbing → `.str.replace(regex=True)`. Equivalence verified; also silences a pandas FutureWarning.

### Q6.7: `create_bcb_currency_data` — serial HTTP loop over all currencies, no error isolation
- **File:** `brasa/etl.py:212-240`
- **Severity:** low
- **Type:** resilience
- **Effort (est.):** M
- **Issue:** Iterates every PTAX currency with one blocking request each (inside an *ETL* handler — network I/O in the transform layer, unlike every other ETL which reads local datasets). One failing currency aborts the whole run; there's no retry/timeout (Q2.1 applies).
- **Proposed fix:** Move acquisition into a proper download template (per currency arg), keep the ETL pure.
- **Answer:**

---

## Theme 7 — Query layer (`queries.py`)

### Q7.1: `get_returns`/`get_prices` silently *average* duplicate (refdate, symbol) rows
- **File:** `brasa/queries.py:392, 433-435`
- **Severity:** high
- **Type:** correctness
- **Effort (est.):** S
- **Issue:** `pivot_table` defaults to `aggfunc="mean"`. If the underlying dataset ever contains duplicate rows for a (refdate, symbol) pair (reprocessing overlap, extra-key snapshots, concat ETLs), users get the *average of prices* with no warning — the worst possible failure mode for financial data.
- **Proposed fix:** Use `pivot` (raises on duplicates) or `pivot_table(aggfunc="last")` after an explicit duplicate check that logs/raises.
- **Answer:** fix — implemented: explicit pre-pivot duplicate check in get_returns/get_prices raises ValueError naming up to 10 offending (refdate, symbol) pairs; silent averaging is gone.

### Q7.2: Empty-result crashes: `df.index[0]` / `pc.max` with no guard
- **File:** `brasa/queries.py:399, 442` (IndexError on unknown symbol or reversed dates), `:923, 939, 957` (`pyarrow.compute.max` on possibly-empty tables)
- **Severity:** medium
- **Type:** bug
- **Effort (est.):** S
- **Issue:** `get_returns("TYPO3")` raises a bare `IndexError` from deep inside; `get_symbols` helpers on empty datasets propagate nulls. No `start <= end` validation either (reversed range → empty → IndexError).
- **Proposed fix:** Validate inputs, return an empty typed DataFrame (or raise a clear `ValueError("no data for symbols ...")`).
- **Answer:** fix — implemented (3dc9229): `start > end` raises ValueError (`_resolve_date_range`); empty pivots return an empty DataFrame before the calendar reindex. The `pc.max` helpers were verified safe on empty datasets (max=None filter matches nothing → `[]`) — no change needed there.

### Q7.3: `BrasaDB` class-level shared read-write DuckDB connection
- **File:** `brasa/queries.py:37-55`
- **Severity:** medium
- **Type:** concurrency
- **Effort (est.):** M
- **Issue:** One process-global connection, no lock; the health-check/reconnect (`:50-54`) races under threads; a second process holding the file blocks (DuckDB single-writer). Also every consumer gets read-**write** access (see Q12.4).
- **Proposed fix:** Document single-threaded intent + add a lock, or per-call `duckdb.connect(read_only=True)` for queries and a dedicated writer path for view creation.
- **Answer:**

### Q7.4: Library code prints to stdout (75 `print()` calls outside cli.py)
- **File:** `brasa/queries.py` (`create_all_views` `:195-224`, `describe` `:792`), `engine/catalog.py`, `engine/doctor.py`, `engine/download_plan.py`, `engine/pipeline_map.py`, `engine/reporting.py`, `fieldsets/adapters/*`
- **Severity:** medium
- **Type:** api-design
- **Effort (est.):** M
- **Issue:** Importing brasa as a library (notebook, service) produces unavoidable stdout noise; there's no way to silence `create_all_views`. Reporting/pipeline_map printing is arguably their job; adapters and queries printing is not.
- **Proposed fix:** Route non-CLI modules through `logging`; keep rich/print only in `reporting.py` display paths and the CLI.
- **Question(s):** Which modules do you consider "presentation" (allowed to print)?
- **Answer:**

### Q7.5: bizdays global option mutation without try/finally
- **File:** `brasa/queries.py:396-401, 439-444`; `brasa/etl.py:1172-1175`; import-time `set_option("mode.datetype", "datetime")` at `brasa/util.py:18`
- **Severity:** medium
- **Type:** correctness
- **Effort (est.):** S
- **Issue:** `set_option("mode", "pandas")` … restore is not exception-safe (an error mid-block leaks global state), not thread-safe, and `import brasa` permanently mutates bizdays' global datetype for the whole process — surprising for co-resident code using bizdays.
- **Proposed fix:** try/finally (or a context manager) around option flips; document the import side effect or scope it.
- **Answer:** fix — implemented (09e674d): new `util.bizdays_mode()` context manager used in get_returns/get_prices and the adjusted-returns ETL handler. The import-time `set_option("mode.datetype")` side effect remains (documenting/scoping it is a separate decision).

### Q7.6: `list_tables` returns `[]` on any exception; view-creation errors truncated to 100 chars
- **File:** `brasa/queries.py:283-284`, `:150`
- **Severity:** low
- **Type:** error-handling
- **Effort (est.):** S
- **Issue:** "DB corrupt" and "no views yet" are indistinguishable; SQL error detail is cut off exactly where the useful part (path) usually is.
- **Proposed fix:** Log the exception in `list_tables`; drop the `[:100]` truncation.
- **Answer:** fix — implemented (47e809c): `list_tables` logs a warning (connection errors included) before returning `[]`; view-creation errors are no longer truncated to 100 chars.

### Q7.7: `get_marketdata` swallows everything and returns `None`
- **File:** `brasa/engine/api.py:116-127`
- **Severity:** medium
- **Type:** error-handling
- **Effort (est.):** S
- **Issue:** `except Exception: cache.remove_meta(meta); return None` — download bug, parse bug, disk full: all become a silent `None` with the cache entry *removed*. Callers can't distinguish "no data for that date" from "everything is broken".
- **Proposed fix:** Log with traceback at minimum; consider raising for non-download exceptions.
- **Answer:**

### Q7.8: `describe()` requires pandas metadata; superseded by `describe_dataset`
- **File:** `brasa/queries.py:787-793`
- **Severity:** low
- **Type:** cleanup
- **Effort (est.):** S
- **Issue:** `schema.metadata[b"pandas"]` KeyErrors on parquet written without pandas metadata (pipeline writes via `pa.Table.from_pandas` keep it, but pure-arrow paths may not). `describe_dataset` is the robust replacement.
- **Proposed fix:** Deprecate `describe()` in favor of `describe_dataset()`.
- **Answer:**

### Q7.9: `get_symbols` contract: unknown type → `[]`, unknown kwargs ignored
- **File:** `brasa/queries.py:970-1002`
- **Severity:** low
- **Type:** api-design
- **Effort (est.):** S
- **Issue:** `get_symbols("equities")` (plural typo) silently returns `[]`; the `$symbol` CLI DSL then expands to an empty list, downloading nothing without explanation.
- **Proposed fix:** Raise `ValueError` listing valid types.
- **Answer:**

### Q7.10: Commented-out `get_timeseries` block (29 lines) and `__all__`/`__init__` export mismatch
- **File:** `brasa/queries.py:343-371`; `queries.__all__` (18 names) vs `brasa/__init__.py.__all__` (38 names, missing `get_template_*`)
- **Severity:** low
- **Type:** cleanup
- **Effort (est.):** S
- **Issue:** Dead commented code; and two different notions of "public API" (see also Q12.5).
- **Proposed fix:** Delete the comment block; reconcile export lists.
- **Answer:** fix — partially implemented (8e0ee4d): dead `get_timeseries` block deleted. Export-list reconciliation deferred to Q12.5, since the right fix depends on whether the public API shrinks.

---

## Theme 8 — Readers & parsers correctness

### Q8.1: `PandasAdapter(template.fields, ...)` passed a `TemplateFields`, not a `Fieldset`
- **File:** `brasa/readers/helpers.py:~397` (the correct `Fieldset.from_template_fields` pattern sits ~90 lines above at `:303-305`, and its commented-out remains sit directly above the bug)
- **Severity:** high
- **Type:** bug
- **Effort (est.):** S
- **Issue:** Second occurrence builds the adapter from the raw template fields object instead of a `Fieldset`. The commented-out correct version right above it strongly suggests an unfinished edit. Whether this path currently "works by duck-typing" or silently mis-parses needs a test.
- **Proposed fix:** Use `Fieldset.from_template_fields(...)` like the first occurrence; add a regression test for that reader.
- **Question(s):** Which template exercises this function? Is its output currently correct?
- **Answer:** stale premise — `template.fields` has been a `Fieldset` since the fieldsets refactor, and both construction patterns produce identical fields (pinned by regression test). Aligned the site with the explicit `Fieldset.from_template_fields` pattern and removed the commented-out remains (faffe34). Note: no template currently uses this reader (settlement-prices is pipeline-based).

### Q8.2: FWF text-mode path: `_line` referenced before assignment → `NameError`
- **File:** `brasa/parsers/fwf.py:135-160`
- **Severity:** high
- **Type:** bug
- **Effort (est.):** S
- **Issue:** `_line = line.decode(...)` only happens `if isinstance(line, bytes)`, but the file is opened in *text* mode (`Path.open(encoding=...)`) — so in the `str`-filename branch `_line` is never assigned and the first line raises `NameError`. Ergo that branch is unreachable in practice (callers must pass byte iterators), i.e. it's broken dead code. The old review flagged the dead bytes check; the reality is worse.
- **Proposed fix:** `_line = line.decode(encoding) if isinstance(line, bytes) else line` (both branches), plus one test with a real FWF file per mode; also consider `strict=True` in the `zip(...)` (Q8.3).
- **Answer:** fix — implemented (4c5735b): `_line = line.decode(encoding) if isinstance(line, bytes) else line` in both branches; tests cover text-file and byte-iterator modes.

### Q8.3: `zip(..., strict=False)` silently drops mismatched columns in parsers
- **File:** `brasa/parsers/util.py:54, 78`, `brasa/parsers/fwf.py:29, 148, 160`
- **Severity:** medium
- **Type:** correctness
- **Effort (est.):** S
- **Issue:** A header/row length mismatch (format change from B3, truncated file) silently truncates fields instead of failing. For financial parsing, silent truncation is the wrong default. (`util.py:252` in `KwargsIterator` is fine — lengths are guaranteed there.)
- **Proposed fix:** `strict=True` in the parsers; let the resulting `ValueError` surface as `CorruptedContentException`.
- **Answer:**

### Q8.4: Decimal casting goes through float64 — precision loss for high-precision fields
- **File:** `brasa/fieldsets/adapters/pyarrow_adapter.py:365-397`
- **Severity:** medium
- **Type:** correctness
- **Effort (est.):** M
- **Issue:** String → float64 → decimal loses digits beyond ~15 significant figures. For current B3/ANBIMA fields this may never bite (rates have ≤8 decimals and small integer parts), but the adapter is generic.
- **Proposed fix:** Cast string→decimal directly (PyArrow supports it) with the float path as fallback; add a test with a 20-digit decimal.
- **Question(s):** Do any current datasets carry values where float64 precision is insufficient?
- **Answer:**

### Q8.5: Bitwise `|` instead of `or` in `unified_reader`
- **File:** `brasa/fieldsets/adapters/unified_reader.py:103`
- **Severity:** low
- **Type:** cleanup
- **Effort (est.):** S
- **Issue:** `isinstance(a, str) | isinstance(a, Path)` — works, but returns the wrong type and reads as a typo. `isinstance(a, (str, Path))` is the idiom.
- **Proposed fix:** One-line change.
- **Answer:** fix — implemented (6664f6f): replaced with `isinstance(x, (str, Path))`; behavior was already correct (bool | bool is bool), so this is idiom cleanup pinned by tests.

### Q8.6: Hardcoded `latin1` default and no-context-manager zips in `unzip_and_get_content`
- **File:** `brasa/util.py:214-224`
- **Severity:** low
- **Type:** correctness
- **Effort (est.):** S
- **Issue:** `encoding="latin1"` default with `index=-1` member selection by position; no `with`; decoding failure propagates as a raw `UnicodeDecodeError`.
- **Proposed fix:** Context manager; take encoding from the template (readers already carry `encoding`); select member by name where possible.
- **Answer:** fix — implemented (folded into 62f4a6f): both zip helpers use `with`; `_is_zip` accepts `Path`. The hardcoded `latin1` default remains (changing it is a behavior decision).

### Q8.7: `SuppressUserWarnings` resets filters instead of restoring them
- **File:** `brasa/util.py:21-26`
- **Severity:** low
- **Type:** cleanup
- **Effort (est.):** S
- **Issue:** `filterwarnings("default", ...)` on exit clobbers any caller-installed filters. `warnings.catch_warnings()` does this correctly. Check if the class is even used — if not, delete.
- **Proposed fix:** Replace with `contextlib` + `catch_warnings`, or delete if unused.
- **Answer:** fix — implemented (cf366a6): delegates to `warnings.catch_warnings()`, restoring the caller's filters exactly.

### Q8.8: Silent `except Exception: return None` in parser helpers
- **File:** `brasa/parsers/util.py:88-89`, `brasa/parsers/b3/bvbg087.py:12-13`, `brasa/engine/pipeline/steps/b3_steps.py:295-296, 422-423`
- **Severity:** medium
- **Type:** error-handling
- **Effort (est.):** M
- **Issue:** Field-level parse failures degrade to `None` with no counter or log. For a dataset with a format change, you get a column of nulls instead of an error. (The fieldsets adapters have an `errors="coerce"` policy — these ad-hoc swallows bypass any policy.)
- **Proposed fix:** Funnel through the fieldset `errors=` policy, or at least count+log coercions per column.
- **Answer:**

---

## Theme 9 — Dependency resolution & orchestration

### Q9.1: Inconsistent `return` vs `continue` on optional-dependency failure
- **File:** `brasa/engine/dependency_resolver.py:330-344` vs `:346-359`
- **Severity:** high
- **Type:** bug
- **Effort (est.):** S
- **Issue:** In `_run_upstream_templates`, when an optional upstream *raises*, the code warns and `return`s — abandoning all remaining dataset refs for that dependency. When it merely *reports failure*, it warns and `continue`s. The asymmetry looks accidental; an optional failure on ref 1 of 3 silently skips refreshing refs 2–3.
- **Proposed fix:** `continue` in both branches (or document why early-return is right).
- **Answer:** fix — implemented (671e7fb): `continue` in both optional-failure branches; regression test proves ref 2 is still attempted when ref 1's producer raises, and that required failures still raise `DependencyResolutionError`.

### Q9.2: Upstream resolution processes but never *downloads*
- **File:** `brasa/engine/dependency_resolver.py:322-326`
- **Severity:** medium
- **Type:** correctness
- **Effort (est.):** M
- **Issue:** For a download-type producer, `_run_upstream_templates` calls `process_marketdata(producer)` — which only parses already-downloaded entries. If the upstream was never downloaded (fresh cache), the dependency query then fails with "no rows" even though the system knows how to fetch it. Is auto-download intentionally excluded (cost/safety), or an oversight?
- **Proposed fix:** Either auto-`download_marketdata` with smart-update for download producers, or emit a clear error telling the user which download command to run.
- **Answer:**

### Q9.3: `TemplateDependencyGraph` rebuilt from scratch at every use site
- **File:** `brasa/engine/dependency_graph.py:149-171`; constructed in `cli.py` (deps/plan/graph/map/run/run-all), `dependency_resolver.py:195`, `api.py:894-920`, orchestrator
- **Severity:** medium
- **Type:** performance
- **Effort (est.):** S
- **Issue:** Construction loads *all* 93 templates (YAML parse + pipeline construction each). A `download` with 3 dependency args can build the graph, and each nested `process_etl(..., resolve_dependencies=True)` builds it again recursively.
- **Proposed fix:** Cache the graph per process (invalidate with `clear_template_cache`), or pass one instance down the call tree.
- **Answer:**

### Q9.4: Staleness via full `rglob` mtime scans
- **File:** `brasa/engine/dependency_resolver.py:36-52` (`_get_latest_mtime`)
- **Severity:** low
- **Type:** performance
- **Effort (est.):** M
- **Issue:** Every freshness check walks the entire input dataset tree. The `.last_processed` marker already exists for outputs — inputs could use the same marker instead of scanning thousands of partition files.
- **Proposed fix:** Compare output marker vs input *markers* (fall back to scan when marker missing).
- **Answer:**

### Q9.5: `doctor.py` at 1880 lines — split into a package?
- **File:** `brasa/engine/doctor.py`
- **Severity:** low
- **Type:** refactor
- **Effort (est.):** M
- **Issue:** 15+ check functions, a validations rule engine, fix closures, and report rendering in one module. It's well-organized internally but past the size where navigation and testing stay comfortable (its test file is 1425 lines too).
- **Proposed fix:** `engine/doctor/` package: `checks_raw.py`, `checks_db.py`, `checks_gaps.py`, `validations.py`, `report.py`.
- **Answer:**

---

## Theme 10 — Concurrency model

### Q10.1: What is the supported concurrency contract, overall?
- **File:** cross-cutting (`api.py:643` db_lock, `queries.py:37`, `core.py:52`, `cache.py:344`)
- **Severity:** medium
- **Type:** architecture
- **Effort (est.):** M
- **Issue:** Today: singletons without locks, a shared DuckDB connection, per-call SQLite connections, one ThreadPoolExecutor in `process_marketdata`, and no statement anywhere about whether two brasa processes may share one `BRASA_DATA_PATH`. Several findings above (Q3.3, Q3.4, Q5.3, Q7.3) hang on this decision.
- **Proposed fix:** Write the contract down (e.g. "one process, library single-threaded except process_marketdata's internal pool; multi-process unsupported"), then fix only what violates it.
- **Question(s):** Do you run concurrent brasa processes against one cache (e.g. cron + manual)?
- **Answer:**

### Q10.2: Is 4-worker processing actually faster given the db_lock + GIL?
- **File:** `brasa/engine/api.py:573-737`
- **Severity:** low
- **Type:** performance
- **Effort (est.):** M
- **Issue:** Workers parallelize `_read_marketdata` (pandas parsing — partly GIL-bound, partly native) and serialize SQLite writes. No benchmark recorded. If the win is small, dropping to sequential removes Q5.3's race for free.
- **Proposed fix:** Benchmark 1 vs 4 workers on a real batch (e.g. a month of bvbg086); keep or remove accordingly.
- **Answer:**

### Q10.3: `process_single` reads via private `cache._load_meta_dict_by_id`
- **File:** `brasa/engine/api.py:656`; also `cli.py:1431`
- **Severity:** low
- **Type:** refactor
- **Effort (est.):** S
- **Issue:** Two call sites reach into a `_`-private CacheManager method — it's de facto public.
- **Proposed fix:** Promote to `load_meta_by_id()` (public, documented) or add a proper accessor.
- **Answer:**

### Q10.4: `Singleton.__new__` race (double-init) under threads
- **File:** `brasa/engine/core.py:58-65`
- **Severity:** low
- **Type:** concurrency
- **Effort (est.):** S
- **Issue:** Two threads can both see `__it__ is None` and both run `init()` (directory creation + DB migration!). Today CacheManager is usually constructed on the main thread first, so it's latent. Superseded by Q3.1 if that refactor happens.
- **Proposed fix:** Class-level lock, or resolve via Q3.1.
- **Answer:**

---

## Theme 11 — Testing

### Q11.1: Zero coverage for `queries.py` (1002 lines) — the primary consumer API
- **File:** `tests/` (no test_queries.py)
- **Severity:** high
- **Type:** testing
- **Effort (est.):** L
- **Issue:** `get_prices`, `get_returns`, `get_symbols`, `BrasaDB`, view creation — all untested. Q7.1/Q7.2 would have been caught by 5 tests with a small parquet fixture.
- **Proposed fix:** Fixture: tiny in-tmpdir cache with 2 symbols × 10 dates written via the real writer; test the public query functions including duplicate-row and empty-result cases.
- **Answer:**

### Q11.2: Zero coverage for `etl.py` handlers
- **File:** `tests/` (only `test_b3_listed_funds.py` touches one)
- **Severity:** high
- **Type:** testing
- **Effort (est.):** L
- **Issue:** ~30 functions transforming financial data untested — Q6.1's silent unsorted-write is the canary. Testing pairs naturally with the migration decision (Q6.3): migrated pipelines get step tests, surviving handlers get golden-file tests.
- **Proposed fix:** For each *actively used* handler, one golden test: small input datasets → expected output frame.
- **Answer:**

### Q11.3: Readers/parsers largely untested (except bvbg087/bvbg086/bvbg028 paths)
- **File:** `brasa/readers/helpers.py` (675 lines), `readers/csv.py`, `parsers/anbima/*`, `parsers/td.py`, `parsers/fwf.py`, `parsers/util.py`
- **Severity:** high
- **Type:** testing
- **Effort (est.):** L
- **Issue:** The most correctness-critical layer. Q8.1/Q8.2 are live bugs in this untested zone. Sample raw files already exist in `data/` — the fixtures are there, the tests aren't.
- **Proposed fix:** One parse test per reader function using the existing sample files, asserting shape + a few spot values (the DatasetCase pattern from `test_bvbg028_datasets.py` generalizes well).
- **Answer:**

### Q11.4: Integration tests hit live B3 endpoints with `time.sleep(5)`
- **File:** `tests/test_downloads.py` and friends (`@pytest.mark.integration`)
- **Severity:** medium
- **Type:** testing
- **Effort (est.):** M
- **Issue:** Slow, flaky, external. The `--no-integration` flag exists (good), but the default run still includes them, and 7 tests are permanently skipped for dead endpoints — noise that hides real regressions.
- **Proposed fix:** Invert the default (integration opt-*in* via `--integration` or a marker filter in `pyproject.toml`); delete the 7 dead-endpoint tests or convert to mocked equivalents; consider `responses`/VCR for downloader unit tests.
- **Answer:**

### Q11.5: Pipeline steps (~3500 lines under `engine/pipeline/steps/`) lack unit tests
- **File:** `tests/test_pipeline.py` (registry only), `test_pipeline_steps_multi.py` (partial)
- **Severity:** medium
- **Type:** testing
- **Effort (est.):** L
- **Issue:** Steps are the new backbone (the migration target of Q6.3) — each is a small pure-ish function, ideal unit-test material, currently tested only indirectly.
- **Proposed fix:** Table-driven tests per step: input frame + params → expected frame.
- **Answer:**

### Q11.6: No coverage tooling or threshold
- **File:** `pyproject.toml`
- **Severity:** medium
- **Type:** testing
- **Effort (est.):** S
- **Issue:** No `pytest-cov`; coverage is unknown and can regress invisibly.
- **Proposed fix:** Add `pytest-cov`, publish the number, gate at the current baseline (ratchet up later).
- **Answer:** fix — implemented: pytest-cov added; baseline measured at 67% (--no-integration), CI gates at 65% (`--cov-fail-under=65`) — ratchet up as Q11.1–Q11.3 land.

### Q11.7: No CI workflow
- **File:** `.github/` (has copilot instructions/agents/prompts, but **no `workflows/`**)
- **Severity:** high
- **Type:** testing
- **Effort (est.):** S
- **Issue:** The Definition of Done (pytest + ruff + pre-commit) is enforced only by discipline. A 10-line GitHub Actions workflow (`uv sync && uv run pytest --no-integration && uv run ruff check`) closes the gap.
- **Proposed fix:** Add `.github/workflows/ci.yaml` for pushes/PRs, py310–py313 matrix if cheap.
- **Answer:** fix — implemented (1ecdddb): `.github/workflows/ci.yaml` runs ruff check, format check, and `pytest --no-integration` on Python 3.10/3.11/3.12 via uv, on pushes and PRs to main.

### Q11.8: Duplicate `temp_cache`/singleton-reset fixtures across ≥4 test files
- **File:** `tests/test_cache.py`, `test_download_status.py`, `test_invalid_downloads.py`, `test_download_retry.py`
- **Severity:** low
- **Type:** testing
- **Effort (est.):** S
- **Issue:** Each re-implements CacheManager reset; drift between copies causes order-dependent test bugs. (Root cause is Q3.1.)
- **Proposed fix:** One canonical fixture in `conftest.py`.
- **Answer:** fix — implemented (ab5a073): the four byte-identical fixtures collapsed into one `temp_cache` in conftest.py.

### Q11.9: No per-test timeout
- **File:** `pyproject.toml`
- **Severity:** low
- **Type:** testing
- **Effort (est.):** S
- **Issue:** With no HTTP timeouts (Q2.1), a hung endpoint hangs the suite.
- **Proposed fix:** `pytest-timeout` with e.g. 60s default (higher for marked integration tests).
- **Answer:** fix — implemented (35bec8b): pytest-timeout added with `timeout = 120` (generous because WIL-97 read timeouts reach 90s for integration runs).

---

## Theme 12 — CLI & public API

### Q12.1: `main()` is a ~600-line if/elif chain
- **File:** `brasa/cli.py:853-1456`
- **Severity:** medium
- **Type:** refactor
- **Effort (est.):** M
- **Issue:** Every command lives inline in one function (`# noqa: PLR0912, PLR0915` acknowledges it). Untestable per-command, and adding a command means editing a monolith.
- **Proposed fix:** One `cmd_<name>(args)` function per command wired via `set_defaults(func=...)`; `main()` shrinks to parse + dispatch. Mechanical, high-value.
- **Answer:**

### Q12.2: `download`/`import`/`process` never propagate failure to the exit code
- **File:** `brasa/cli.py:915-925, 933-940, 944-959`
- **Severity:** medium
- **Type:** bug
- **Effort (est.):** S
- **Issue:** These commands ignore `report.success`; a run where every download failed still exits 0 — cron/CI can't detect failure. (`run`, `run-all`, `map`, `doctor` do exit 1 correctly.)
- **Proposed fix:** `sys.exit(1)` when any report has failures (perhaps gated so SKIPPED-only runs stay 0).
- **Answer:**

### Q12.3: `query -o` with unsupported extension silently does nothing
- **File:** `brasa/cli.py:1030-1046`
- **Severity:** medium
- **Type:** bug
- **Effort (est.):** S
- **Issue:** `brasa query "..." -o out.txt` matches no branch — no output, no error, exit 0. (`head` handles the same case correctly with an error at `:1095-1097`.)
- **Proposed fix:** Add the same `else: error + exit 1`.
- **Answer:**

### Q12.4: `query` advertises "read-only SQL" but runs on the read-write connection
- **File:** `brasa/cli.py:290-292` (help text) vs `queries.py:48` (`read_only=False`)
- **Severity:** medium
- **Type:** correctness
- **Effort (est.):** S
- **Issue:** `brasa query "DROP VIEW ..."` works. Either the help text or the connection is wrong.
- **Proposed fix:** Open a `read_only=True` DuckDB connection for the query command.
- **Answer:**

### Q12.5: Public API surface: 38 exports including internal report/plan classes
- **File:** `brasa/__init__.py:44+`
- **Severity:** low
- **Type:** api-design
- **Effort (est.):** M
- **Issue:** `ExecutionStep`, `MigrationReport`, `OrchestratorReport`, `RunAllReport`, `DownloadPlanReport`, `TemplateDependencyGraph`… exported at top level pre-1.0 means implicit stability promises for internals. Also mismatched with `queries.__all__` (Q7.10).
- **Proposed fix:** Trim `__all__` to the workflow functions + `get_*` + `BrasaDB`/`sql`; keep internals importable from `brasa.engine` for power users.
- **Answer:**

### Q12.6: `_parse_download_args` calls `sys.exit(1)`; `--output` uses `nargs=1`
- **File:** `brasa/cli.py:836-850`, `:294-300` + `:1018`
- **Severity:** low
- **Type:** cleanup
- **Effort (est.):** S
- **Issue:** Helper exits the process directly (untestable); `nargs=1` yields a list while the default is a str, requiring the `isinstance` dance at `:1018`.
- **Proposed fix:** Raise `argparse.ArgumentTypeError`/`SystemExit` via parser.error; drop `nargs=1`.
- **Answer:** fix — implemented (948b532): `_parse_download_args` raises ValueError (callers translate to stderr + exit 1); `nargs=1` dropped from `sql_query`/`--output`, removing the isinstance dance.

### Q12.7: Parser tree built at module import time
- **File:** `brasa/cli.py:161-594`
- **Severity:** low
- **Type:** refactor
- **Effort (est.):** S
- **Issue:** `import brasa.cli` (e.g. from tests) executes ~430 lines of parser construction as a side effect. Cheap, but it makes the module untestable in isolation and slows any import.
- **Proposed fix:** Wrap in `build_parser()`; folds naturally into Q12.1.
- **Answer:**

### Q12.8: Arg DSL discoverability (`@`, `$`, `~`, comma, auto-int)
- **File:** `brasa/util.py:388-450`, help text at `cli.py:174-183`
- **Severity:** low
- **Type:** docs
- **Effort (est.):** S
- **Issue:** The DSL is powerful and the `--arg` help is decent, but failure modes are quiet: an invalid `@` date raises deep in `DateRangeParser`; the ISO auto-detect swallows parse errors (`util.py:435-436`) and falls through to string, so `2026-02-30` becomes the *string* `"2026-02-30"` silently.
- **Proposed fix:** On auto-detect failure of something that *looked* like a date, error out instead of falling back to string; add a `docs/CLI.md` DSL section with examples (may already exist — verify).
- **Answer:**

---

## Theme 13 — Repo hygiene, packaging & docs

### Q13.1: Root-level clutter: driver scripts, plan YAMLs, analysis docs
- **File:** repo root — `cli.py`, `cli-full.py`, `cli-companies.py`, `cli-ei.py`, `cli-random.py` (personal driver scripts with commented Windows paths), `bvbg086.yaml`, `daily-b3.yaml`, `companies-b3*.yaml`, `equity-options-b3.yaml`, `indexes-b3.yaml` (download plans), `ERRORS.md`, `DEPENDENCY_PROCESSING_ANALYSIS.md`, `DEPENDENCY_VERIFICATION.md`
- **Severity:** medium
- **Type:** cleanup
- **Effort (est.):** S
- **Issue:** The repo root mixes package config with personal run scripts, operational plan files, and one-off analysis writeups. New contributors (and tooling) can't tell what's product vs scratch. Note root `cli.py` shadows `brasa/cli.py` conceptually.
- **Proposed fix:** `plans/` (or `examples/plans/`) for the YAMLs, `examples/` for driver scripts, `docs/analysis/` for the writeups; delete what's stale.
- **Question(s):** Which of the root cli-*.py scripts are still in active personal use?
- **Answer:** fix — implemented: driver scripts → examples/, download plans → examples/plans/, analysis writeups (ERRORS.md, DEPENDENCY_*) → docs/analysis/. Nothing referenced the old paths; scripts kept (none deleted) pending the which-are-active question.

### Q13.2: ~25 MB of binary test data committed at `data/`, plus notebooks with output
- **File:** `data/` (36 tracked files incl. `COTAHIST_A1986.zip` at 8.7 MB), `notebooks/` (45 tracked); pack size 27.7 MiB
- **Severity:** medium
- **Type:** cleanup
- **Effort (est.):** M
- **Issue:** Fixture data lives at root `data/` while tests also reference `tests/data/` (Q63 of the old review, still true); big binaries inflate every clone; notebooks with stored outputs bloat diffs. History rewrite is likely not worth it, but growth should stop.
- **Proposed fix:** Consolidate fixtures under `tests/data/` with a README index of which test uses which file; strip notebook outputs via pre-commit (`nbstripout`); consider Git LFS for future large fixtures.
- **Answer:**

### Q13.3: `docs/ARCHITECTURE.md` describes a structure that no longer exists
- **File:** `docs/ARCHITECTURE.md` (references `brasa/engine.py`, `templates/*.yaml` at old paths); also `docs/QUESTIONS.md`, `docs/SUMMARY.md`, `docs/IDEAS.md` freshness unknown
- **Severity:** medium
- **Type:** docs
- **Effort (est.):** M
- **Issue:** The engine became a package (`brasa/engine/`) with major new subsystems (doctor, orchestrator, download plans, update strategies, catalog) that the architecture doc predates. Docs that contradict the code are worse than no docs — they misdirect both humans and AI assistants.
- **Proposed fix:** Rewrite ARCHITECTURE.md against the current layout (the CLAUDE.md architecture section is accurate and can seed it); date-stamp docs; delete or archive stale ones.
- **Answer:**

### Q13.4: Supersede `docs/QUESTIONS.md` and `~/dev/python/brasa_QUESTIONS.md` with this file
- **File:** `docs/QUESTIONS.md` (88 questions, never answered), external `brasa_QUESTIONS.md` (50 questions, partially stale)
- **Severity:** low
- **Type:** docs
- **Effort (est.):** S
- **Issue:** Three parallel review documents will drift. All still-valid content from both prior files is folded into this one (with re-verified line numbers).
- **Proposed fix:** Delete `docs/QUESTIONS.md` in this branch and note in its place that `QUESTIONS.md` (root) supersedes it; delete the external file.
- **Answer:**

### Q13.5: PyPI/versioning posture: `0.0.1` with this feature surface
- **File:** `pyproject.toml` (`version = "0.0.1"`); docs mention a PyPI release (WIL-76)
- **Severity:** low
- **Type:** packaging
- **Effort (est.):** S
- **Issue:** If it's on PyPI, 0.0.1 undersells and gives no upgrade signal; if not, decide the release cadence. Also no `py.typed` marker, so downstream type-checking of brasa's annotations is off.
- **Proposed fix:** Adopt semver-ish `0.x` bumps per release; add `py.typed` to the wheel.
- **Question(s):** Is brasa published on PyPI today? Who are the consumers besides you?
- **Answer:**

### Q13.6: 15 direct runtime dependencies — candidates for extras
- **File:** `pyproject.toml`
- **Severity:** low
- **Type:** packaging
- **Effort (est.):** M
- **Issue:** `openpyxl`+`xlrd` (Excel export/legacy read), `html5lib`+`beautifulsoup4` (a few HTML readers), `progressbar2` (vs rich, both present) are needed only for specific templates/outputs, yet every install pulls all 390 locked packages' worth of tree.
- **Proposed fix:** Optional extras (`brasa[excel]`, `brasa[html]`); pick one progress library (rich already does progress) and drop the other.
- **Answer:**

### Q13.7: One-off migration scripts accumulating in `scripts/`
- **File:** `scripts/migrate_*.py` (4 cache/DB migration scripts), `wil12_*.py`, `bvbg028_capture_expected_values.py`
- **Severity:** low
- **Type:** cleanup
- **Effort (est.):** M
- **Issue:** Cache schema migrations live as ad-hoc scripts, while `CacheManager.init()` also runs two inline migrations (`_migrate_download_trials`, `_migrate_processed_files`). Two mechanisms, no version stamp in the DB — the next migration will repeat the pattern.
- **Proposed fix:** Add a `schema_version` table + ordered migration list run by `CacheManager.init()`; archive the one-off issue scripts (`wil12_*`) once their issues closed.
- **Answer:**

### Q13.8: `processors/` directory — pre-engine legacy scripts?
- **File:** `processors/pyarrow-1-*.py`, `pyarrow-2-*.py` (5 scripts)
- **Severity:** low
- **Type:** cleanup
- **Effort (est.):** S
- **Issue:** These look like the pre-template exploratory processing scripts (dataset names overlap with today's ETL outputs). If the templates now cover them, they're dead.
- **Proposed fix:** Confirm and delete (or move under `examples/` with a note).
- **Answer:**

---

*End of review. Fill each **Answer**, commit, and hand the file back — themes with at least one `fix` answer become Linear sub-issues under the parent audit issue.*
