# QUESTIONS.md — Comprehensive Code Review

This document contains architectural, refactoring, performance, security, and technical questions found during a full codebase review of the **brasa** project. Each question is independent. Please answer directly below each question to guide improvements.

---

## Table of Contents

1. [Architecture & Design](#1-architecture--design)
2. [Security](#2-security)
3. [Data Integrity & Correctness](#3-data-integrity--correctness)
4. [Performance](#4-performance)
5. [Error Handling & Resilience](#5-error-handling--resilience)
6. [Code Quality & Maintainability](#6-code-quality--maintainability)
7. [Testing](#7-testing)
8. [Templates & Configuration](#8-templates--configuration)
9. [Public API & CLI](#9-public-api--cli)
10. [Dependencies & Packaging](#10-dependencies--packaging)

---

## 1. Architecture & Design

### Q1. Singleton pattern for CacheManager and DatasetCatalog

`CacheManager` and `DatasetCatalog` use a custom `Singleton` base class (`engine/core.py:52-73`) that stores the instance in `cls.__dict__["__it__"]`. This makes testing painful — tests must hack `CacheManager.__it__ = None` to get a fresh instance, and if a test fails mid-reset the singleton is corrupted for subsequent tests. Have you considered replacing this with a simpler pattern (e.g., module-level instance, `functools.lru_cache`, or dependency injection)?

**Answer:**

---

### Q2. CacheManager is a God class (883 lines)

`CacheManager` manages: file I/O, SQLite metadata persistence, download folder creation, parquet writing, schema registration, checksum verification, and meta cleanup. Have you considered splitting responsibilities into smaller classes (e.g., `CacheMetadataStore` for DB ops, `CacheFileManager` for file I/O, `SchemaRegistry` for schema ops)?

**Answer:**

---

### Q3. Module-level template cache with no invalidation

`template.py` uses a module-level `_template_cache` dict that persists for the process lifetime. There's no TTL, no invalidation on file changes, and no scoping to a CacheManager instance. Two CacheManager instances with different `BRASA_DATA_PATH` values could share stale template entries. Is the assumption that `BRASA_DATA_PATH` never changes within a process? Should the cache be attached to the CacheManager instead?

**Answer:**

---

### Q4. BrasaDB uses a class-level singleton connection

`queries.py:38-55`: `BrasaDB.connection` is a class variable holding a single DuckDB connection. This is not thread-safe — concurrent calls to `get_connection()` could race on the health check/reconnect. Is BrasaDB intended for single-threaded use only? If so, should this be documented? If not, should we add a threading lock or connection pool?

**Answer:**

---

### Q5. Tight coupling between Template, CacheManager, and readers

`retrieve_template()` is imported directly in many modules. `MarketDataReader.read()` requires a full `CacheMetadata` object. This makes unit testing difficult without a full cache setup. Would you be open to introducing a lightweight interface/Protocol for template retrieval and reader inputs to improve testability?

**Answer:**

---

### Q6. No downloader abstraction / interface

There's only one downloader implementation (`MarketDataDownloader`). There's no Protocol or ABC defining the downloader contract. If you ever need FTP, S3, or custom API downloaders, the current design requires modifying the existing class. Would it be useful to extract a `Downloader` Protocol?

**Answer:**

---

### Q7. Pipeline step isolation

`pipeline/executor.py`: All steps in a ReaderPipeline share the same `PipelineContext`. Steps can mutate `intermediate_results`, potentially affecting later steps in unexpected ways. Is this shared-state-by-design (for step communication), or would you prefer per-step isolation with explicit data passing?

**Answer:**

---

### Q8. ETL pipeline has no transaction/checkpoint semantics

`pipeline/etl_executor.py:131-170`: If the output write fails after all transformation steps complete, the computation is lost. For expensive ETL pipelines, would checkpoint/recovery or write-ahead logging be valuable?

**Answer:**

---

### Q9. Legacy vs modern template coexistence

The codebase supports both legacy templates (with `reader.function` and `handler`-based fields) and modern templates (with `reader.pipeline` and `type`-based fields). This creates two code paths in `processing.py`, `template.py`, and throughout the reader system. What's the plan for deprecating legacy templates? Is there a timeline?

**Answer:**

---

### Q10. etl.py is 1251 lines of individual handler functions

`etl.py` contains ~30 individual ETL functions (`create_b3_rate_futures`, `create_b3_equities_returns`, etc.) with highly repeated patterns: load dataset → transform → write. This is a God module. Is the plan to migrate all of these to pipeline-based ETL templates? If so, which ones are candidates for near-term migration?

**Answer:**

---

### Q11. Deprecated function `create_b3_listed_funds` still fully implemented

`etl.py:896-933`: This function is marked `@deprecated` with a deprecation warning, but it still has the full 30+ line implementation. Is it still called by any template? Can it be removed, or does removing it break backward compatibility?

**Answer:**

---

### Q12. `load_function_by_name()` allows loading any Python function

`core.py:37-49` dynamically loads any Python function by fully-qualified name using `__import__`. This is used for legacy template `function:` fields. If templates ever come from an untrusted source, this is an arbitrary code execution risk. Are templates always trusted (authored by you)? Would a whitelist of allowed modules be prudent?

**Answer:**

---

### Q13. DataLayer enum vs string-based layers

`layers.py` defines a `DataLayer` enum (RAW, INPUT, STAGING, CURATED), but many parts of the codebase use plain strings like `"input"`, `"staging"`. Is the intent to migrate everything to use the enum? Or are strings the preferred interface?

**Answer:**

---

---

## 2. Security

### Q14. Zip path traversal in `unzip_file_to()`

`util.py:54-61`: `zf.extract(name, dest)` is called without validating that extracted paths stay within `dest`. A crafted zip with entries like `../../../etc/passwd` would extract outside the destination directory. This is a known zipfile vulnerability (ZipSlip). Since you download zip files from external B3 URLs, should we add path validation?

**Answer:**

---

### Q15. Unbounded recursion in `unzip_recursive()`

`util.py:69-77`: This function recursively extracts zip-in-zip files with no depth limit and no cycle detection. A pathological archive (zip containing itself) would cause infinite recursion and crash with a stack overflow. It also leaks temp files (each extracted level goes to `gettempdir()` but is never cleaned). Should we add a max depth and cleanup?

**Answer:**

---

### Q16. MD5 used for checksums

`util.py:43` uses `hashlib.md5(pickle.dumps(obj))` for template checksums, and `util.py:47-51` uses MD5 for file checksums. MD5 is cryptographically broken. While this isn't being used for security (just deduplication), is there a reason not to use SHA-256 instead? Also, using `pickle.dumps` for hashing is fragile — pickle output can vary between Python versions.

**Answer:**

---

### Q17. SQL executed directly from template config in `execute_query()`

`etl.py` has an `execute_query()` function that runs `handler.query` directly against DuckDB without parameterization. The query string comes from template YAML. Similarly, `dependency_resolver.py` executes SQL from dependency configs. Are templates the only source of these queries? Is YAML considered a trusted input?

**Answer:**

---

### Q18. Credential leakage in download retry logs

`template.py:516-524`: Download retry error messages are logged with full exception strings, which could include API keys embedded in URLs (e.g., `?key=...`). Should exception messages be sanitized before logging?

**Answer:**

---

### Q19. No YAML structure validation on template load

Templates are loaded with `yaml.safe_load()` (good — no arbitrary code exec), but there's no schema validation of the loaded dict structure. A template with typos in field names (e.g., `reader.encodng` instead of `reader.encoding`) is silently ignored. Would adding JSON Schema or Pydantic validation for templates be worthwhile?

**Answer:**

---

---

## 3. Data Integrity & Correctness

### Q20. Bitwise OR instead of logical OR in `unified_reader.py:103`

```python
is_filepath = isinstance(filepath_or_buffer, str) | isinstance(filepath_or_buffer, Path)
```

This uses `|` (bitwise) instead of `or` (logical). It works by accident (True/False are 0/1 in Python), but returns `int` instead of `bool`, which could fail with `is True` comparisons. Is this intentional or a bug?

**Answer:**

---

### Q21. Type mismatch in `readers/helpers.py:397`

```python
adapter = PandasAdapter(template.fields, errors="coerce")
```

`template.fields` is a `TemplateFields` object, but `PandasAdapter.__init__` expects a `Fieldset`. Line 303-305 of the same file shows the correct pattern: `Fieldset.from_template_fields(template.fields, ...)`. Is this a bug? Is this code path ever hit?

**Answer:**

---

### Q22. Dead bytes check in FWF parser

`parsers/fwf.py:140-141`: When opening a file in text mode (`Path.open(encoding=...)`), lines are always strings, never bytes. The `isinstance(line, bytes)` check is dead code. If the branch were ever entered, `_line` would be used before assignment (NameError). Should this be cleaned up?

**Answer:**

---

### Q23. `dict(zip(..., strict=False))` silently drops data

Several places use `dict(zip(headers, values, strict=False))` (e.g., `parsers/util.py:54`, `parsers/fwf.py`). If there's a column count mismatch between header and data row, extra values are silently dropped. For financial data parsing, is silent truncation acceptable, or should this raise an error?

**Answer:**

---

### Q24. Float precision loss in PyArrow decimal casting

`pyarrow_adapter.py:374-395`: Decimal columns are cast through `float64` intermediate, which has only ~15 significant digits. For financial data with high precision (e.g., BRL rates with 8+ decimal places), this can introduce rounding errors. Should this use string-based conversion instead?

**Answer:**

---

### Q25. `idxmax()` semantics in `ffill_n_remove_duplicates`

`etl.py:1026-1035`: `ix.idxmax()` on a boolean Series returns the index of the **first** True value, not the maximum. The function name says "ffill" (forward-fill), but the implementation takes the first non-NaN value, not the last. Is this the intended behavior?

**Answer:**

---

### Q26. No validation that `start <= end` in query functions

`queries.py:385-387`: `get_returns()` and `get_prices()` accept `start` and `end` datetime parameters but never check that `start <= end`. Reversed dates silently return empty results. Should this validate and warn/error?

**Answer:**

---

### Q27. Empty dataset edge cases

Multiple functions (`get_returns`, `get_prices`, `_get_indexes_names`, `_get_equity_symbols`) call `pyarrow.compute.max(tb.column(...))` or `df.index[0]` without checking if the table/DataFrame is empty. These would return `null` or raise `IndexError`. Should there be guards for empty datasets?

**Answer:**

---

### Q28. `describe()` assumes pandas metadata exists in parquet

`queries.py:789-792`: `describe()` reads `schema.metadata[b"pandas"]` from parquet files. If a parquet file was created without pandas metadata (e.g., by PyArrow directly), this raises `KeyError`. Is this a realistic scenario in brasa?

**Answer:**

---

### Q29. Partition column type conflicts not detected

`queries.py:583-591`: When adding partition columns to a schema, the code only checks if the column name exists, not if the type matches. A partition column with `string` type in the schema but `date` in the partitioning would cause silent type coercion. Is this a concern?

**Answer:**

---

---

## 4. Performance

### Q30. Recursive file mtime scan for dependency staleness

`dependency_resolver.py:49-51`: `_get_latest_mtime()` calls `rglob("*")` to find the most recent file modification time in a directory. For datasets with thousands of parquet partitions, this is a full directory scan on every staleness check. Would a marker file (written on dataset update) be more efficient?

**Answer:**

---

### Q31. Template lookup does `rglob()` on every cache miss

`template.py:730-749`: `retrieve_template()` calls `rglob(f"{template_name}.yaml")` to find template files when not in cache. With 112+ templates across nested directories, this is a full filesystem scan per miss. Should we build an in-memory index at startup?

**Answer:**

---

### Q32. Repeated dataset loads in `get_symbols()` family

`queries.py:858-998`: Each call to `get_symbols("equity")`, `get_symbols("etf")`, etc. loads the underlying PyArrow dataset from scratch. In CLI batch operations, the same dataset may be loaded dozens of times. Should there be a session-level dataset cache?

**Answer:**

---

### Q33. Pandas conversions when PyArrow would suffice

`etl.py` and `queries.py` frequently do `.to_table().to_pandas()` when operations could stay in PyArrow. For example, `etl.py:558-564` loads a PyArrow table, converts to pandas just to get unique symbols, then discards the DataFrame. Should we prefer staying in PyArrow until the final output?

**Answer:**

---

### Q34. Repeated glob for parquet files in view creation

`queries.py:186-196`: `create_all_views()` calls `_create_single_view()` for each dataset, which internally globs for `**/*.parquet` files. With 42 datasets and potentially thousands of partitions each, this is expensive. Could we cache the file list or use PyArrow dataset discovery?

**Answer:**

---

### Q35. SQLite lock contention in multi-threaded download

`api.py:427-428`: `process_marketdata()` uses `max_workers=4` with a `db_lock` for SQLite writes. SQLite has limited concurrent write support, so 4 workers competing for one lock degrades to near-serial execution for the DB portion. Is multi-threaded download actually faster than sequential, given the lock? Would batching writes help?

**Answer:**

---

### Q36. No schema caching in `_get_schema_from_fields()`

`processing.py:125-140`: `_get_schema_from_fields()` creates a PyArrow schema from template fields on every call, with no caching. For templates processed repeatedly (multi-date downloads), this is redundant. Should schemas be cached per template?

**Answer:**

---

### Q37. `apply(lambda ...)` instead of vectorized operations

`etl.py:670,943,952` use `df[col].apply(lambda x: re.sub(...))` for regex operations. Pandas `.str.replace()` is vectorized and significantly faster. Same pattern appears with other string operations. Should these be migrated to vectorized ops?

**Answer:**

---

---

## 5. Error Handling & Resilience

### Q38. Silent failures: `list_tables()` returns `[]` on error

`queries.py:283`: `list_tables()` catches all exceptions and returns an empty list. The caller can't distinguish "no tables exist" from "database is corrupt/unreachable." Should this at minimum log the error?

**Answer:**

---

### Q39. `_create_single_view()` truncates error messages to 100 chars

`queries.py:149`: Error messages are truncated with `str(e)[:100]`. For complex SQL errors or file path issues, this loses critical diagnostic information. Is there a reason for this truncation?

**Answer:**

---

### Q40. `_get_schema_from_fields()` silently returns None on failure

`processing.py:125-140`: If schema generation fails, this catches all exceptions and returns `None`. Data is then written without schema validation. No warning is logged. Should this at least emit a warning?

**Answer:**

---

### Q41. Generic `raise Exception("empty zip file")` in download.py

`download.py:47`: `raise Exception(...)` instead of a specific exception type. This bypasses the custom exception hierarchy (`DownloadException`, `InvalidContentException`). Should this use a specific exception?

**Answer:**

---

### Q42. Inconsistent error handling across `get_*()` query functions

| Function | Error Handling |
|---|---|
| `get_returns()` | Crashes on invalid input |
| `get_prices()` | Crashes on invalid input |
| `get_symbols()` | Returns `[]` for unknown types |
| `get_industry_sectors()` | Crashes on error |
| `describe_dataset()` | Raises `ValueError` |

There's no consistent strategy. Should all functions raise on invalid input? Or all return empty/None? What's the intended contract?

**Answer:**

---

### Q43. No jitter in exponential backoff for retries

`template.py:421-545`: The retry system uses exponential backoff but with no jitter. If multiple downloads fail simultaneously (e.g., during a batch of 100 dates against B3), all retry at the same time, causing a thundering herd. Should we add random jitter?

**Answer:**

---

### Q44. Retry info is lost on final failure

`template.py`: When all retry attempts fail, the exception is raised but retry metadata (which attempts were tried, which status codes were seen) is lost. The caller only gets the final exception. Should retry info be attached to the exception?

**Answer:**

---

### Q45. `clean_meta_db_folder()` is a no-op

`cache.py:528`: This method contains only `pass` with a comment about partitioned datasets. It's called from `remove_meta()`. Is this intentionally a placeholder for future logic, or should it be removed?

**Answer:**

---

---

## 6. Code Quality & Maintainability

### Q46. Commented-out `get_timeseries()` function (29 lines)

`queries.py:343-371`: A fully commented-out function left in the file with no explanation. Was this superseded by `get_returns()`? Should it be removed (it's in git history if needed)?

**Answer:**

---

### Q47. `json_convert_to_object()` regex matches too broadly

`core.py:30-33`: The date regex `\d{4}-\d{2}-\d{2}` matches any dict value that looks like a date string and converts it to `datetime`. This could unintentionally convert non-date strings like version numbers (`"2024-01-01"` as a version). Is this used only for deserialization of known-date fields, or could it hit arbitrary dict values?

**Answer:**

---

### Q48. `SuppressUserWarnings` context manager doesn't restore properly

`util.py:18-23`: Uses `warnings.filterwarnings("default")` on exit, which resets to default rather than restoring the previous state. If user code had custom warning filters, they'd be lost. Is this context manager actually used? Should it use `warnings.catch_warnings()` instead?

**Answer:**

---

### Q49. Hardcoded field/schema definitions in etl.py

Throughout `etl.py` (lines 156-160, 290-296, 370-376, etc.), field names and schemas are hardcoded in Python functions rather than in template YAML configs. This means schema changes require code changes, not just config changes. Is this a temporary state while migrating to pipeline-based ETL, or is it the intended pattern?

**Answer:**

---

### Q50. Duplicate temp_cache fixture across test files

At least 4 test files (`test_cache.py`, `test_download_status.py`, `test_invalid_downloads.py`, `test_download_retry.py`) independently create `temp_cache` fixtures that reset the CacheManager singleton. This should be centralized in `conftest.py`. Is there a reason these are duplicated?

**Answer:**

---

### Q51. `queries.py` exports differ between `__all__` and `__init__.py`

`queries.py:15-34` has `__all__` including `get_template_dataset`, `get_template_layer`, `get_template_partitioning`, `get_template_schema` — but `__init__.py` doesn't re-export all of these. Is this intentional? Which is the canonical public API?

**Answer:**

---

### Q52. `nargs=1` makes `args.output` a list, but default is a string

`cli.py:852,878`: `add_argument("-o", "--output", nargs=1, default="display")` — with `nargs=1`, the parsed value is always a list, but the default `"display"` is a string. The code then has `args.output[0] if isinstance(args.output, list) else args.output` to handle both. Should this just use `nargs=None` (default) or `type=str`?

**Answer:**

---

### Q53. `sys.exit(1)` in utility function

`cli.py:704-718`: `_parse_download_args()` calls `sys.exit(1)` directly on invalid input. This makes the function untestable and unusable outside CLI context. Should it raise an exception instead?

**Answer:**

---

---

## 7. Testing

### Q54. Zero test coverage for queries.py (998 lines)

The entire query module — `BrasaDB`, `create_all_views`, `get_prices`, `get_returns`, `get_symbols`, `describe_dataset`, etc. — has no unit tests. This is the primary public API for data consumers. What's the barrier to testing this? Is it the DuckDB dependency? Would in-memory DuckDB fixtures help?

**Answer:**

---

### Q55. Zero test coverage for etl.py (1251 lines)

All 30+ ETL handler functions are untested. Some have complex logic (join, filter, aggregate, pivot). Since these transform financial data, correctness is critical. What's the testing strategy here?

**Answer:**

---

### Q56. Zero test coverage for readers/helpers.py (675 lines) and readers/csv.py

These modules do the actual data parsing — the most critical correctness path. No tests exist. What's the barrier?

**Answer:**

---

### Q57. Zero test coverage for 12 parser modules

All parsers in `parsers/b3/` (bvbg028, bvbg086, cdi, indic, stock_indexes, futures_settlement_prices, cotahist), `parsers/anbima/` (debentures, tpf), `parsers/td.py`, `parsers/fwf.py`, `parsers/util.py` are untested (except bvbg087). Are these parsers considered stable/legacy, or would tests catch real issues?

**Answer:**

---

### Q58. Integration tests depend on real B3 APIs with `time.sleep(5)`

`test_downloads.py`: 4 integration tests make real HTTP calls to B3/BMFBOVESPA endpoints with hardcoded `time.sleep(5)` between them. These are slow, flaky, and depend on external availability. Would you accept test doubles (VCR cassettes, `responses` library, or mock fixtures) for the default test suite, with real integration tests as a separate opt-in suite?

**Answer:**

---

### Q59. 7 permanently skipped tests

These tests are skipped due to broken external endpoints or missing resources:
- `test_get_marketdata` — www2.bmf.com.br issue
- `test_metadata_fulfilment` — SGS endpoint unstable
- `test_download_settlement_prices` — resource no longer available
- `test_brasa_companies_pipeline_execution` — requires external datalake
- (and 3 more)

Should these be removed, converted to mock-based tests, or kept as documentation of known issues?

**Answer:**

---

### Q60. Over-mocking in large test files

`test_dependency_graph.py` (1763 lines) and `test_orchestrator.py` (785 lines) use `MagicMock` for nearly all objects. If `MarketDataTemplate` changes its constructor signature, these tests won't catch the breakage. Are you comfortable with this level of mocking, or should some tests use real objects?

**Answer:**

---

### Q61. No pytest-cov, no coverage thresholds

There's no coverage tool configured. Adding `pytest-cov` with a minimum threshold (even 40-50% initially) would prevent coverage regressions. Would you like this added?

**Answer:**

---

### Q62. No test timeouts configured

Tests that make HTTP calls or read large files could hang indefinitely. `pytest-timeout` is not configured. Should we add a default timeout (e.g., 30s per test)?

**Answer:**

---

### Q63. No shared test data directory

Test data (CSV samples, parquet fixtures, zip files) is scattered or generated inline. `test_bvbg087_parser.py` references `tests/data/IR210423.zip` but there's no organized `tests/data/` or `tests/fixtures/` directory. Would a structured test data directory be useful?

**Answer:**

---

### Q64. Pipeline steps have zero unit tests

All pipeline steps in `engine/pipeline/steps/` (b3_steps, column_steps, custom_steps, etl_steps, html_steps, io_steps, transform_steps, shared_transforms) — totaling ~2000 lines — have no dedicated unit tests. `test_pipeline.py` (159 lines) only tests registry/construction. Should steps have individual unit tests?

**Answer:**

---

---

## 8. Templates & Configuration

### Q65. Three different field definition formats coexist

Templates use three formats:
1. **Legacy handler block**: `handler: { type: Date, format: "%Y-%m-%d" }`
2. **Modern type string**: `type: date(format='%Y%m%d')`
3. **Mixed** (some templates have both)

This creates parser ambiguity and maintenance burden. Is there a migration plan to standardize on format 2?

**Answer:**

---

### Q66. No template schema validation

A template with `reader.encodng: "utf-8"` (typo) silently uses the default encoding. There's no validation that template YAML contains only known keys with valid values. Would JSON Schema or Pydantic validation be worth the effort?

**Answer:**

---

### Q67. Template naming inconsistencies

- Modern templates use kebab-case: `b3-cotahist-daily`
- Legacy templates use PascalCase: `PremioOpcaoAcao`, `CenariosSpot`
- Some legacy use ALLCAPS: `BDIN`, `FPR`

No test validates naming conventions. Should naming be standardized? Does the CLI handle all formats correctly?

**Answer:**

---

### Q68. Template ID validation happens after full load

`template.py:755-759`: The template ID is checked against the filename after the entire YAML is parsed and the `MarketDataTemplate` is constructed. For large templates, this is wasted work. Should we validate the ID first (read just the `id:` field)?

**Answer:**

---

### Q69. Legacy templates in `brasa/files/templates/b3/companies/legacy/` — what's their status?

There are 6 legacy templates that use `etl.function: brasa.etl.create_*` patterns. Are these actively used in production? Can they be migrated to pipeline-based ETL? Is there a compatibility risk in removing them?

**Answer:**

---

### Q70. Default ETL layer logic is implicit

`template.py:644-649`: ETL templates default to STAGING, others to INPUT. This is hardcoded with no documentation in the method. Should this be made explicit (required field in template YAML) rather than implicit?

**Answer:**

---

### Q71. `writer` section is auto-created if missing

`template.py:635-638`: If a template has no `writer:` section, a default `MarketDataWriter()` is created. What defaults does this use? Could the defaults be wrong for specific templates? Should `writer` be required?

**Answer:**

---

---

## 9. Public API & CLI

### Q72. Large public API surface (38 exports)

`__init__.py` exports 38 symbols, including internal engine classes like `ExecutionPlan`, `ExecutionStep`, `MigrationReport`, `OrchestratorReport`. Are all of these intended for end-user consumption? Could the public API be smaller (just the core workflow functions)?

**Answer:**

---

### Q73. `get_symbols()` returns `[]` for unknown type

`queries.py:966-998`: If you call `get_symbols("invalid_type")`, it silently returns an empty list. The function also accepts `**kwargs` but silently ignores unknown keys. Should unknown types raise `ValueError`?

**Answer:**

---

### Q74. `write_dataset()` has no input validation

`queries.py:800-856`: No check that `df` is non-empty, `name` is a valid identifier, `format` is supported, or `schema` matches the DataFrame. Should there be validation?

**Answer:**

---

### Q75. `describe()` function assumes pandas metadata

`queries.py:789-792`: `describe()` reads `schema.metadata[b"pandas"]` — fails if parquet was written without pandas metadata. The newer `describe_dataset()` function seems more robust. Should `describe()` be deprecated in favor of `describe_dataset()`?

**Answer:**

---

### Q76. No timezone handling in date parameters

Throughout `queries.py` and `etl.py`, datetime defaults use naive datetimes (`datetime(2000, 1, 1)`, `datetime.today()`). If data is stored with UTC timezone, naive datetime filters would cause silent mismatches. Is all data timezone-naive by design?

**Answer:**

---

### Q77. CLI `download` command error output goes to stderr but exit code is 0

Does the CLI consistently return non-zero exit codes on failure? For scripting/CI integration, this matters. Some paths use `sys.exit(1)`, others may not.

**Answer:**

---

### Q78. CLI graphviz rendering with unvalidated output path

`cli.py:1133-1143`: The `output_file` from CLI args is passed to `subprocess.run(["dot", ... "-o", output_file])`. While list-form subprocess prevents shell injection, the file path itself is not validated. Could a user accidentally overwrite an important file?

**Answer:**

---

---

## 10. Dependencies & Packaging

### Q79. `requests` is used but not in dependencies

The downloaders use HTTP requests, but `requests` is not listed in `pyproject.toml` dependencies. Is it pulled in transitively through `python-bcb`? Should it be an explicit dependency?

**Answer:**

---

### Q80. Heavy dependency tree for a data library

The project requires: lxml, pandas, numpy, xlrd, pyarrow, duckdb, beautifulsoup4, html5lib, rich, openpyxl, python-bcb, progressbar2, bizdays, regexparser, pyyaml. That's 15 direct dependencies. Are all of them needed for core functionality? Could some (e.g., `rich`, `progressbar2`, `openpyxl`, `html5lib`) be optional extras?

**Answer:**

---

### Q81. Version 0.0.1 with MIT license

The project is at `0.0.1` — is this intended for public release on PyPI? Are there API stability guarantees? The version suggests pre-alpha, but the feature set is extensive.

**Answer:**

---

### Q82. Python 3.10+ target but no CI configuration visible

`pyproject.toml` targets `py310+`, but there's no CI configuration (GitHub Actions, etc.) in the repo. Is CI run elsewhere? Should we add a basic CI workflow for automated testing?

**Answer:**

---

### Q83. `pytest>=7.1.3,<8.0.0` — why pinned below 8?

pytest 8.x has been stable for over a year. Is there a known incompatibility, or can this constraint be relaxed?

**Answer:**

---

### Q84. No `py.typed` marker for type checking consumers

If downstream projects want to type-check code using brasa, they need a `py.typed` marker file. Is this on the roadmap?

**Answer:**

---

---

## Bonus: Quick Wins

### Q85. `unzip_file_to()` doesn't use context manager

`util.py:54-61`: `zf = zipfile.ZipFile(fname)` ... `zf.close()` — should use `with` statement. Same issue with `unzip_and_get_content()` at line 80-90.

**Answer:**

---

### Q86. `_is_zip()` checks `isinstance(fname, str)` before `zipfile.is_zipfile()`

`util.py:64-66`: `_is_zip()` rejects Path objects. Since `zipfile.is_zipfile()` accepts both `str` and `Path`, the isinstance check unnecessarily restricts the input type.

**Answer:**

---

### Q87. Inconsistent use of `Path` vs `str` for file paths

Some functions accept `str`, others `Path`, some both. There's no consistent convention. Should we standardize on `Path` internally and accept `str | Path` at public API boundaries?

**Answer:**

---

### Q88. `download.py:47` raises bare `Exception`

`raise Exception("Market data download failed: empty zip file")` should be `raise InvalidContentException(...)` or similar, to fit the exception hierarchy.

**Answer:**

---

---

*End of review. Please answer each question directly below the "Answer:" marker. Your answers will guide the improvement plan.*
