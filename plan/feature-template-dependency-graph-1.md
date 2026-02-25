---
goal: Implement a template dependency graph for automatic upstream processing in ETL pipelines
version: 1.0
date_created: 2025-02-25
last_updated: 2025-02-25
owner: brasa-team
status: 'In Progress'
tags: ['feature', 'architecture', 'etl', 'dependency-graph', 'orchestration']
---

# Introduction

![Status: In Progress](https://img.shields.io/badge/status-In%20Progress-yellow)

This plan implements a **template dependency graph** that enables automatic upstream processing when running ETL templates. Currently, calling `brasa.process_etl("b3-equities-register")` only executes that single ETL template. If its upstream dependency (`input.b3-bvbg028-equities`, produced by the download template `b3-bvbg028`) has unprocessed files, the user must manually process them first.

The goal is to make `process_etl()` automatically:
1. Discover the full dependency chain for a template
2. Detect which upstream templates have unprocessed data
3. Execute upstream templates in topological order
4. Finally execute the target template

This transforms brasa from a manual step-by-step ETL tool into an intelligent pipeline orchestrator.

### Dependency Model

Templates in brasa form a **directed acyclic graph (DAG)** where:

- **Source nodes** (download templates): Templates with `downloader:` + `reader:` sections that produce datasets in the `input` layer. They have no upstream template dependencies.
  - Single-output: `b3-cotahist-daily` → `input/b3-cotahist-daily`
  - Multi-output: `b3-bvbg028` → `input/b3-bvbg028-equities`, `input/b3-bvbg028-options_on_equities`, `input/b3-bvbg028-future_contracts`, etc.

- **ETL nodes** (transform templates): Templates with `etl:` section that consume datasets and produce derived datasets.
  - Pipeline-based: `b3-equities-register` loads `input.b3-bvbg028-equities` → `staging/b3-equities-register`
  - Function-based (legacy): `b3-futures-dol` loads `b3-futures-settlement-prices` → `staging/b3-futures-dol`

**Scope: pipeline-based templates only.** Only templates that declare `etl.pipeline` or `reader.pipeline` are included in the dependency graph. Legacy function-based templates (those using `etl.function` or `reader.function`) are excluded — their dependency patterns are too varied and implicit to discover reliably.

Dependency discovery handles these pipeline patterns:

| Pattern | Example Template | Dependency Discovery Method |
|---------|-----------------|----------------------------|
| Pipeline `load` step | `b3-futures` | `step.params["input"]` in `load` step |
| Pipeline `concat_datasets` step | `b3-cotahist` | `step.params["inputs"]` in `concat_datasets` step |
| Pipeline `sql_query` step | `b3-equities-register` | `step.params["datasets"]` in `sql_query` step |

### Dataset-to-Template Reverse Index

A critical component is the **reverse index** that maps dataset names back to their producing templates:

- `input/b3-bvbg028-equities` → produced by template `b3-bvbg028` (multi-output)
- `input/b3-cotahist-daily` → produced by template `b3-cotahist-daily` (single-output)
- `staging/b3-equities-register` → produced by template `b3-equities-register` (ETL)
- `staging/b3-cotahist` → produced by template `b3-cotahist` (ETL)

For multi-output templates, the output dataset name follows the pattern: `{template-id}-{dataset-key}` (e.g., `b3-bvbg028-equities` from template `b3-bvbg028`, dataset key `equities`).

## 1. Requirements & Constraints

- **REQ-001**: Build a complete dependency graph from all templates by scanning YAML configurations
- **REQ-002**: Support dependency discovery from pipeline-based ETL steps (`load`, `concat_datasets`, `sql_query`)
- **REQ-003**: Only include templates that use `etl.pipeline` or `reader.pipeline`. Legacy function-based templates (`etl.function`, `reader.function`) are excluded from the graph
- **REQ-004**: Build a reverse index mapping dataset names (with layer prefix) to their producing template
- **REQ-005**: Handle multi-output download templates where one template produces multiple datasets (e.g., `b3-bvbg028` → `input/b3-bvbg028-equities`, `input/b3-bvbg028-future_contracts`)
- **REQ-006**: Perform topological sort to determine correct execution order
- **REQ-007**: Detect circular dependencies and raise clear errors
- **REQ-008**: Determine "staleness" of upstream datasets — detect when download templates have unprocessed raw files
- **REQ-009**: Enhanced `process_etl()` must automatically process stale upstream dependencies before running the target
- **REQ-010**: Support `force` mode to re-execute all upstream templates regardless of staleness
- **REQ-011**: Provide a `dry_run` mode that reports planned execution order without executing
- **REQ-012**: Expose the dependency graph via public API functions (`get_dependency_graph()`, `get_execution_plan()`)
- **REQ-013**: Backward compatibility: existing `process_etl(template_name)` calls must continue working unchanged when no upstream work is needed
- **SEC-001**: No external dependencies beyond what is already in `pyproject.toml`
- **CON-001**: Only pipeline-based templates (`etl.pipeline` or `reader.pipeline`) are in scope. Legacy function-based templates are ignored during graph construction
- **CON-002**: Must handle dataset references in both `layer.dataset` format (e.g., `input.b3-bvbg028-equities`) and bare dataset names (e.g., `b3-futures-settlement-prices`)
- **CON-003**: Multi-output templates produce datasets named `{template-id}-{dataset-key}` in the `input` layer
- **CON-004**: Download templates require `download_marketdata()` + `process_marketdata()` to refresh data; ETL templates require `process_etl()`
- **CON-005**: Must not modify existing YAML template schemas — all metadata is derived from existing template fields
- **GUD-001**: Follow existing code patterns (dataclasses, type hints, Google-style docstrings)
- **GUD-002**: Use `logging` module for progress reporting during multi-step execution
- **GUD-003**: Graph operations should be cacheable and invalidated when templates change
- **PAT-001**: Follow the Singleton pattern used by `CacheManager` and `DatasetCatalog`
- **PAT-002**: Follow the `TaskReport` / `TaskResult` reporting pattern used by existing API functions

## 2. Implementation Steps

### Implementation Phase 1: Dependency Discovery & Graph Construction

- GOAL-001: Create the core dependency graph module that scans all templates and builds a DAG with reverse index
- **Status: COMPLETED** (2025-02-25)
- **Results**: Graph discovers 37 pipeline-based templates, 44 datasets, and 24 dependency edges from the real template files. 46 unit tests all passing.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-001 | Create `brasa/engine/dependency_graph.py` with `TemplateDependencyGraph` class. Constructor scans all templates via `list_templates()` + `retrieve_template()`, filtering to only those with `reader.has_pipeline` (download templates) or `etl.is_pipeline` (ETL templates). Legacy function-based templates are skipped. Must handle load errors gracefully (log warning, skip broken templates). | ✅ | 2025-02-25 |
| TASK-002 | Implement `_discover_outputs(template) -> list[DatasetOutput]` method. For download templates: single-output uses `writer.layer.value + "/" + writer.dataset`; multi-output iterates `template.datasets` and produces `writer.layer.value + "/" + template.id + "-" + dataset_key`. For ETL templates: uses `writer.layer.value + "/" + writer.dataset`. Return `DatasetOutput(dataset_id, layer, dataset_name, template_id)` dataclass instances. | ✅ | 2025-02-25 |
| TASK-003 | Implement `_discover_dependencies(template) -> list[str]` method. For pipeline ETL: call `template.etl.pipeline.get_input_datasets()` which aggregates `step.get_input_datasets()` across all steps. For pipeline readers (download templates): no upstream template dependencies (source nodes). Return list of dataset reference strings. Legacy function-based templates are not processed (filtered out in TASK-001). | ✅ | 2025-02-25 |
| TASK-004 | Implement `_normalize_dataset_ref(ref: str) -> tuple[str, str]` to parse dataset references. Handle `"layer.dataset-name"` → `(layer, dataset-name)` and bare `"dataset-name"` → resolve layer by checking template writer config or defaulting to `input`. | ✅ | 2025-02-25 |
| TASK-005 | Build the reverse index `dict[str, str]` mapping `"layer/dataset-name"` → `template_id`. Populate during graph construction. Raise `ValueError` for duplicate dataset producers. | ✅ | 2025-02-25 |
| TASK-006 | Implement `_build_template_edges()` method that connects ETL template dependencies to their producing templates via the reverse index. Result: `dict[str, list[str]]` mapping `template_id` → `[upstream_template_ids]`. | ✅ | 2025-02-25 |
| TASK-007 | Write unit tests in `tests/test_dependency_graph.py`: test output discovery for single-output, multi-output, and ETL templates; test dependency discovery for `load`, `concat_datasets`, `sql_query` pipeline steps; test that legacy function-based templates are excluded from the graph; test reverse index construction; test edge cases (missing templates, unknown datasets). | ✅ | 2025-02-25 |

**Implementation notes (Phase 1):**

- **Bug fix**: Added `ConcatDatasetsStep.get_input_datasets()` override in `brasa/engine/pipeline/steps/etl_steps.py`. The base `PipelineStep.get_input_datasets()` only checked `params["input"]` (singular) but `ConcatDatasetsStep` uses `params["inputs"]` (plural), so concat dependencies were not being reported.
- **Additional public helpers**: `get_upstream()`, `get_downstream()`, `get_template_type()`, `get_outputs()`, `get_producer()`, `template_ids`, `__len__`, `__contains__` were added beyond the plan scope to support downstream phases.
- **Verified dependency chains**: `b3-bvbg028` → `b3-equities-register` → `b3-equities-spot-market`, `b3-cotahist-yearly` + `b3-cotahist-daily` → `b3-cotahist`, and 14 other edge relationships confirmed correct against real templates.

### Implementation Phase 2: Topological Sort & Cycle Detection

- GOAL-002: Implement topological sort for execution ordering and cycle detection

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-008 | Implement `topological_sort(template_id: str) -> list[str]` method using Kahn\'s algorithm (BFS-based). Takes a target template and returns the ordered list of all templates that must be processed, starting from source templates (no upstream deps) down to the target. Only includes ancestors of the target, not the entire graph. | | |
| TASK-009 | Implement `detect_cycles() -> list[list[str]]` method using DFS-based cycle detection. Returns list of cycles found, each cycle as a list of template IDs. Called during graph construction to validate integrity. | | |
| TASK-010 | Implement `get_ancestors(template_id: str) -> set[str]` method that returns all transitive upstream templates. Used to scope topological sort to only relevant subgraph. | | |
| TASK-011 | Implement `get_descendants(template_id: str) -> set[str]` method that returns all downstream templates. Useful for understanding impact of changes. | | |
| TASK-012 | Write unit tests: topological sort ordering correctness; cycle detection with artificial cycles; ancestor/descendant discovery for multi-level chains like `b3-bvbg028` → `b3-equities-register` → `b3-equities-spot-market`. | | |

### Implementation Phase 3: Staleness Detection

- GOAL-003: Implement freshness checking to determine which upstream templates need reprocessing

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-013 | Implement `_check_download_template_staleness(template_id: str) -> bool` method. Uses `CacheManager` to query `cache_metadata` for entries where `template = template_id` and `processed_files` is empty or null. Returns `True` if unprocessed downloads exist. | | |
| TASK-014 | Implement `_check_etl_template_staleness(template_id: str) -> bool` method. Checks if the output dataset exists in the filesystem (`CacheManager.db_path(layer/dataset)` directory exists and has `.parquet` files). If no output exists, it is stale. If output exists, compare modification times with upstream datasets. | | |
| TASK-015 | Implement `get_execution_plan(template_id: str, force: bool = False) -> ExecutionPlan` method. Computes topological order, checks staleness for each template, and returns an `ExecutionPlan` dataclass containing: `steps: list[ExecutionStep]` where each step has `template_id`, `action` ("download+process", "process", "etl", "skip"), and `reason`. If `force=True`, all ancestors are marked for execution. | | |
| TASK-016 | Create `ExecutionPlan` and `ExecutionStep` dataclasses in `brasa/engine/dependency_graph.py`. `ExecutionPlan` has `steps`, `target_template`, and `__str__()` for human-readable output. `ExecutionStep` has `template_id`, `action: Literal["download", "process", "etl", "skip"]`, `reason: str`, `template_type: Literal["download", "etl"]`. | | |
| TASK-017 | Write unit tests: staleness detection for download templates with mocked `CacheManager`; staleness detection for ETL templates with mocked filesystem; execution plan with mixed stale/fresh ancestors. | | |

### Implementation Phase 4: Orchestrated Execution

- GOAL-004: Enhance `process_etl()` to automatically execute upstream dependencies

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-018 | Create `brasa/engine/orchestrator.py` with `PipelineOrchestrator` class. Method `execute(template_id: str, force: bool = False, dry_run: bool = False, verbosity: Verbosity = Verbosity.NORMAL) -> OrchestratorReport`. Uses `TemplateDependencyGraph` to build execution plan, then executes each step in order. | | |
| TASK-019 | Implement step execution dispatch in `PipelineOrchestrator`: for download templates, call `process_marketdata(template_id)` (not `download_marketdata` — data is already downloaded, just needs processing); for ETL templates, call `process_etl(template_id)`. Collect `TaskReport` from each step. | | |
| TASK-020 | Create `OrchestratorReport` dataclass that aggregates `TaskReport` instances from each executed step. Properties: `steps_executed: int`, `steps_skipped: int`, `total_duration: float`, `all_reports: list[TaskReport]`, `success: bool`, `summary() -> str`. | | |
| TASK-021 | Implement `dry_run` mode: when `dry_run=True`, `execute()` returns the `OrchestratorReport` with planned steps but no actual execution. Each step\'s report shows what would happen. | | |
| TASK-022 | Add `resolve_dependencies: bool = False` and `force: bool = False` parameters to `process_etl()` in `brasa/engine/api.py`. When `resolve_dependencies=True`, use `PipelineOrchestrator` instead of direct execution. Default is `False` for backward compatibility. | | |
| TASK-023 | Write integration tests: mock-based test of full orchestration flow for `b3-equities-register` (should trigger `b3-bvbg028` processing → then `b3-equities-register` ETL); test `dry_run` mode returns correct plan; test `force` mode includes all ancestors. | | |

### Implementation Phase 5: Public API & CLI

- GOAL-005: Expose dependency graph functionality through public API and CLI

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-024 | Add public functions to `brasa/engine/api.py`: `get_dependency_graph() -> TemplateDependencyGraph`, `get_execution_plan(template_id: str, force: bool = False) -> ExecutionPlan`. | | |
| TASK-025 | Export new public API functions from `brasa/engine/__init__.py` and `brasa/__init__.py`. Add to `__all__` lists: `get_dependency_graph`, `get_execution_plan`, `PipelineOrchestrator`, `ExecutionPlan`, `ExecutionStep`, `OrchestratorReport`. | | |
| TASK-026 | Add CLI commands to `brasa/cli.py`: `deps <template>` (show upstream dependencies), `plan <template>` (show execution plan), `run <template> [--force] [--dry-run]` (execute with dependency resolution). | | |
| TASK-027 | Add `graph` CLI command: `graph [--output deps.dot]` to export the full dependency graph in DOT format for visualization. Optional `--template <name>` flag to show only the subgraph for a specific template. | | |
| TASK-028 | Write tests for CLI commands: test `deps` output format; test `plan` output format; test `graph` DOT output. | | |

### Implementation Phase 6: Documentation & Polish

- GOAL-006: Document the dependency graph system and update existing docs

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-029 | Update `docs/ETL_PIPELINE_DESIGN.md` Phase 2 (Dependency Graph) section with implementation details, replacing the placeholder content. | | |
| TASK-030 | Update `docs/TEMPLATES.md` with a new section on dependency resolution and how templates reference upstream datasets. | | |
| TASK-031 | Update `docs/API_REFERENCE.md` with new public functions: `get_dependency_graph()`, `get_execution_plan()`, `PipelineOrchestrator`. | | |
| TASK-032 | Add inline code examples in docstrings showing typical usage patterns. | | |

## 3. Alternatives

- **ALT-001**: **External DAG framework (Airflow/Prefect/Dagster)** — Rejected because brasa is a lightweight library, not a workflow orchestration platform. Adding a heavy dependency would change the project\'s scope entirely. The dependency graph is simple enough to implement with standard library tools.

- **ALT-002**: **Makefile-style timestamp comparison only** — Rejected because brasa\'s staleness model is more nuanced than file timestamps. Download templates have metadata in SQLite (`processed_files` emptiness), and ETL outputs may need re-running if upstream schemas changed, not just if files are newer.

- **ALT-003**: **Store dependency graph in a separate YAML/JSON file** — Rejected because dependencies are already declaratively expressed in template YAML files. Maintaining a separate dependency file would create synchronization issues. The graph should be derived dynamically from templates.

- **ALT-004**: **Always re-run all upstream templates (no staleness check)** — Rejected because downloading and processing all upstream data is expensive and time-consuming. Staleness detection ensures only necessary work is done.

- **ALT-005**: **Use `process_etl` for everything including download template processing** — Rejected because download templates require `process_marketdata()` (which reads raw files and writes parquet), not `process_etl()` (which runs transformation logic). These are fundamentally different operations.

## 4. Dependencies

- **DEP-001**: `brasa.engine.template` — `retrieve_template()`, `list_templates()`, `MarketDataTemplate`, `MarketDataETL`, `MarketDataWriter` for template introspection
- **DEP-002**: `brasa.engine.cache` — `CacheManager` for staleness detection (querying `cache_metadata` table for unprocessed downloads)
- **DEP-003**: `brasa.engine.api` — `process_marketdata()` and `process_etl()` for executing individual template steps
- **DEP-004**: `brasa.engine.layers` — `DataLayer` enum for layer resolution
- **DEP-005**: `brasa.engine.pipeline.step` — `PipelineStep.get_input_datasets()` for pipeline dependency extraction
- **DEP-006**: `brasa.engine.pipeline.steps.etl_steps` — `LoadDatasetStep`, `ConcatDatasetsStep`, `RunQueryStep` for step-specific dependency extraction via `get_input_datasets()`
- **DEP-007**: `brasa.engine.catalog` — `DatasetCatalog` for validating dataset existence
- **DEP-008**: `brasa.engine.reporting` — `TaskReport`, `Verbosity` for consistent progress reporting
- **DEP-009**: Python standard library `graphlib` (Python 3.9+) — `TopologicalSorter` as an alternative to custom implementation, or implement Kahn\'s algorithm manually for more control

## 5. Files

- **FILE-001**: `brasa/engine/dependency_graph.py` — NEW. Core module containing `TemplateDependencyGraph`, `DatasetOutput`, `ExecutionPlan`, `ExecutionStep` classes. Responsible for scanning templates, building the DAG, reverse index, topological sort, cycle detection, and staleness checking.
- **FILE-002**: `brasa/engine/orchestrator.py` — NEW. `PipelineOrchestrator` class that uses the dependency graph to execute templates in correct order. Contains `OrchestratorReport` dataclass.
- **FILE-003**: `brasa/engine/api.py` — MODIFIED. Add `resolve_dependencies` and `force` parameters to `process_etl()`. Add `get_dependency_graph()` and `get_execution_plan()` public functions.
- **FILE-004**: `brasa/engine/__init__.py` — MODIFIED. Export new classes and functions.
- **FILE-005**: `brasa/__init__.py` — MODIFIED. Export new public API functions.
- **FILE-006**: `brasa/cli.py` — MODIFIED. Add `deps`, `plan`, `run`, and `graph` CLI commands.
- **FILE-007**: `brasa/engine/template.py` — NO CHANGES NEEDED. `MarketDataETL.get_input_datasets()` already delegates to `pipeline.get_input_datasets()` for pipeline-based ETL. Legacy templates are excluded from the graph entirely.
- **FILE-008**: `tests/test_dependency_graph.py` — NEW. Unit tests for graph construction, topological sort, cycle detection, staleness, and execution planning.
- **FILE-009**: `tests/test_orchestrator.py` — NEW. Integration tests for `PipelineOrchestrator`.
- **FILE-010**: `docs/ETL_PIPELINE_DESIGN.md` — MODIFIED. Update Phase 2 section with implementation details.
- **FILE-011**: `docs/TEMPLATES.md` — MODIFIED. Add dependency resolution section.
- **FILE-012**: `docs/API_REFERENCE.md` — MODIFIED. Document new public API.

## 6. Testing

- **TEST-001**: `test_discover_outputs_single_dataset` — Verify that a single-output download template (e.g., `b3-cotahist-daily`) produces `DatasetOutput("input/b3-cotahist-daily", "input", "b3-cotahist-daily", "b3-cotahist-daily")`.
- **TEST-002**: `test_discover_outputs_multi_dataset` — Verify that `b3-bvbg028` produces outputs `input/b3-bvbg028-equities`, `input/b3-bvbg028-options_on_equities`, `input/b3-bvbg028-future_contracts`, etc.
- **TEST-003**: `test_discover_dependencies_load_step` — Verify that `b3-futures` (which has `load` step with `input: b3-futures-settlement-prices`) reports dependency on `b3-futures-settlement-prices`.
- **TEST-004**: `test_discover_dependencies_concat_step` — Verify that `b3-cotahist` (which has `concat_datasets` with `inputs: [b3-cotahist-yearly, b3-cotahist-daily]`) reports both dependencies.
- **TEST-005**: `test_discover_dependencies_sql_query_step` — Verify that `b3-equities-register` (which has `sql_query` with `datasets: [input.b3-bvbg028-equities]`) reports dependency on `input.b3-bvbg028-equities`.
- **TEST-006**: `test_legacy_function_templates_excluded` — Verify that legacy function-based templates (e.g., `b3-futures-wdo` with `etl.function`) are not included in the dependency graph.
- **TEST-007**: `test_legacy_reader_templates_excluded` — Verify that legacy reader-based templates (e.g., templates with `reader.function` instead of `reader.pipeline`) are excluded from the graph.
- **TEST-008**: `test_reverse_index_construction` — Verify reverse index correctly maps dataset IDs to producing templates.
- **TEST-009**: `test_topological_sort_simple_chain` — Verify correct ordering for `b3-bvbg028` → `b3-equities-register` → `b3-equities-spot-market`.
- **TEST-010**: `test_topological_sort_diamond` — Verify correct ordering when a template has two paths to a common ancestor.
- **TEST-011**: `test_cycle_detection` — Create artificial cycle in test fixtures and verify `detect_cycles()` finds it.
- **TEST-012**: `test_execution_plan_with_stale_upstream` — Mock `CacheManager` to report unprocessed downloads, verify execution plan includes processing step.
- **TEST-013**: `test_execution_plan_all_fresh` — Mock all upstreams as fresh, verify execution plan only includes the target ETL.
- **TEST-014**: `test_execution_plan_force_mode` — Verify `force=True` marks all ancestors for execution.
- **TEST-015**: `test_orchestrator_dry_run` — Verify dry run returns plan without executing anything.
- **TEST-016**: `test_orchestrator_execute` — Mock `process_marketdata` and `process_etl`, verify they are called in correct order.
- **TEST-017**: `test_backward_compatibility` — Verify `process_etl("template")` without `resolve_dependencies` works exactly as before.

## 7. Risks & Assumptions

- **RISK-001**: Template loading time — scanning all ~108 templates at graph construction time may be slow. **Mitigation**: Templates are already cached by `retrieve_template()`. Graph construction can also be cached with TTL or invalidation on template file changes.
- **RISK-002**: Legacy function-based templates are excluded — templates using `etl.function` or `reader.function` will not participate in the dependency graph. If a pipeline-based template depends on a dataset produced by a legacy template, the reverse index will not find a producer. **Mitigation**: Log a warning when a dataset dependency cannot be resolved to a producing template. Document that legacy templates should be migrated to pipeline-based templates to participate in dependency resolution.
- **RISK-003**: Dataset name resolution ambiguity — bare dataset names (without layer prefix) may match multiple datasets across layers. **Mitigation**: Use the producing template\'s writer config to determine the canonical layer. Fall back to `input` layer if ambiguous.
- **RISK-004**: Download templates require download + process, not just process — orchestrator must distinguish between "data not yet downloaded" (needs `download_marketdata`) and "data downloaded but not processed" (needs `process_marketdata`). **Mitigation**: Check `cache_metadata` for existing download entries; if none exist, the download step itself is outside the orchestrator\'s scope (user must download first). Orchestrator only handles processing of already-downloaded data.
- **RISK-005**: Graph may become stale if templates are modified while cached. **Mitigation**: Add `clear_graph_cache()` and consider using template file modification timestamps for invalidation.
- **ASSUMPTION-001**: All template YAML files are valid and parseable. Broken templates are logged and skipped during graph construction.
- **ASSUMPTION-002**: The dependency graph is acyclic. Circular dependencies are a template authoring error and should be caught at build time.
- **ASSUMPTION-003**: Dataset naming convention `{template-id}-{dataset-key}` for multi-output templates is consistent across all existing templates.
- **ASSUMPTION-004**: The existing `process_marketdata()` function correctly handles reprocessing when called with `reprocess=False` — it skips already-processed entries and only processes new ones.
- **ASSUMPTION-005**: Users have already downloaded data via `download_marketdata()` before calling `process_etl()` with dependency resolution. The orchestrator handles processing, not downloading.

## 8. Related Specifications / Further Reading

- [ETL Pipeline Design Document](../docs/ETL_PIPELINE_DESIGN.md) — Phase 2 describes the dependency graph concept
- [Templates Documentation](../docs/TEMPLATES.md) — Template types and configuration
- [Architecture Overview](../docs/ARCHITECTURE.md) — System architecture and data flow
- [Dataset Catalog Plan](feature-dataset-catalog-1.md) — Related feature for dataset metadata tracking
- Python `graphlib.TopologicalSorter` — [Python docs](https://docs.python.org/3/library/graphlib.html)
- Kahn\'s Algorithm — [Wikipedia](https://en.wikipedia.org/wiki/Topological_sorting#Kahn\'s_algorithm)
