# Plan: Expose download template `dependencies:` block in dependency graph

## Context

`TemplateDependencyGraph._discover_dependencies()` treats all download templates as source
nodes with no upstream dependencies, returning `[]` unconditionally. But download templates
can declare a `dependencies:` block (e.g. `b3-indexes-theoretical-portfolio`) that at
runtime drives `resolve_dependencies()` to run upstream ETL templates. The graph has no
knowledge of this, so `deps` CLI shows no ancestors/downstream for these templates, and
`topological_sort` / `get_execution_plan` are also blind to the relationship.

The fix: parse the `dependencies:` block of download templates in `_discover_dependencies()`
and return the referenced dataset refs, exactly as ETL templates return their load inputs.

## Key facts

- `template.dependencies` is stored raw from YAML as a list of dicts:
  `[{arg_name: {required: bool, from: {datasets: ["staging.foo"], query: "..."}}}]`
  (via `self.__dict__[section_name] = section_data` in `_process_template_section`)
- `_normalize_dataset_ref()` already handles both `"layer.name"` and bare `"name"` formats,
  so the refs from `from.datasets` can be used as-is.
- No changes needed to `dependency_resolver.py` — it already works correctly at runtime.

## Files to modify

- `brasa/engine/dependency_graph.py` — `_discover_dependencies()` static method (lines 264–289)
- `tests/test_dependency_graph.py` — add tests and update `_make_download_template()`

## Implementation

### 1. `_discover_dependencies()` in `dependency_graph.py`

Replace the download template branch:

```python
@staticmethod
def _discover_dependencies(template: MarketDataTemplate) -> list[str]:
    """Discover the dataset references that *template* depends on.

    For download templates, extracts dataset refs from the ``dependencies:``
    block (the ``from.datasets`` lists).  For pipeline-based ETL templates
    it delegates to ``ETLPipeline.get_input_datasets()``.

    Args:
        template: A loaded ``MarketDataTemplate``.

    Returns:
        List of raw dataset reference strings (e.g.
        ``"staging.b3-indexes-composition"`` or ``"b3-futures-settlement-prices"``).
    """
    # Download templates: extract dataset refs from the `dependencies:` block
    if template.has_reader:
        raw_deps = getattr(template, "dependencies", None)
        if not raw_deps:
            return []
        refs: list[str] = []
        for dep_entry in raw_deps:
            if not isinstance(dep_entry, dict):
                continue
            for dep_config in dep_entry.values():
                from_block = dep_config.get("from", {}) if isinstance(dep_config, dict) else {}
                refs.extend(from_block.get("datasets", []))
        return refs

    # ETL templates
    if template.is_etl and hasattr(template, "etl") and template.etl.is_pipeline:
        return template.etl.get_input_datasets()

    return []
```

### 2. `_make_download_template()` in `tests/test_dependency_graph.py`

Add a `dependencies` parameter so tests can set `tmpl.dependencies`:

```python
def _make_download_template(
    template_id: str,
    *,
    datasets: dict | None = None,
    writer_layer: str = "input",
    has_pipeline: bool = True,
    dependencies: list | None = None,   # NEW
) -> MarketDataTemplate:
    ...
    tmpl.dependencies = dependencies  # NEW (None when not set)
    return tmpl
```

### 3. New tests to add

- `test_discover_dependencies_download_no_deps` — download template with no `dependencies` attr → `[]`
- `test_discover_dependencies_download_with_deps` — download template with one dep entry → returns the dataset refs
- `test_discover_dependencies_download_multiple_deps` — multiple dep entries / multiple datasets per entry → all refs returned
- `test_edges_include_download_template_deps` — graph built with a download template whose dep points to an ETL producer → edge appears in `graph.edges`
- `test_get_ancestors_download_template` — `get_ancestors()` returns the upstream ETL for a download template with `dependencies:`
- `test_get_downstream_etl_visible_from_download_dep` — `get_downstream()` on the upstream ETL returns the download template

## Verification

```bash
uv run pytest tests/test_dependency_graph.py -v
uv run pytest --no-integration -v
uv run python -m brasa.cli deps b3-indexes-theoretical-portfolio
# Expected: shows b3-indexes-composition-consolidated as ancestor
uv run python -m brasa.cli deps b3-indexes-composition-consolidated
# Expected: shows b3-indexes-theoretical-portfolio as downstream
uv run ruff check . && uv run ruff format --check .
uv run pre-commit run --all-files
```
