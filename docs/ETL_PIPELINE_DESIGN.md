# ETL Pipeline Design Document

This document describes the design and implementation of the ETL pipeline system for brasa, enabling declarative dataset transformations through YAML templates.

## Problem Statement

The brasa system downloads and processes market data into partitioned parquet files. For example, `b3-futures-settlement-prices` creates many small parquet files partitioned by `refdate`.

**Need:** Create derived datasets from existing datasets with different partitioning or transformations, without writing custom Python functions for each case.

**Example use case:** Create a `b3-futures` dataset that consolidates `b3-futures-settlement-prices` into fewer files partitioned by `commodity` instead of `refdate`.

## Design Goals

1. **Declarative** - Define transformations in YAML, not Python code
2. **Dependency tracking** - Know which datasets depend on which
3. **Chainable** - Support multi-step transformations (A → B → C)
4. **Consistent** - Reuse existing template patterns (`etl:`, `writer:`, `fields:`)
5. **Backward compatible** - Existing function-based ETL templates continue working

## Solution: Pipeline-based ETL

Extend the existing `etl:` section with a `pipeline:` key that defines transformation steps:

```yaml
id: b3-futures
description: Consolidated futures by commodity
etl:
  pipeline:
    - step: load
      input: b3-futures-settlement-prices
writer:
  partitioning: [commodity]
```

### Key Design Decisions

1. **Uses `etl:` section** - Consistent with existing derived dataset pattern
2. **Pipeline steps mirror reader pipeline** - Familiar syntax for users
3. **Implicit step chaining** - Output of step N is input to step N+1
4. **Dependencies from `input` parameters** - Each step declares its source
5. **Writer controls output** - `writer.partitioning` determines file structure

## Architecture

### Template Types

| Type | Has `downloader:` | Has `reader:` | Has `etl:` |
|------|-------------------|---------------|------------|
| Source | ✓ | ✓ | ✗ |
| Function ETL | ✗ | ✗ | `function:` |
| Pipeline ETL | ✗ | ✗ | `pipeline:` |

### Detection Logic

```python
if "pipeline" in etl_config:
    # New pipeline-based ETL
    self._pipeline = ETLPipeline.from_config(etl_config["pipeline"])
else:
    # Legacy function-based ETL
    self.process_function = load_function_by_name(etl_config["function"])
```

## Components

### 1. Shared Transforms (Code Reuse Layer)

Location: `brasa/engine/pipeline/steps/shared_transforms.py`

Provides reusable transformation functions that work with both reader and ETL pipelines:

```python
# Functions that operate on DataFrames or PyArrow Datasets
def filter_data(data, where: dict) -> DataFrame | Dataset
def select_columns(data, columns: list[str]) -> DataFrame | Dataset
def sort_data(data, by: str | list, descending: bool = False) -> DataFrame
def drop_columns(data, columns: list[str]) -> DataFrame
def rename_columns(data, mapping: dict[str, str]) -> DataFrame
def drop_duplicates(data, subset: list[str] | None, keep: str = "first") -> DataFrame
def fill_na(data, value=None, method=None, columns=None) -> DataFrame
def to_dataframe(data) -> DataFrame
```

### 2. PipelineContextProtocol

Location: `brasa/engine/pipeline/context_protocol.py`

Defines the common interface both contexts implement:

```python
@runtime_checkable
class PipelineContextProtocol(Protocol):
    template_id: str
    fields: Fieldset | None
    intermediate_results: dict[str, Any]

    def store_result(self, name: str, value: Any) -> None: ...
    def get_result(self, name: str, default: Any = None) -> Any: ...
```

### 3. ETLPipelineContext

Location: `brasa/engine/pipeline/etl_context.py`

Carries metadata through pipeline execution:

```python
@dataclass
class ETLPipelineContext:
    template_id: str = ""
    writer: MarketDataWriter | None = None
    fields: Fieldset | None = None
    intermediate_results: dict[str, Any] = field(default_factory=dict)
```

### 4. PipelineStep (Base Class)

Location: `brasa/engine/pipeline/step.py`

Abstract base class for all ETL steps:

```python
class PipelineStep(ABC):
    name: str = ""

    @abstractmethod
    def execute(self, data: Any, context: ETLPipelineContext) -> Any:
        ...

    def get_input_datasets(self) -> list[str]:
        """Returns dataset names this step depends on."""
        ...
```

### 3. ETLStepRegistry

Location: `brasa/engine/pipeline/steps/etl_steps.py`

Registry pattern for step discovery:

```python
@ETLStepRegistry.register("load")
class LoadDatasetStep(PipelineStep):
    def execute(self, data, context):
        return get_dataset(self.require_param("input"))
```

### 4. ETLPipeline (Executor)

Location: `brasa/engine/pipeline/etl_executor.py`

Orchestrates step execution and writes output:

```python
class ETLPipeline:
    def execute(self, template_id, writer, fields) -> pd.DataFrame:
        """Execute all steps and return result."""

    def execute_and_write(self, template_id, writer, fields) -> None:
        """Execute and write to output dataset."""
```

### 5. MarketDataETL (Updated)

Location: `brasa/engine/template.py`

Extended to support both patterns:

```python
class MarketDataETL:
    @property
    def is_pipeline(self) -> bool:
        return self._is_pipeline

    def get_input_datasets(self) -> list[str]:
        """Get dependencies for graph building."""
```

## Built-in Steps (Phase 1)

| Step | Description | Parameters |
|------|-------------|------------|
| `load` | Load a dataset by name | `input`: dataset name |
| `filter` | Filter rows | `where`: dict of column → value(s) |
| `select` | Select columns | `columns`: list of column names |
| `sort` | Sort rows | `by`: column(s), `descending`: bool |
| `to_dataframe` | Convert to pandas | (none) |
| `drop_columns` | Remove columns | `columns`: list of column names |
| `rename_columns` | Rename columns | `mapping`: dict of old → new |
| `drop_duplicates` | Remove duplicate rows | `subset`: columns, `keep`: first/last |
| `fill_na` | Fill missing values | `value`, `method`, `columns` |

## Sharing Code Between Reader and ETL Pipelines

ETL steps use the `shared_transforms` module which provides the actual transformation logic. This enables:

1. **Code reuse**: Same logic for both reader and ETL pipelines
2. **Direct function usage**: Use transforms without step wrappers
3. **Testing**: Test transforms in isolation

Example - using shared transforms directly:
```python
from brasa.engine.pipeline import shared_transforms

# Filter a dataset
filtered = shared_transforms.filter_data(df, {"commodity": "DI1"})

# Sort and select
sorted_df = shared_transforms.sort_data(df, by=["refdate", "symbol"])
selected = shared_transforms.select_columns(sorted_df, ["symbol", "price"])
```

## Template Examples

### Simple Repartitioning

```yaml
id: b3-futures
description: Futures repartitioned by commodity
etl:
  pipeline:
    - step: load
      input: b3-futures-settlement-prices
writer:
  partitioning: [commodity]
```

### Filter and Consolidate

```yaml
id: b3-futures-di1-consolidated
description: DI1 futures in single file
etl:
  pipeline:
    - step: load
      input: b3-futures-settlement-prices
    - step: filter
      where:
        commodity: "DI1"
    - step: sort
      by: [refdate, maturity_code]
writer:
  partitioning: []  # empty = single file
```

### Filter with Multiple Values

```yaml
id: b3-futures-rates
description: Rate futures only (DI1, DDI, DAP)
etl:
  pipeline:
    - step: load
      input: b3-futures-settlement-prices
    - step: filter
      where:
        commodity: ["DI1", "DDI", "DAP"]
writer:
  partitioning: [commodity, refdate]
```

## Dependency Graph

Dependencies are extracted by scanning all templates:

```python
def build_dependency_graph():
    graph = {}
    for template in all_templates():
        if hasattr(template, 'etl') and template.etl.is_pipeline:
            deps = template.etl.get_input_datasets()
            graph[template.id] = deps
    return graph
```

Example graph:
```
b3-futures-settlement-prices: []  (source)
b3-futures: [b3-futures-settlement-prices]
b3-futures-di1: [b3-futures-settlement-prices]
b3-futures-di1-consolidated: [b3-futures-di1]
```

## Usage

### Running an ETL Pipeline

```python
from brasa.engine import process_etl

# Works for both function-based and pipeline-based ETL
process_etl("b3-futures")
```

### Programmatic Access

```python
from brasa.engine import retrieve_template

template = retrieve_template("b3-futures")
print(template.etl.is_pipeline)  # True
print(template.etl.get_input_datasets())  # ['b3-futures-settlement-prices']
```

## Implementation Phases

### Phase 1: Core Infrastructure ✅
- ETLPipelineContext
- PipelineStep base class
- ETLStepRegistry
- ETLPipeline executor
- Steps: load, filter, select, sort, to_dataframe
- Template detection (pipeline vs function)
- Updated process_etl()

### Phase 2: Dependency Graph
- Build graph from all templates
- Topological sort for processing order
- Detect circular dependencies

### Phase 3: Freshness Checking
- Track input/output modification times
- Only rebuild if inputs changed
- `--force` flag to rebuild anyway

### Phase 4: Join Step
```yaml
- step: join
  input: b3-futures
  right: b3-commodity-info
  on: [commodity]
  how: left
```

### Phase 5: Aggregate Step
```yaml
- step: aggregate
  by: [refdate, commodity]
  agg:
    price: mean
    volume: sum
```

### Phase 6: CLI Commands
```bash
brasa pipeline run b3-futures
brasa pipeline deps b3-futures
brasa pipeline graph --output deps.png
```

## File Locations

```
brasa/engine/pipeline/
├── __init__.py
├── context.py              # Reader pipeline context
├── context_protocol.py     # Common interface for contexts (NEW)
├── etl_context.py          # ETL pipeline context (NEW)
├── etl_executor.py         # ETL pipeline executor (NEW)
├── executor.py         # Reader pipeline executor
├── registry.py         # Reader step registry
├── step.py             # Reader step base class
└── steps/              # Reader + ETL step implementations
  ├── etl_steps.py    # ETL step base + registry + built-ins (NEW)
  └── shared_transforms.py  # Shared transformation functions (NEW)

templates/
├── b3-futures.yaml                    # Pipeline ETL example (NEW)
├── b3-futures-di1-consolidated.yaml   # Pipeline ETL example (NEW)
├── b3-futures-dol.yaml                # Function ETL (existing)
└── b3-futures-settlement-prices.yaml  # Source template (existing)
```

## Backward Compatibility

Existing function-based ETL templates continue to work unchanged:

```yaml
# This still works exactly as before
id: b3-futures-dol
etl:
  function: brasa.etl.create_b3_price_futures
  futures_dataset: b3-futures-settlement-prices
  maturity_day: first day
  commodity: DOL
```

The system detects the pattern by checking for `pipeline` key in the `etl` section.

## Design Alternatives Considered

### Alternative 1: Separate `pipeline:` Top-Level Key
```yaml
id: b3-futures
pipeline:
  input: b3-futures-settlement-prices
  steps:
    - consolidate
```
**Rejected:** Creates a new concept; `etl:` already exists for derived datasets.

### Alternative 2: View Concept
```yaml
id: b3-futures
view:
  source: b3-futures-settlement-prices
  materialized: true
```
**Rejected:** Too different from existing patterns; views imply different semantics.

### Alternative 3: Explicit Step Naming
```yaml
etl:
  pipeline:
    - step: filter
      name: filtered_data
      input: source-dataset
    - step: sort
      input: filtered_data  # explicit reference
```
**Deferred:** Can be added later if needed; implicit chaining is simpler for now.

## Testing

Test coverage in `tests/test_pipeline.py`:

```python
def test_etl_pipeline_template_loading():
    """Test loading an ETL template with pipeline configuration."""
    template = retrieve_template("b3-futures")
    assert template.is_etl
    assert template.etl.is_pipeline
    assert "b3-futures-settlement-prices" in template.etl.get_input_datasets()

def test_etl_step_registry():
    """Test the ETL step registry."""
    steps = ETLStepRegistry.get_all_steps()
    assert "load" in steps
    assert "filter" in steps
```
