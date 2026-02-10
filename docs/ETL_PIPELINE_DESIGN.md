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

## Available Pipeline Steps

All steps are registered in the unified `StepRegistry` and can be used in both reader and ETL pipelines. Steps are organized by category for easier reference.

### Data Loading Steps (ETL)

| Step | Input Type | Output Type | Uses Context | Description | Parameters |
|------|------------|-------------|--------------|-------------|------------|
| `load` | Any (ignored) | Dataset | No | Load a dataset by name (template or explicit) | `template`: template name OR `input`: dataset name, `layer`: data layer, `partitioning`: partitioning scheme |
| `concat_datasets` | Any (ignored) | DataFrame | No | Concatenate multiple datasets vertically | `inputs`: list of dataset names, `layer`: data layer, `columns`: optional column filter |
| `dataset_filter` | Dataset/DataFrame | DataFrame | No | Filter rows in a Dataset or DataFrame | `where`: dict of column → value(s) for equality filtering |
| `dataset_select` | Dataset/DataFrame | DataFrame | No | Select specific columns from Dataset/DataFrame | `columns`: list of column names |
| `select_fields` | Dataset/DataFrame | DataFrame | Yes | Select columns based on field names from template | (uses `context.fields`) |
| `dataset_sort` | Dataset/DataFrame | DataFrame | No | Sort data by columns | `by`: column(s), `descending`: bool or list |
| `to_dataframe` | Dataset/DataFrame | DataFrame | No | Convert PyArrow Dataset to pandas DataFrame | (none) |
| `dataset_drop_columns` | Dataset/DataFrame | DataFrame | No | Drop columns from Dataset/DataFrame | `columns`: list of column names |
| `dataset_rename_columns` | Dataset/DataFrame | DataFrame | No | Rename columns in Dataset/DataFrame | `mapping`: dict of old → new |
| `dataset_drop_duplicates` | Dataset/DataFrame | DataFrame | No | Remove duplicate rows | `subset`: columns to check, `keep`: 'first'/'last'/False |
| `dataset_fill_na` | Dataset/DataFrame | DataFrame | No | Fill missing values | `value`: fill value, `method`: 'ffill'/'bfill', `columns`: optional |

### Data Loading Steps (I/O)

| Step | Input Type | Output Type | Uses Context | Description | Parameters |
|------|------------|-------------|--------------|-------------|------------|
| `read_csv` | Any (ignored) | DataFrame | Yes | Read CSV file into DataFrame | `separator`: field separator, `skip`: rows to skip, `header`: header row, `names`: column names, `converters`: column converters |
| `read_fwf` | Any (ignored) | DataFrame | Yes | Read fixed-width format file | `colspecs`: list of (start, end) tuples, `names`: column names, `skip`: rows to skip |
| `read_json` | Any (ignored) | DataFrame | Yes | Read JSON file into DataFrame | `orient`: JSON orientation, `path`: JSON path to extract |
| `read_excel` | Any (ignored) | DataFrame | Yes | Read Excel file into DataFrame | `sheet`: sheet name/index, `skip`: rows to skip, `header`: header row |

### HTML Steps

| Step | Input Type | Output Type | Uses Context | Description | Parameters |
|------|------------|-------------|--------------|-------------|------------|
| `read_html` | Any (ignored) | List[DataFrame] | Yes | Read HTML tables into list of DataFrames | `attrs`: HTML attributes dict, `match`: regex to match table text, `flavor`: parser to use |
| `select_table` | List[DataFrame] | DataFrame | No | Select a single table from list | `index`: table index (default: 0) |
| `first_table` | List[DataFrame] | DataFrame | No | Select the first table from list | (none) |
| `parse_html_element` | Any | Any | Yes | Parse HTML element using XPath | `xpath`: XPath expression, `attribute`: attribute to extract, `store_as`: context storage name |

### Column Manipulation Steps

| Step | Input Type | Output Type | Uses Context | Description | Parameters |
|------|------------|-------------|--------------|-------------|------------|
| `set_columns` | DataFrame | DataFrame | No | Set column names for DataFrame | `names`: list of column names |
| `rename_columns` | DataFrame | DataFrame | No | Rename columns using mapping | `mapping`: dict of old → new names |
| `select_columns` | DataFrame | DataFrame | No | Select specific columns | `columns`: list of column names |
| `drop_columns` | DataFrame | DataFrame | No | Drop columns from data | `columns`: list of columns to drop, `errors`: 'raise'/'ignore' |
| `add_column` | DataFrame | DataFrame | Yes | Add a new column with static or dynamic value | `name`: column name, `value`: static value OR `from`: dict with 'where'/'key', `only_if_missing`: bool |
| `add_column_multi` | Dict[str, DataFrame] | Dict[str, DataFrame] | Yes | Add column to multiple DataFrames in dict | (same as `add_column`) |
| `set_column` | DataFrame | DataFrame | Yes | Alias for `add_column` | (same as `add_column`) |
| `set_column_multi` | Dict[str, DataFrame] | Dict[str, DataFrame] | Yes | Alias for `add_column_multi` | (same as `add_column_multi`) |
| `reorder_columns` | DataFrame | DataFrame | No | Reorder columns in specific order | `order`: list of column names, `keep_rest`: keep unlisted columns |

### Row Filtering & Transformation Steps

| Step | Input Type | Output Type | Uses Context | Description | Parameters |
|------|------------|-------------|--------------|-------------|------------|
| `filter_rows` | DataFrame | DataFrame | No | Filter rows based on condition | `column`: column to filter, `operator`: comparison operator, `value`: comparison value |
| `drop_duplicates` | DataFrame | DataFrame | No | Remove duplicate rows | `subset`: columns to check, `keep`: 'first'/'last'/False |
| `drop_na` | DataFrame | DataFrame | No | Drop rows with NA/NaN values | `columns`: columns to check, `how`: 'any'/'all' |
| `sort` | DataFrame | DataFrame | No | Sort data by columns | `by`: column(s), `ascending`: bool/list OR `descending`: bool/list, `na_position`: 'first'/'last' |
| `melt` | DataFrame | DataFrame | No | Unpivot DataFrame from wide to long | `id_vars`: identifier columns, `value_vars`: columns to unpivot, `var_name`: variable column name, `value_name`: value column name |

### Type Conversion & Parsing Steps

| Step | Input Type | Output Type | Uses Context | Description | Parameters |
|------|------------|-------------|--------------|-------------|------------|
| `apply_fields` | DataFrame | DataFrame | Yes | Apply field definitions to DataFrame | `errors`: 'raise'/'coerce'/'ignore', `set_columns`: bool |
| `apply_fields_multi` | Dict[str, DataFrame] | Dict[str, DataFrame] | Yes | Apply fields to multiple DataFrames | `errors`: 'raise'/'coerce'/'ignore' |
| `parse_numeric` | DataFrame | DataFrame | Yes | Parse string columns as numeric | `columns`: list of columns, `errors`: error handling |
| `parse_date` | DataFrame | DataFrame | No | Parse string columns as dates | `columns`: list of columns, `format`: date format, `errors`: error handling |
| `parse_datetime` | DataFrame | DataFrame | No | Parse string columns as datetime | `columns`: list of columns, `format`: datetime format, `errors`: error handling |
| `cast` | DataFrame | DataFrame | No | Cast columns to specific type | `column`: column(s) to cast, `dtype`: target type, `errors`: error handling |
| `make_date` | DataFrame | DataFrame | No | Create date from year/month/day components | `year_column`: year column, `month_column`: month column, `day_column`: day column, `output`: output name, `errors`: error handling |

### Data Transformation Steps

| Step | Input Type | Output Type | Uses Context | Description | Parameters |
|------|------------|-------------|--------------|-------------|------------|
| `fill_na` | DataFrame | DataFrame | No | Fill NA/NaN values | `columns`: columns to fill, `value`: fill value, `method`: 'ffill'/'bfill' |
| `forward_fill_column` | DataFrame | DataFrame | No | Forward fill values in column | `column`: column to fill, `condition`: condition to check |
| `extract_regex` | DataFrame | DataFrame | No | Extract values using regex | `column`: source column, `pattern`: regex pattern, `output`: output column, `group`: capture group index |
| `concat_columns` | DataFrame | DataFrame | No | Concatenate multiple columns | `columns`: list of columns, `output`: output column, `separator`: separator string |
| `str_replace` | DataFrame | DataFrame | No | Replace pattern in string column | `column`: column to process, `pattern`: search pattern, `replacement`: replacement string, `output`: output column, `regex`: bool |

### B3-Specific Steps

| Step | Input Type | Output Type | Uses Context | Description | Parameters |
|------|------------|-------------|--------------|-------------|------------|
| `b3_parse_refdate_from_html` | DataFrame | DataFrame | Yes | Parse reference date from B3 HTML | `xpath`: XPath expression, `attribute`: attribute name, `store_as`: context storage name |
| `b3_forward_fill_commodity` | DataFrame | DataFrame | No | Forward fill commodity names in table | `column`: column name (default: 'commodity') |
| `b3_extract_commodity_code` | DataFrame | DataFrame | No | Extract commodity code from name | `column`: column to process (default: 'commodity') |
| `b3_create_symbol` | DataFrame | DataFrame | No | Create futures symbol from parts | `commodity_column`: commodity column, `maturity_column`: maturity column, `output_column`: output name |
| `b3_read_bvbg086_xml` | Any (ignored) | DataFrame | Yes | Read B3 BVBG086 XML file (price reports) | (uses field tags from context) |
| `b3_read_bvbg028_xml` | Any (ignored) | Dict[str, DataFrame] | Yes | Read B3 BVBG028 XML file (multi-dataset) | (uses datasets config from context) |
| `b3_read_bvbg087_xml` | Any (ignored) | Dict[str, DataFrame] | Yes | Read B3 BVBG087 XML file (indexes) | (uses datasets config from context) |
| `b3_read_company_info_json` | Any (ignored) | Dict[str, DataFrame] | Yes | Read B3 company info JSON | (uses datasets config from context) |
| `b3_read_company_details_json` | Any (ignored) | DataFrame | Yes | Read B3 company details JSON | (none) |
| `b3_add_columns_from_json_fields` | DataFrame | DataFrame | Yes | Parse JSON fields and add as columns | `mapping`: dict of column → JSON path |

### Custom Function Steps

| Step | Input Type | Output Type | Uses Context | Description | Parameters |
|------|------------|-------------|--------------|-------------|------------|
| `custom` | Any | Any | Yes | Execute custom function with data and context | `function`: fully qualified function name |
| `custom_simple` | Any | Any | No | Execute simple custom function (data only) | `function`: fully qualified function name |
| `legacy_reader` | Any (ignored) | DataFrame | Yes | Execute legacy reader function | `function`: fully qualified function name |
| `apply_lambda` | DataFrame | DataFrame | No | Apply lambda expression to column | `column`: column to process, `expression`: Python expression, `output`: output column, `axis`: 0/1 |
| `exec_code` | DataFrame | DataFrame | Yes | Execute arbitrary Python code | `code`: Python code (must assign to 'result') |

### Notes

- **Input Type**: The type of data the step expects to receive from the previous step. "Any (ignored)" means the step doesn't use the incoming data (typically first steps in a pipeline).
- **Output Type**: The type of data the step returns. Common types:
  - `DataFrame`: pandas DataFrame
  - `Dataset`: PyArrow Dataset
  - `Dataset/DataFrame`: Accepts and returns either type
  - `List[DataFrame]`: List of DataFrames (from HTML parsing)
  - `Dict[str, DataFrame]`: Dictionary mapping dataset names to DataFrames (multi-output)
- **Uses Context**: Whether the step needs access to the pipeline context for metadata, configuration, or side effects (e.g., storing intermediate results, accessing file paths, field definitions).
- **Dataset vs DataFrame**: Steps prefixed with `dataset_` work with both PyArrow Datasets and pandas DataFrames. Other steps typically require DataFrames.
- **Multi-output steps**: Some B3 steps (e.g., `b3_read_bvbg028_xml`) return `Dict[str, DataFrame]` for multi-dataset processing.
- **Context access**: Steps like `add_column` can access pipeline context for dynamic values from `download_args`, `extra_key`, or stored intermediate results.
- **Field-aware steps**: Steps like `apply_fields` use the template's fieldset definition from the context.

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
