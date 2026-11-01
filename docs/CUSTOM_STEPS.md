# Custom pipeline steps

Most pipelines only need the built-in steps (`read_csv`, `sql_query`,
`apply_fields`, …). When you need your own Python, put it in your
[template folder](TEMPLATES.md#template-folders), next to the templates that use it:

```
my-folder/
  templates/
    my-dataset.yaml
  steps.py          # or a steps/ package with an __init__.py
```

brasa imports `steps.py` (and `steps/__init__.py`) of every template folder the
first time it looks for templates — from the CLI or from Python, with no extra
configuration. Importing runs the `@StepRegistry.register` decorators, so the
steps can be used in that folder's templates.

## A minimal step

```python
# my-folder/steps.py
from brasa.engine.pipeline import PipelineStep, StepRegistry


@StepRegistry.register("my_add_source")
class AddSourceStep(PipelineStep):
    """Add a constant `source` column (parameter: value)."""

    def execute(self, data, context):
        return data.assign(source=self.get_param("value", "my-folder"))
```

```yaml
# my-folder/templates/my-dataset.yaml
id: my-dataset
description: CSV import with a custom step
importer:
  format: csv
reader:
  pipeline:
    steps:
      - read_csv
      - my_add_source
      - apply_fields
fields:
  - {name: refdate, description: Reference date, type: date}
  - {name: value, description: Value, type: numeric}
  - {name: source, description: Added by my_add_source, type: string}
```

```bash
export BRASA_TEMPLATE_PATH=~/my-folder
brasa import my-dataset --path data.csv && brasa process my-dataset
brasa head input.my-dataset
```

The step API: subclass `PipelineStep`, implement `execute(data, context)` and
return the new data; read parameters with `self.get_param(name, default)`.
In a pipeline, a step is written as its name, or as a mapping with `step:` and
its parameters.

## Rules

- **Names are global.** A step name that is already registered (built-in or
  from another folder) is an error — steps are never shadowed, unlike
  templates. Prefix your step names with your folder or organization
  (`myorg_…`).
- **A `steps/` package** must have an `__init__.py` that imports its modules;
  relative imports inside it work.
- **Errors name the file.** A syntax error or a duplicate name stops template
  loading with `Failed to import custom steps from <file>: …`.
- **Steps are code that runs on import.** Only add template folders you trust.
- **Compatibility:** `PipelineStep`, `get_param`, `execute` and the pipeline
  context are public API. While brasa is 0.x they may change in a minor
  release (with a note in the CHANGELOG's "Template format changes"); from 1.0
  on, a breaking change first ships with a warning for one minor release.
