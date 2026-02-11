Here are a few viable alternatives, then a DRAFT plan for the preprocessor approach you selected.

**Alternatives**
- Runtime inheritance in loader: Add `extends` resolution and merge logic in [brasa/engine/template.py](brasa/engine/template.py#L322-L526) so everything happens transparently at load time; higher complexity, but no build step.
- Preprocess/compile step (your selection): Create a template compiler that resolves `extends`, writes expanded YAMLs, and keep loader unchanged; adds a build step but keeps runtime simpler.
- YAML anchors/aliases only: Use native YAML anchors in each file; minimal code changes but limited to single-file reuse and awkward across files.
- Jinja2 templating: Treat templates as Jinja and render with a small context; flexible but adds a new dependency and a separate rendering step.

## Plan: Template Inheritance via Preprocessor

TL;DR: Add an optional template compiler that resolves `extends` references, performs a keyed merge for lists, and writes fully-expanded templates to a build output directory. Keep runtime loader unchanged, document the workflow, and add tests to guarantee backward compatibility. This aligns with your preference for `extends`, custom list merge rules, and optional adoption.

**Steps**
1. Define inheritance rules and schema: add `extends` to the documented schema and specify merge semantics (maps deep-merge; lists merge by key) in [docs/TEMPLATES.md](docs/TEMPLATES.md#L24-L55) and cross-reference in [docs/ETL_PIPELINE_DESIGN.md](docs/ETL_PIPELINE_DESIGN.md#L21-L63).
2. Implement a template compiler module (new file), likely in brasa/engine or a new scripts/ path; it should load YAML, resolve `extends` recursively, and apply keyed merges for `fields`, `datasets`, and `reader.pipeline` steps (keys: `name` for fields, dataset keys for datasets, and `step` for pipeline items).
3. Provide a CLI entry point to run the compiler, wiring it through [brasa/cli.py](brasa/cli.py) or a small script; output to a dedicated directory (e.g., templates/compiled) while preserving filenames.
4. Add or adjust a template retrieval helper to optionally read from compiled output first (config or CLI argument), without changing existing behavior in [brasa/engine/template.py](brasa/engine/template.py#L432-L526).
5. Update the four target templates to introduce a shared base template and small child overrides using `extends`, keeping their output equivalent to the current YAMLs.
6. Add tests to ensure compiled templates are identical in structure to current ones and that legacy templates load unchanged, likely in tests/test_templates.py.

**Verification**
- poetry run pytest tests/test_templates.py
- Manual: run template compilation on the four templates and confirm runtime loads for existing templates are unchanged.

**Decisions**
- Use `extends` as the inheritance keyword.
- Use keyed list merge (field `name`, pipeline `step`, dataset keys).
- Keep inheritance optional and backward-compatible.
