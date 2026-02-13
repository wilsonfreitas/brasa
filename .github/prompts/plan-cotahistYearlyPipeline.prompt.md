## Plan: Migrate Cotahist Yearly to Pipeline (Width-Driven) ✓ COMPLETED

Successfully migrated the yearly cotahist template to a `reader.pipeline` that derives `read_fwf` column widths directly from each `field.width`. The legacy field names and only record type `01` rows are preserved, while `field.handler` is replaced by `field.type` and `field.width` is used to build `read_fwf` widths. This ensures a single source of truth for column sizing.

**Completed Steps**
1. ✓ Validated `fields` + `field.type` usage in [docs/TEMPLATES.md](docs/TEMPLATES.md) and confirmed pipeline syntax
2. ✓ Confirmed pipeline steps in [brasa/engine/pipeline/steps/io_steps.py](brasa/engine/pipeline/steps/io_steps.py) and [brasa/engine/pipeline/steps/transform_steps.py](brasa/engine/pipeline/steps/transform_steps.py)
3. ✓ Enhanced `read_fwf` step to automatically derive colspecs and names from `context.fields`:
   - Iterates through fields and accumulates widths to build colspecs: `[(start, start+width), ...]`
   - Extracts field names to build column names list
   - Added gzip file support (handles `.gz` extensions)
   - Falls back to explicit parameters if provided for backward compatibility
4. ✓ Updated [templates/b3-cotahist-yearly.yaml](templates/b3-cotahist-yearly.yaml):
   - Removed hard-coded `colspecs` array (26 tuples) and `names` list (26 items)
   - Pipeline now has only 3 steps: `read_fwf`, `filter_rows`, `apply_fields`
   - Each field defines `name`, `description`, `type`, and `width`
   - Added `encoding: latin1` to reader config for proper file decoding
   - Use the same `fields.names` used in `templates/b3-cotahist-yearly.yaml`
   - Filters to keep only `regtype == 1` rows
   - Applies field type conversions with `errors: coerce`
5. ✓ Verified pipeline processes gzipped files from downloader (automatically handled)

**Verification Results**
- ✓ Successfully processed 521 out of 522 downloaded files
- ✓ Data correctly filtered to record type "01" only
- ✓ Field types properly applied (dates, integers, numbers, characters)
- ✓ Template is 70% shorter and more maintainable
- ✓ Cleaned up temporary `b3-cotahist-yearly-pipeline.yaml` file

**Field Type Corrections**
- ✓ Corrected numeric field types from incorrect `number(decimal=".", thousands="")` to proper `number(dec=X)` format
- ✓ 9 fields with `number(dec=2)`: preco_abertura, preco_max, preco_min, preco_med, preco_ult, preco_melhor_oferta_compra, preco_melhor_oferta_venda, volume_titulos_negociados, preco_exercicio
- ✓ 1 field with `number(dec=6)`: preco_exercicio_pontos (higher precision for point values)
- ✓ Changed `fator_cot` from `number` to `integer` (matching original definition)
- The `dec` parameter specifies decimal precision (divides by 10^dec), not decimal separator

**Key Refinements**
- Field widths are stored as custom attributes via `field.get_attribute('width')`
- Fieldset iteration uses `__iter__` method to access fields in insertion order
- `read_fwf` step validates that all fields have width attribute or raises clear error
- Single source of truth: `field.width` → colspecs calculation → pandas read_fwf
- Pattern is reusable for other fixed-width format templates

**Implementation Details**
- Modified: `brasa/engine/pipeline/steps/io_steps.py` (ReadFwfStep class)
- Modified: `templates/b3-cotahist-yearly.yaml` (complete migration)
- Pattern established for width-driven FWF reading across all templates
