# Dependency Graph

Every brasa template sits in a pipeline where download templates fetch raw data
and ETL templates transform it into curated datasets. The `graph` and `deps` CLI
commands let you inspect those relationships so you understand what runs before
what, and why.

## Commands

| Command | Purpose |
|---------|---------|
| `graph` | Visualise the full graph or a template's subgraph |
| `deps`  | Show direct/ancestor/downstream links for one template |

---

## `graph` — visualise the dependency graph

```
usage: python -m brasa.cli graph [-h] [--format FORMAT] [--output FILE] [--template NAME]

options:
  --format FORMAT   output format: dot (default), ascii, png, svg, pdf
  --output FILE     write output to file (default: stdout for text formats)
  --template NAME   show only the subgraph for a specific template
```

### Print an ASCII tree

The quickest way to explore a template's ancestry in the terminal:

```bash
poetry run python -m brasa.cli graph --template b3-equities-returns --format ascii
```

```
[D] = download   [E] = etl/processing

[E] b3-equities-returns
├── [D] b3-bvbg086
├── [E] b3-cotahist
│   ├── [D] b3-cotahist-daily
│   └── [D] b3-cotahist-yearly
├── [E] b3-equities-spot-market
│   └── [E] b3-equities-register
│       └── [D] b3-bvbg028
└── [E] b3-listed-funds
    ├── [D] b3-listed-cripto-etfs
    ├── [D] b3-listed-fixed-income-etfs
    ├── [D] b3-listed-reits
    └── [D] b3-listed-stock-etfs
```

`[D]` marks a **download** template (leaf node, fetches raw data).
`[E]` marks an **ETL** template (transforms or aggregates upstream data).

### Export the DOT spec

The default format outputs a [Graphviz DOT](https://graphviz.org/doc/info/lang.html)
string, suitable for further processing or storage:

```bash
# Print to stdout
poetry run python -m brasa.cli graph --template b3-equities-returns

# Save to a file
poetry run python -m brasa.cli graph --template b3-equities-returns --output graph.dot
```

### Render an image with `dot`

The `png`, `svg`, and `pdf` formats require the
[Graphviz](https://graphviz.org/) system package. Install it with:

```bash
sudo apt install graphviz   # Debian / Ubuntu
brew install graphviz       # macOS
```

Then render directly from the CLI:

```bash
# PNG
poetry run python -m brasa.cli graph --template b3-equities-returns \
    --format png --output graph.png

# SVG (scales well in browsers and reports)
poetry run python -m brasa.cli graph --template b3-equities-returns \
    --format svg --output graph.svg

# PDF
poetry run python -m brasa.cli graph --template b3-equities-returns \
    --format pdf --output graph.pdf
```

### Render manually by piping to `dot`

You can pipe the DOT output to the `dot` command yourself for more control over
layout and styling:

```bash
# PNG via pipe
poetry run python -m brasa.cli graph --template b3-equities-returns \
    | dot -Tpng -o graph.png

# SVG with a different layout engine (neato, fdp, circo, twopi…)
poetry run python -m brasa.cli graph --template b3-equities-returns \
    | dot -Tsvg -Kneato -o graph.svg

# Open directly without saving (requires imagemagick)
poetry run python -m brasa.cli graph --template b3-equities-returns \
    | dot -Tpng | display
```

### Full graph (all templates)

Omit `--template` to include every template in the registry:

```bash
poetry run python -m brasa.cli graph --format ascii
poetry run python -m brasa.cli graph --format svg --output full-graph.svg
```

---

## `deps` — inspect a single template's links

```bash
poetry run python -m brasa.cli deps b3-equities-returns
```

```
Template: b3-equities-returns (etl)
Outputs: staging/b3-equities-returns

Direct upstream (4):
  b3-listed-funds (etl)
  b3-equities-spot-market (etl)
  b3-cotahist (etl)
  b3-bvbg086 (download)

All ancestors (12):
  b3-bvbg028 (download)
  b3-bvbg086 (download)
  b3-cotahist (etl)
  b3-cotahist-daily (download)
  b3-cotahist-yearly (download)
  b3-equities-register (etl)
  b3-equities-spot-market (etl)
  b3-listed-cripto-etfs (download)
  b3-listed-fixed-income-etfs (download)
  b3-listed-funds (etl)
  b3-listed-reits (download)
  b3-listed-stock-etfs (download)

Direct downstream: (none)
```

`deps` is useful for quickly answering:

- *What raw files does this dataset ultimately depend on?* → check **All ancestors**
- *Which other templates consume this one?* → check **Direct downstream**
- *Where is the output stored?* → check **Outputs**

---

## Reading the graph

- **Left-to-right** flow: upstream (sources) on the left, downstream
  (consumers) on the right.
- **Light blue nodes** in rendered images are download templates.
- **Light yellow nodes** are ETL templates.
- A node with no incoming edges has no dependencies (it downloads directly from
  an external source).
- A node with no outgoing edges is a terminal dataset (nothing depends on it
  yet, or it is the query target).
