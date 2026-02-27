"""Template dependency graph for automatic upstream processing.

This module builds a directed acyclic graph (DAG) from brasa templates,
enabling automatic discovery and ordering of upstream dependencies for
ETL pipelines.

The graph has two types of nodes:

- **Source nodes** (download templates): Templates with ``downloader:`` +
  ``reader:`` sections that produce datasets in the ``input`` layer.
  They have no upstream template dependencies.
- **ETL nodes** (transform templates): Templates with ``etl:`` section
  that consume datasets and produce derived datasets.

Only pipeline-based templates (``reader.pipeline`` or ``etl.pipeline``)
are included.  Legacy function-based templates are excluded.

Classes:
    DatasetOutput: Describes a dataset produced by a template.
    ExecutionStep: A single step in an execution plan.
    ExecutionPlan: An ordered plan of steps for processing a template.
    TemplateDependencyGraph: Scans templates and builds the DAG with
        reverse index, edge map, and dependency queries.
"""

from __future__ import annotations

import bisect
import json
import logging
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from .cache import CacheManager
from .template import (
    MarketDataTemplate,
    list_templates,
    retrieve_template,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatasetOutput:
    """A dataset produced by a template.

    Attributes:
        dataset_id: Full qualified id ``layer/dataset-name``.
        layer: The data layer (e.g. ``input``, ``staging``).
        dataset_name: The bare dataset name (no layer prefix).
        template_id: The template that produces this dataset.
    """

    dataset_id: str
    layer: str
    dataset_name: str
    template_id: str


class CyclicDependencyError(Exception):
    """Raised when a cycle is detected in the template dependency graph."""


@dataclass(frozen=True)
class ExecutionStep:
    """A single step in an execution plan.

    Attributes:
        template_id: The template to execute.
        action: What action to take for this step.
        reason: Human-readable explanation of why this action was chosen.
        template_type: Whether this is a ``download`` or ``etl`` template.
    """

    template_id: str
    action: Literal["download", "process", "etl", "skip"]
    reason: str
    template_type: Literal["download", "etl"]


@dataclass
class ExecutionPlan:
    """An ordered plan of execution steps for processing a template.

    Attributes:
        target_template: The template that was requested.
        steps: Ordered list of ``ExecutionStep`` instances, from
            upstream sources to the target.
    """

    target_template: str
    steps: list[ExecutionStep] = field(default_factory=list)

    @property
    def steps_to_execute(self) -> list[ExecutionStep]:
        """Return only the steps that require execution (not skipped)."""
        return [s for s in self.steps if s.action != "skip"]

    @property
    def steps_to_skip(self) -> list[ExecutionStep]:
        """Return only the steps that will be skipped."""
        return [s for s in self.steps if s.action == "skip"]

    def __str__(self) -> str:
        lines = [f"Execution plan for '{self.target_template}':"]
        for i, step in enumerate(self.steps, 1):
            marker = "SKIP" if step.action == "skip" else step.action.upper()
            lines.append(
                f"  {i}. [{marker}] {step.template_id} "
                f"({step.template_type}) — {step.reason}"
            )
        total = len(self.steps)
        execute = len(self.steps_to_execute)
        skip = len(self.steps_to_skip)
        lines.append(f"  Total: {total} steps, {execute} to execute, {skip} to skip")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Dependency graph
# ---------------------------------------------------------------------------


class TemplateDependencyGraph:
    """Builds and queries a DAG of template dependencies.

    Construction scans every template returned by ``list_templates()`` and
    keeps only those with ``reader.has_pipeline`` (download templates) or
    ``etl.is_pipeline`` (ETL templates).  Legacy function-based templates
    are silently skipped.

    Attributes:
        templates: Mapping of template_id → ``MarketDataTemplate``.
        outputs: Mapping of template_id → list of ``DatasetOutput``.
        reverse_index: Mapping of ``layer/dataset-name`` → template_id.
        edges: Mapping of template_id → list of upstream template_ids.
        dependency_refs: Mapping of template_id → list of raw dataset
            reference strings (before resolution).
    """

    def __init__(self) -> None:
        self.templates: dict[str, MarketDataTemplate] = {}
        self.outputs: dict[str, list[DatasetOutput]] = {}
        self.reverse_index: dict[str, str] = {}
        self.edges: dict[str, list[str]] = {}
        self.dependency_refs: dict[str, list[str]] = {}

        self._build()

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def _build(self) -> None:
        """Scan all templates and build the full dependency graph."""
        self._load_templates()
        self._build_all_outputs()
        self._build_reverse_index()
        self._build_all_dependencies()
        self._build_template_edges()
        self._validate_no_cycles()

    def _load_templates(self) -> None:
        """Load all pipeline-based templates, skipping legacy ones."""
        for name in list_templates():
            try:
                tmpl = retrieve_template(name)
            except Exception:
                logger.warning(
                    "Skipping template '%s': failed to load", name, exc_info=True
                )
                continue

            if self._is_pipeline_template(tmpl):
                self.templates[tmpl.id] = tmpl
            else:
                logger.debug("Skipping legacy/non-pipeline template '%s'", name)

    @staticmethod
    def _is_pipeline_template(tmpl: MarketDataTemplate) -> bool:
        """Return True if the template uses a pipeline-based reader or ETL.

        Args:
            tmpl: The template to check.

        Returns:
            True for pipeline-based templates, False otherwise.
        """
        if tmpl.has_reader and tmpl.reader.has_pipeline:
            return True
        return tmpl.is_etl and hasattr(tmpl, "etl") and tmpl.etl.is_pipeline

    # ------------------------------------------------------------------
    # Output discovery (TASK-002)
    # ------------------------------------------------------------------

    def _build_all_outputs(self) -> None:
        """Discover outputs for every loaded template."""
        for tid, tmpl in self.templates.items():
            self.outputs[tid] = self._discover_outputs(tmpl)

    @staticmethod
    def _discover_outputs(
        template: MarketDataTemplate,
    ) -> list[DatasetOutput]:
        """Discover the datasets produced by *template*.

        Args:
            template: A loaded ``MarketDataTemplate``.

        Returns:
            List of ``DatasetOutput`` instances.
        """
        writer = template.writer
        layer = writer.layer.value

        # Multi-output download templates
        if (
            template.has_reader
            and hasattr(template, "datasets")
            and template.datasets is not None
        ):
            results: list[DatasetOutput] = []
            for dataset_key in template.datasets:
                ds_name = f"{template.id}-{dataset_key}"
                results.append(
                    DatasetOutput(
                        dataset_id=f"{layer}/{ds_name}",
                        layer=layer,
                        dataset_name=ds_name,
                        template_id=template.id,
                    )
                )
            return results

        # Single-output (download or ETL)
        ds_name = writer.dataset
        return [
            DatasetOutput(
                dataset_id=f"{layer}/{ds_name}",
                layer=layer,
                dataset_name=ds_name,
                template_id=template.id,
            )
        ]

    # ------------------------------------------------------------------
    # Dependency discovery (TASK-003)
    # ------------------------------------------------------------------

    def _build_all_dependencies(self) -> None:
        """Discover raw dependency refs for every loaded template."""
        for tid, tmpl in self.templates.items():
            self.dependency_refs[tid] = self._discover_dependencies(tmpl)

    @staticmethod
    def _discover_dependencies(
        template: MarketDataTemplate,
    ) -> list[str]:
        """Discover the dataset references that *template* depends on.

        For download templates (source nodes) this always returns an
        empty list.  For pipeline-based ETL templates it delegates to
        ``ETLPipeline.get_input_datasets()``.

        Args:
            template: A loaded ``MarketDataTemplate``.

        Returns:
            List of raw dataset reference strings (e.g.
            ``"input.b3-bvbg028-equities"`` or ``"b3-futures-settlement-prices"``).
        """
        # Download templates are source nodes — no upstream deps
        if template.has_reader:
            return []

        # ETL templates
        if template.is_etl and hasattr(template, "etl") and template.etl.is_pipeline:
            return template.etl.get_input_datasets()

        return []

    # ------------------------------------------------------------------
    # Dataset reference normalisation (TASK-004)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_dataset_ref(ref: str) -> tuple[str, str]:
        """Parse a dataset reference into ``(layer, dataset_name)``.

        Handles two formats:

        * ``"layer.dataset-name"`` → ``(layer, dataset-name)``
        * ``"dataset-name"``       → ``("input", dataset-name)``

        Args:
            ref: A raw dataset reference string.

        Returns:
            Tuple of ``(layer, dataset_name)``.
        """
        if "." in ref:
            layer, dataset_name = ref.split(".", 1)
            return (layer, dataset_name)
        # Bare name — default to input layer
        return ("input", ref)

    # ------------------------------------------------------------------
    # Reverse index (TASK-005)
    # ------------------------------------------------------------------

    def _build_reverse_index(self) -> None:
        """Build the reverse index mapping dataset_id → template_id.

        Raises:
            ValueError: If two templates produce the same dataset.
        """
        for tid, ds_outputs in self.outputs.items():
            for ds_out in ds_outputs:
                existing = self.reverse_index.get(ds_out.dataset_id)
                if existing is not None and existing != tid:
                    raise ValueError(
                        f"Duplicate dataset producer for "
                        f"'{ds_out.dataset_id}': templates "
                        f"'{existing}' and '{tid}' both produce it."
                    )
                self.reverse_index[ds_out.dataset_id] = tid

    # ------------------------------------------------------------------
    # Edge building (TASK-006)
    # ------------------------------------------------------------------

    def _build_template_edges(self) -> None:
        """Connect each template to its upstream producers via the reverse index.

        Populates ``self.edges`` mapping ``template_id`` →
        ``[upstream_template_ids]``.

        Logs a warning when a dataset dependency cannot be resolved to a
        producing template (e.g. produced by a legacy template outside the
        graph).
        """
        for tid, refs in self.dependency_refs.items():
            upstreams: list[str] = []
            for ref in refs:
                layer, ds_name = self._normalize_dataset_ref(ref)
                ds_id = f"{layer}/{ds_name}"
                producer = self.reverse_index.get(ds_id)
                if producer is None:
                    logger.warning(
                        "Template '%s' depends on dataset '%s' which has "
                        "no known producer in the dependency graph. "
                        "It may be produced by a legacy template.",
                        tid,
                        ds_id,
                    )
                elif producer != tid and producer not in upstreams:
                    upstreams.append(producer)
            self.edges[tid] = upstreams

    def _validate_no_cycles(self) -> None:
        """Check for cycles and raise if any are found.

        Raises:
            CyclicDependencyError: If the graph contains cycles.
        """
        cycles = self.detect_cycles()
        if cycles:
            cycle_strs = [" -> ".join(c) for c in cycles]
            raise CyclicDependencyError(
                f"Circular dependencies detected in template graph: "
                f"{'; '.join(cycle_strs)}"
            )

    # ------------------------------------------------------------------
    # Public query helpers
    # ------------------------------------------------------------------

    def get_upstream(self, template_id: str) -> list[str]:
        """Return the direct upstream template ids for *template_id*.

        Args:
            template_id: The template to query.

        Returns:
            List of direct upstream template ids.

        Raises:
            KeyError: If *template_id* is not in the graph.
        """
        if template_id not in self.templates:
            raise KeyError(f"Template '{template_id}' is not in the dependency graph.")
        return list(self.edges.get(template_id, []))

    def get_downstream(self, template_id: str) -> list[str]:
        """Return the direct downstream template ids for *template_id*.

        Args:
            template_id: The template to query.

        Returns:
            List of direct downstream template ids.

        Raises:
            KeyError: If *template_id* is not in the graph.
        """
        if template_id not in self.templates:
            raise KeyError(f"Template '{template_id}' is not in the dependency graph.")
        return [tid for tid, ups in self.edges.items() if template_id in ups]

    def get_template_type(self, template_id: str) -> str:
        """Return ``'download'`` or ``'etl'`` for the given template.

        Args:
            template_id: The template to classify.

        Returns:
            ``'download'`` for reader-based templates,
            ``'etl'`` for ETL templates.

        Raises:
            KeyError: If *template_id* is not in the graph.
        """
        if template_id not in self.templates:
            raise KeyError(f"Template '{template_id}' is not in the dependency graph.")
        tmpl = self.templates[template_id]
        if tmpl.is_etl:
            return "etl"
        return "download"

    def get_outputs(self, template_id: str) -> list[DatasetOutput]:
        """Return the datasets produced by *template_id*.

        Args:
            template_id: The template to query.

        Returns:
            List of ``DatasetOutput`` instances.

        Raises:
            KeyError: If *template_id* is not in the graph.
        """
        if template_id not in self.outputs:
            raise KeyError(f"Template '{template_id}' is not in the dependency graph.")
        return list(self.outputs[template_id])

    def get_producer(self, dataset_id: str) -> str | None:
        """Return the template that produces *dataset_id*, or None.

        Args:
            dataset_id: Full-qualified dataset id ``layer/dataset-name``.

        Returns:
            Template id string or ``None``.
        """
        return self.reverse_index.get(dataset_id)

    def topological_sort(self, template_id: str) -> list[str]:
        """Return templates in topological order for processing *template_id*.

        Uses Kahn's algorithm (BFS-based) scoped to the subgraph
        consisting of *template_id* and all its transitive ancestors.
        Source templates (no upstream dependencies within the subgraph)
        come first; *template_id* comes last.

        Args:
            template_id: The target template to process.

        Returns:
            Ordered list of template ids, from sources to target.

        Raises:
            KeyError: If *template_id* is not in the graph.
            CyclicDependencyError: If a cycle is detected in the
                subgraph.
        """
        if template_id not in self.templates:
            raise KeyError(f"Template '{template_id}' is not in the dependency graph.")

        # Scope to the relevant subgraph
        ancestors = self.get_ancestors(template_id)
        subgraph_nodes = ancestors | {template_id}

        # Build in-degree map and adjacency restricted to subgraph
        in_degree: dict[str, int] = dict.fromkeys(subgraph_nodes, 0)
        for tid in subgraph_nodes:
            for upstream in self.edges.get(tid, []):
                if upstream in subgraph_nodes:
                    in_degree[tid] += 1

        # Kahn's algorithm
        queue = sorted(tid for tid, deg in in_degree.items() if deg == 0)
        result: list[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            # Find nodes in the subgraph that depend on this node
            for tid in subgraph_nodes:
                if node in self.edges.get(tid, []):
                    in_degree[tid] -= 1
                    if in_degree[tid] == 0:
                        bisect.insort(queue, tid)

        if len(result) != len(subgraph_nodes):
            # Some nodes still have non-zero in-degree → cycle
            remaining = subgraph_nodes - set(result)
            raise CyclicDependencyError(
                f"Cyclic dependency detected among templates: {sorted(remaining)}"
            )

        return result

    def detect_cycles(self) -> list[list[str]]:
        """Detect all cycles in the dependency graph using DFS.

        Uses a three-color DFS (white/gray/black) to find back edges
        that indicate cycles.

        Returns:
            List of cycles, where each cycle is a list of template ids
            forming the cycle path.  Returns an empty list if the graph
            is acyclic.
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[str, int] = dict.fromkeys(self.templates, WHITE)
        parent: dict[str, str | None] = dict.fromkeys(self.templates)
        cycles: list[list[str]] = []

        def _dfs(node: str) -> None:
            color[node] = GRAY
            for upstream in self.edges.get(node, []):
                if upstream not in color:
                    continue
                if color[upstream] == GRAY:
                    # Back edge found — reconstruct the cycle
                    cycle = [upstream, node]
                    current = node
                    while (
                        parent.get(current) is not None and parent[current] != upstream
                    ):
                        current = parent[current]  # type: ignore[assignment]
                        cycle.insert(1, current)
                    cycles.append(cycle)
                elif color[upstream] == WHITE:
                    parent[upstream] = node
                    _dfs(upstream)
            color[node] = BLACK

        for tid in sorted(self.templates.keys()):
            if color[tid] == WHITE:
                _dfs(tid)

        return cycles

    def get_ancestors(self, template_id: str) -> set[str]:
        """Return all transitive upstream templates for *template_id*.

        Performs a breadth-first traversal of the edge map to collect
        every template that *template_id* transitively depends on.

        Args:
            template_id: The template to query.

        Returns:
            Set of upstream template ids (does not include *template_id*
            itself).

        Raises:
            KeyError: If *template_id* is not in the graph.
        """
        if template_id not in self.templates:
            raise KeyError(f"Template '{template_id}' is not in the dependency graph.")
        visited: set[str] = set()
        queue = list(self.edges.get(template_id, []))
        while queue:
            tid = queue.pop(0)
            if tid in visited:
                continue
            visited.add(tid)
            queue.extend(self.edges.get(tid, []))
        return visited

    def get_descendants(self, template_id: str) -> set[str]:
        """Return all transitive downstream templates for *template_id*.

        Performs a breadth-first traversal to collect every template
        that transitively depends on *template_id*.

        Args:
            template_id: The template to query.

        Returns:
            Set of downstream template ids (does not include
            *template_id* itself).

        Raises:
            KeyError: If *template_id* is not in the graph.
        """
        if template_id not in self.templates:
            raise KeyError(f"Template '{template_id}' is not in the dependency graph.")
        # Build a reverse adjacency list (child → parents becomes parent → children)
        children: dict[str, list[str]] = {tid: [] for tid in self.templates}
        for tid, upstreams in self.edges.items():
            for up in upstreams:
                children[up].append(tid)

        visited: set[str] = set()
        queue = list(children.get(template_id, []))
        while queue:
            tid = queue.pop(0)
            if tid in visited:
                continue
            visited.add(tid)
            queue.extend(children.get(tid, []))
        return visited

    # ------------------------------------------------------------------
    # Staleness detection (Phase 3)
    # ------------------------------------------------------------------

    def _check_download_template_staleness(self, template_id: str) -> bool:
        """Check if a download template has unprocessed raw files.

        Queries the ``cache_metadata`` SQLite table for entries where
        ``template = template_id`` and ``processed_files`` is empty or
        represents an empty collection.

        Args:
            template_id: A download template id.

        Returns:
            ``True`` if there are unprocessed downloads (stale),
            ``False`` if all downloads have been processed or no
            downloads exist.
        """
        cache = CacheManager()
        with closing(cache.meta_db_connection) as conn, conn:
            c = conn.cursor()
            c.execute(
                "SELECT processed_files FROM cache_metadata WHERE template = ?",
                (template_id,),
            )
            rows = c.fetchall()

        if not rows:
            # No cache entries at all — nothing downloaded yet
            return False

        for (processed_files_json,) in rows:
            if not processed_files_json:
                return True
            # processed_files is stored as JSON; empty dict/list means unprocessed
            try:
                parsed = json.loads(processed_files_json)
            except (json.JSONDecodeError, TypeError):
                return True
            if not parsed:
                return True

        return False

    def _check_etl_template_staleness(self, template_id: str) -> bool:
        """Check if an ETL template's output is stale.

        An ETL output is stale if:

        * The output dataset directory does not exist or contains no
          ``.parquet`` files.
        * The output parquet files are older than any upstream dataset's
          parquet files.

        Args:
            template_id: An ETL template id.

        Returns:
            ``True`` if the output is stale and needs reprocessing,
            ``False`` if the output is fresh.
        """
        cache = CacheManager()

        # Determine output dataset path
        output_list = self.outputs.get(template_id, [])
        if not output_list:
            return True

        ds_out = output_list[0]
        output_dir = Path(cache.db_path(ds_out.dataset_id))

        # Check if output exists and has parquet files
        if not output_dir.exists():
            return True
        parquet_files = list(output_dir.rglob("*.parquet"))
        if not parquet_files:
            return True

        # Get the oldest output modification time
        output_mtime = min(f.stat().st_mtime for f in parquet_files)

        # Compare against upstream datasets
        for upstream_tid in self.edges.get(template_id, []):
            upstream_outputs = self.outputs.get(upstream_tid, [])
            for up_ds_out in upstream_outputs:
                up_dir = Path(cache.db_path(up_ds_out.dataset_id))
                if not up_dir.exists():
                    continue
                up_parquets = list(up_dir.rglob("*.parquet"))
                if not up_parquets:
                    continue
                # If any upstream file is newer than the output, it's stale
                newest_upstream = max(f.stat().st_mtime for f in up_parquets)
                if newest_upstream > output_mtime:
                    return True

        return False

    def get_execution_plan(
        self,
        template_id: str,
        force: bool = False,
    ) -> ExecutionPlan:
        """Compute an execution plan for processing *template_id*.

        Determines the topological order of all ancestors plus the
        target, checks staleness for each, and returns an
        ``ExecutionPlan`` describing which steps need execution.

        Args:
            template_id: The target template to process.
            force: If ``True``, all ancestors are marked for execution
                regardless of staleness.

        Returns:
            An ``ExecutionPlan`` with ordered ``ExecutionStep`` entries.

        Raises:
            KeyError: If *template_id* is not in the graph.
        """
        ordered = self.topological_sort(template_id)
        plan = ExecutionPlan(target_template=template_id)

        for tid in ordered:
            tmpl_type = self.get_template_type(tid)

            if force:
                if tmpl_type == "download":
                    action: Literal["download", "process", "etl", "skip"] = "process"
                    reason = "forced execution"
                else:
                    action = "etl"
                    reason = "forced execution"
            elif tmpl_type == "download":
                if self._check_download_template_staleness(tid):
                    action = "process"
                    reason = "unprocessed downloads detected"
                else:
                    action = "skip"
                    reason = "all downloads already processed"
            elif self._check_etl_template_staleness(tid):
                action = "etl"
                reason = "output missing or outdated"
            else:
                action = "skip"
                reason = "output is up to date"

            plan.steps.append(
                ExecutionStep(
                    template_id=tid,
                    action=action,
                    reason=reason,
                    template_type=tmpl_type,
                )
            )

        return plan

    @property
    def template_ids(self) -> list[str]:
        """Return a sorted list of all template ids in the graph."""
        return sorted(self.templates.keys())

    def __len__(self) -> int:
        return len(self.templates)

    def __contains__(self, template_id: str) -> bool:
        return template_id in self.templates

    def __repr__(self) -> str:
        return (
            f"TemplateDependencyGraph("
            f"templates={len(self.templates)}, "
            f"datasets={len(self.reverse_index)}, "
            f"edges={sum(len(v) for v in self.edges.values())})"
        )
