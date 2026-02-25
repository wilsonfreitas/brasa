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
    TemplateDependencyGraph: Scans templates and builds the DAG with
        reverse index, edge map, and dependency queries.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

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
