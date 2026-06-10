"""Global pipeline staleness report (``brasa map``).

Walks every template in the ``TemplateDependencyGraph`` in topological order
and classifies each as ``stale``, ``never-run``, or ``ok``. Renderers turn the
classification into flat / grouped / tree output for the CLI.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from rich.console import Console
from rich.markup import escape

from .dependency_graph import TemplateDependencyGraph

Status = Literal["stale", "ok", "never-run"]
TemplateType = Literal["download", "etl"]


@dataclass(frozen=True)
class TemplateStatus:
    """Status snapshot for a single template in the pipeline map."""

    template_id: str
    template_type: TemplateType
    status: Status
    reason: str  # empty for "ok"


def build_pipeline_map(include_ok: bool = False) -> list[TemplateStatus]:
    """Topologically ordered status of every template.

    Args:
        include_ok: If True, include up-to-date templates with status ``ok``.
            Otherwise only ``stale`` and ``never-run`` entries are returned.

    Returns:
        List of ``TemplateStatus`` in topological order (sources first).
    """
    graph = TemplateDependencyGraph()
    items: list[TemplateStatus] = []
    for tid in graph.global_topological_order():
        ttype = graph.get_template_type(tid)
        if ttype == "download":
            status, reason = graph.get_download_status(tid)
        else:
            status, reason = graph.get_etl_status(tid)
        if not include_ok and status == "ok":
            continue
        items.append(
            TemplateStatus(
                template_id=tid,
                template_type=ttype,
                status=status,
                reason=reason,
            )
        )
    return items


_STATUS_STYLE = {
    "stale": "red",
    "never-run": "yellow",
    "ok": "green",
}


def _format_status(status: Status) -> str:
    """Return a rich markup span for a status value."""
    style = _STATUS_STYLE.get(status, "white")
    return f"[{style}]{status}[/{style}]"


def render_flat(items: list[TemplateStatus], console: Console) -> None:
    """Render a numbered, dependency-ordered list of templates."""
    if not items:
        console.print("[green]All up to date ✓[/green]")
        return

    width_id = max(len(it.template_id) for it in items)
    width_type = max(len(f"[{it.template_type}]") for it in items)
    for i, it in enumerate(items, 1):
        type_label = escape(f"[{it.template_type}]")
        line = (
            f"{i}. {type_label:<{width_type}}  "
            f"{it.template_id:<{width_id}}  "
            f"{_format_status(it.status)}"
        )
        if it.reason:
            line += f"  {it.reason}"
        console.print(line)


def render_grouped(
    items: list[TemplateStatus],
    console: Console,
    graph: TemplateDependencyGraph | None = None,
) -> None:
    """Render templates grouped by stage: Downloads → Staging ETLs → Curated ETLs."""
    if not items:
        console.print("[green]All up to date ✓[/green]")
        return

    if graph is None:
        graph = TemplateDependencyGraph()

    sections: dict[str, list[TemplateStatus]] = {
        "Downloads to process": [],
        "Staging ETLs": [],
        "Curated ETLs": [],
        "Other ETLs": [],
    }
    for it in items:
        if it.template_type == "download":
            sections["Downloads to process"].append(it)
            continue
        outputs = graph.get_outputs(it.template_id)
        layer = outputs[0].layer if outputs else ""
        if layer == "staging":
            sections["Staging ETLs"].append(it)
        elif layer == "curated":
            sections["Curated ETLs"].append(it)
        else:
            sections["Other ETLs"].append(it)

    first = True
    for title, group in sections.items():
        if not group:
            continue
        if not first:
            console.print()
        first = False
        console.print(f"[bold]{title}[/bold]")
        width_id = max(len(it.template_id) for it in group)
        for it in group:
            line = f"  {it.template_id:<{width_id}}  {_format_status(it.status)}"
            if it.reason:
                line += f"  {it.reason}"
            console.print(line)


def render_tree(
    items: list[TemplateStatus],
    console: Console,
    graph: TemplateDependencyGraph | None = None,
    reverse: bool = False,
) -> None:
    """Render templates as an indented tree.

    By default the tree is rooted at sources (templates with no
    upstream among ``items``) and branches downward to dependents.
    With ``reverse=True`` the tree is rooted at leaves (templates
    with no downstream among ``items``) and branches upward.

    Only templates present in ``items`` are printed — templates that
    are ``ok`` and not in ``items`` act as terminators.
    """
    if not items:
        console.print("[green]All up to date ✓[/green]")
        return

    if graph is None:
        graph = TemplateDependencyGraph()

    visible = {it.template_id for it in items}
    by_id = {it.template_id: it for it in items}

    def _children(tid: str) -> list[str]:
        if reverse:
            return [u for u in graph.get_upstream(tid) if u in visible]
        return [d for d in graph.get_downstream(tid) if d in visible]

    def _is_root(tid: str) -> bool:
        if reverse:
            return all(d not in visible for d in graph.get_downstream(tid))
        return all(u not in visible for u in graph.get_upstream(tid))

    roots = sorted(tid for tid in visible if _is_root(tid))

    def _render(tid: str, prefix: str, is_last: bool) -> None:
        connector = "└── " if is_last else "├── "
        it = by_id[tid]
        type_label = escape(f"[{it.template_type}]")
        line = (
            f"{prefix}{connector}{type_label} "
            f"{it.template_id}  {_format_status(it.status)}"
        )
        if it.reason:
            line += f"  {it.reason}"
        console.print(line)
        kids = _children(tid)
        for i, kid in enumerate(kids):
            child_is_last = i == len(kids) - 1
            extension = "    " if is_last else "│   "
            _render(kid, prefix + extension, child_is_last)

    for i, root in enumerate(roots):
        is_last = i == len(roots) - 1
        _render(root, "", is_last)
