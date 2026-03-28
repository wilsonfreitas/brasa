"""Unit tests for the DependencyResolver.

Tests cover:
- Skip-if-provided: resolver does nothing when caller supplies the arg
- Happy path: dependency runs, SQL returns values, args are injected
- Required failure — execution error: DependencyResolutionError raised
- Required failure — SQL returns no rows: DependencyResolutionError raised
- Optional failure — SQL error: warning logged, continues
- Optional failure — query returns nothing: warning logged, continues
- Unknown dataset in reverse_index: fail-fast DependencyResolutionError
- _run_upstream_templates: ETL and download dispatch, report failure handling
- download_marketdata integration: resolved args are merged into kwargs
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from brasa.engine.dependency_resolver import resolve_dependencies
from brasa.engine.exceptions import DependencyResolutionError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_template(template_id: str, dependencies: list | None = None) -> MagicMock:
    """Create a lightweight mock template with optional dependencies."""
    tmpl = MagicMock()
    tmpl.id = template_id
    tmpl.dependencies = dependencies
    return tmpl


def _make_graph(producer: str | None = "upstream-template", template_type: str = "etl"):
    """Create a mock dependency graph."""
    graph = MagicMock()
    graph.get_producer.return_value = producer
    graph.get_template_type.return_value = template_type
    # Return a non-existent path so the freshness check fails and processing runs
    graph.get_dataset_paths.return_value = ["/nonexistent/output/path"]
    graph.get_input_dataset_paths.return_value = []
    return graph


def _make_report(success: bool = True) -> MagicMock:
    report = MagicMock()
    report.success = success
    return report


# ---------------------------------------------------------------------------
# skip-if-provided
# ---------------------------------------------------------------------------


def test_skip_when_caller_supplies_arg():
    """Resolver does nothing when caller already provides the arg."""
    tmpl = _make_template(
        "b3-company-info",
        dependencies=[
            {
                "issuingCompany": {
                    "required": True,
                    "from": {
                        "datasets": ["staging.b3-equities-instrument-assets"],
                        "query": "SELECT DISTINCT instrument_asset FROM 'staging.b3-equities-instrument-assets'",
                    },
                }
            }
        ],
    )
    # Caller provides issuingCompany — graph should never be constructed
    with patch("brasa.engine.dependency_resolver.TemplateDependencyGraph") as MockGraph:
        result = resolve_dependencies(tmpl, {"issuingCompany": ["ABEV", "ITUB"]})

    assert result == {}
    MockGraph.assert_not_called()


def test_no_dependencies_returns_empty():
    """Templates without dependencies return an empty dict."""
    tmpl = _make_template("b3-company-info", dependencies=None)
    result = resolve_dependencies(tmpl, {})
    assert result == {}


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_happy_path_resolves_values():
    """Dependency runs, SQL returns values, args are injected."""
    tmpl = _make_template(
        "b3-company-info",
        dependencies=[
            {
                "issuingCompany": {
                    "required": True,
                    "from": {
                        "datasets": ["staging.b3-equities-instrument-assets"],
                        "query": "SELECT DISTINCT instrument_asset FROM 'staging.b3-equities-instrument-assets'",
                    },
                }
            }
        ],
    )

    graph = _make_graph(producer="b3-equities-instrument-assets", template_type="etl")

    with (
        patch(
            "brasa.engine.dependency_resolver.TemplateDependencyGraph",
            return_value=graph,
        ),
        patch(
            "brasa.engine.dependency_resolver._run_sql",
            return_value=["ABEV", "ITUB", "PETR"],
        ),
        patch(
            "brasa.engine.dependency_resolver._run_upstream_templates",
        ) as mock_run_upstream,
    ):
        result = resolve_dependencies(tmpl, {})

    assert result == {"issuingCompany": ["ABEV", "ITUB", "PETR"]}
    mock_run_upstream.assert_called_once()


# ---------------------------------------------------------------------------
# Required failure — upstream execution error
# ---------------------------------------------------------------------------


def test_required_dep_raises_on_upstream_failure():
    """DependencyResolutionError raised when required upstream fails."""
    tmpl = _make_template(
        "b3-company-info",
        dependencies=[
            {
                "issuingCompany": {
                    "required": True,
                    "from": {
                        "datasets": ["staging.b3-equities-instrument-assets"],
                        "query": "SELECT x FROM 'staging.b3-equities-instrument-assets'",
                    },
                }
            }
        ],
    )

    graph = _make_graph(producer="b3-equities-instrument-assets", template_type="etl")

    with (
        patch(
            "brasa.engine.dependency_resolver.TemplateDependencyGraph",
            return_value=graph,
        ),
        patch(
            "brasa.engine.dependency_resolver._run_upstream_templates",
            side_effect=DependencyResolutionError("upstream failed"),
        ),
        pytest.raises(DependencyResolutionError, match="upstream failed"),
    ):
        resolve_dependencies(tmpl, {})


# ---------------------------------------------------------------------------
# Required failure — SQL returns no rows
# ---------------------------------------------------------------------------


def test_required_dep_raises_on_empty_sql_result():
    """DependencyResolutionError raised when SQL returns no rows."""
    tmpl = _make_template(
        "b3-company-info",
        dependencies=[
            {
                "issuingCompany": {
                    "required": True,
                    "from": {
                        "datasets": ["staging.b3-equities-instrument-assets"],
                        "query": "SELECT DISTINCT instrument_asset FROM 'staging.b3-equities-instrument-assets'",
                    },
                }
            }
        ],
    )

    graph = _make_graph(producer="b3-equities-instrument-assets", template_type="etl")

    with (
        patch(
            "brasa.engine.dependency_resolver.TemplateDependencyGraph",
            return_value=graph,
        ),
        patch(
            "brasa.engine.dependency_resolver._run_sql",
            return_value=[],
        ),
        patch("brasa.engine.dependency_resolver._run_upstream_templates"),
        pytest.raises(DependencyResolutionError, match="no rows"),
    ):
        resolve_dependencies(tmpl, {})


# ---------------------------------------------------------------------------
# Optional failure — SQL error
# ---------------------------------------------------------------------------


def test_optional_dep_continues_on_sql_failure():
    """Optional dependency: SQL failure logs warning and returns empty."""
    tmpl = _make_template(
        "b3-company-info",
        dependencies=[
            {
                "issuingCompany": {
                    "required": False,
                    "from": {
                        "datasets": ["staging.b3-equities-instrument-assets"],
                        "query": "SELECT DISTINCT instrument_asset FROM 'staging.b3-equities-instrument-assets'",
                    },
                }
            }
        ],
    )

    graph = _make_graph(producer="b3-equities-instrument-assets", template_type="etl")

    with (
        patch(
            "brasa.engine.dependency_resolver.TemplateDependencyGraph",
            return_value=graph,
        ),
        patch(
            "brasa.engine.dependency_resolver._run_sql",
            side_effect=RuntimeError("SQL failed"),
        ),
        patch("brasa.engine.dependency_resolver._run_upstream_templates"),
    ):
        result = resolve_dependencies(tmpl, {})

    # Optional failure — no injection, no raise
    assert result == {}


# ---------------------------------------------------------------------------
# Optional failure — query returns nothing
# ---------------------------------------------------------------------------


def test_optional_dep_continues_on_empty_result():
    """Optional dependency: empty SQL result logs warning and returns empty."""
    tmpl = _make_template(
        "b3-company-info",
        dependencies=[
            {
                "issuingCompany": {
                    "required": False,
                    "from": {
                        "datasets": ["staging.b3-equities-instrument-assets"],
                        "query": "SELECT DISTINCT instrument_asset FROM 'staging.b3-equities-instrument-assets'",
                    },
                }
            }
        ],
    )

    graph = _make_graph(producer="b3-equities-instrument-assets", template_type="etl")

    with (
        patch(
            "brasa.engine.dependency_resolver.TemplateDependencyGraph",
            return_value=graph,
        ),
        patch(
            "brasa.engine.dependency_resolver._run_sql",
            return_value=[],
        ),
        patch("brasa.engine.dependency_resolver._run_upstream_templates"),
    ):
        result = resolve_dependencies(tmpl, {})

    assert result == {}


# ---------------------------------------------------------------------------
# Unknown dataset in reverse_index
# ---------------------------------------------------------------------------


def test_unknown_dataset_raises_immediately():
    """Fail-fast DependencyResolutionError when dataset not in graph."""
    tmpl = _make_template(
        "b3-company-info",
        dependencies=[
            {
                "issuingCompany": {
                    "required": True,
                    "from": {
                        "datasets": ["staging.nonexistent-dataset"],
                        "query": "SELECT x FROM 'staging.nonexistent-dataset'",
                    },
                }
            }
        ],
    )

    graph = _make_graph(producer=None)  # None means not found

    with (
        patch(
            "brasa.engine.dependency_resolver.TemplateDependencyGraph",
            return_value=graph,
        ),
        pytest.raises(DependencyResolutionError, match="unknown dataset"),
    ):
        resolve_dependencies(tmpl, {})


# ---------------------------------------------------------------------------
# _run_upstream_templates — dispatch by template type
# ---------------------------------------------------------------------------


def test_run_upstream_etl_template():
    """process_etl is called for ETL upstream templates."""
    from brasa.engine.dependency_resolver import _run_upstream_templates

    graph = _make_graph(producer="upstream-etl", template_type="etl")
    mock_report = _make_report(success=True)

    with patch("brasa.engine.api.process_etl", return_value=mock_report) as mock_etl:
        _run_upstream_templates(
            "b3-company-info",
            "issuingCompany",
            ["staging.upstream-dataset"],
            graph,
            required=True,
        )

    mock_etl.assert_called_once_with("upstream-etl", resolve_dependencies=True)


def test_run_upstream_download_template():
    """process_marketdata is called for download upstream templates."""
    from brasa.engine.dependency_resolver import _run_upstream_templates

    graph = _make_graph(producer="upstream-dl", template_type="download")
    mock_report = _make_report(success=True)

    with patch(
        "brasa.engine.api.process_marketdata", return_value=mock_report
    ) as mock_dl:
        _run_upstream_templates(
            "b3-company-info",
            "issuingCompany",
            ["input.upstream-dataset"],
            graph,
            required=True,
        )

    mock_dl.assert_called_once_with("upstream-dl")


def test_run_upstream_etl_uses_resolve_dependencies():
    """process_etl is called with resolve_dependencies=True so the full
    ancestor chain (downloads, intermediate ETLs) is processed before
    the target ETL runs.

    Regression test for: b3-company-info downloads not processed before
    b3-companies-names ETL when running companies-b3.yaml download plan.
    """
    from brasa.engine.dependency_resolver import _run_upstream_templates

    graph = _make_graph(producer="b3-companies-names", template_type="etl")
    mock_report = _make_report(success=True)

    with patch("brasa.engine.api.process_etl", return_value=mock_report) as mock_etl:
        _run_upstream_templates(
            "b3-company-details",
            "codeCVM",
            ["staging.b3-companies-names"],
            graph,
            required=True,
        )

    # The critical assertion: resolve_dependencies=True ensures the orchestrator
    # processes the full chain (e.g., process_marketdata for b3-company-info
    # before running b3-companies-names ETL)
    mock_etl.assert_called_once_with("b3-companies-names", resolve_dependencies=True)


def test_run_upstream_required_raises_on_report_failure():
    """DependencyResolutionError raised when upstream report.success is False."""
    from brasa.engine.dependency_resolver import _run_upstream_templates

    graph = _make_graph(producer="upstream-etl", template_type="etl")
    mock_report = _make_report(success=False)

    with (
        patch("brasa.engine.api.process_etl", return_value=mock_report),
        pytest.raises(DependencyResolutionError, match="reported failures"),
    ):
        _run_upstream_templates(
            "b3-company-info",
            "issuingCompany",
            ["staging.upstream-dataset"],
            graph,
            required=True,
        )


def test_run_upstream_optional_continues_on_report_failure():
    """Optional upstream: warning logged on failure, no exception raised."""
    from brasa.engine.dependency_resolver import _run_upstream_templates

    graph = _make_graph(producer="upstream-etl", template_type="etl")
    mock_report = _make_report(success=False)

    with patch("brasa.engine.api.process_etl", return_value=mock_report):
        # Should not raise
        _run_upstream_templates(
            "b3-company-info",
            "issuingCompany",
            ["staging.upstream-dataset"],
            graph,
            required=False,
        )


# ---------------------------------------------------------------------------
# _run_upstream_templates — _implicit_reports collection
# ---------------------------------------------------------------------------


def test_run_upstream_appends_report_to_implicit_list():
    """When _implicit_reports list is passed, the ETL report is appended to it."""
    from brasa.engine.dependency_resolver import _run_upstream_templates

    graph = _make_graph(producer="upstream-etl", template_type="etl")
    mock_report = _make_report(success=True)
    implicit: list = []

    with patch("brasa.engine.api.process_etl", return_value=mock_report):
        _run_upstream_templates(
            "b3-company-info",
            "issuingCompany",
            ["staging.upstream-dataset"],
            graph,
            required=True,
            _implicit_reports=implicit,
        )

    assert implicit == [mock_report]


def test_run_upstream_does_not_append_when_no_list():
    """When _implicit_reports is None (default), nothing is appended anywhere."""
    from brasa.engine.dependency_resolver import _run_upstream_templates

    graph = _make_graph(producer="upstream-etl", template_type="etl")
    mock_report = _make_report(success=True)

    with patch("brasa.engine.api.process_etl", return_value=mock_report):
        # Should complete without error even when _implicit_reports is not passed
        _run_upstream_templates(
            "b3-company-info",
            "issuingCompany",
            ["staging.upstream-dataset"],
            graph,
            required=True,
        )


def test_resolve_dependencies_propagates_implicit_reports():
    """_implicit_reports list is populated after resolve_dependencies call."""
    tmpl = _make_template(
        "b3-company-info",
        dependencies=[
            {
                "issuingCompany": {
                    "required": True,
                    "from": {
                        "datasets": ["staging.b3-equities-instrument-assets"],
                        "query": "SELECT DISTINCT instrument_asset FROM 'staging.b3-equities-instrument-assets'",
                    },
                }
            }
        ],
    )

    graph = _make_graph(producer="b3-equities-instrument-assets", template_type="etl")
    mock_report = _make_report(success=True)
    implicit: list = []

    with (
        patch(
            "brasa.engine.dependency_resolver.TemplateDependencyGraph",
            return_value=graph,
        ),
        patch(
            "brasa.engine.dependency_resolver._run_sql",
            return_value=["ABEV"],
        ),
        patch(
            "brasa.engine.api.process_etl",
            return_value=mock_report,
        ),
    ):
        resolve_dependencies(tmpl, {}, _implicit_reports=implicit)

    assert implicit == [mock_report]


# ---------------------------------------------------------------------------
# download_marketdata integration
# ---------------------------------------------------------------------------


def test_download_marketdata_injects_resolved_args():
    """download_marketdata merges resolved dependency values into kwargs."""
    from brasa.engine.api import download_marketdata

    mock_template = MagicMock()
    mock_template.id = "b3-company-info"
    mock_template.downloader.extra_key = ""
    mock_template.downloader.download_delay = 0

    with (
        patch("brasa.engine.api.retrieve_template", return_value=mock_template),
        patch(
            "brasa.engine.dependency_resolver.resolve_dependencies",
            return_value={"issuingCompany": ["ABEV", "ITUB"]},
        ) as mock_resolve,
        patch("brasa.engine.api.CacheManager"),
        patch("brasa.engine.api.KwargsIterator", return_value=[]) as MockIter,
    ):
        download_marketdata("b3-company-info", language="pt-br")

    # resolve_dependencies was called with the template and original kwargs
    mock_resolve.assert_called_once()
    call_args = mock_resolve.call_args
    assert call_args[0][0] is mock_template
    # KwargsIterator received merged args (issuingCompany injected)
    merged = MockIter.call_args[0][0]
    assert merged.get("issuingCompany") == ["ABEV", "ITUB"]
    assert merged.get("language") == "pt-br"
