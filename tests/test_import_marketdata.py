import brasa
from brasa.engine.api import _build_result_skipped
from brasa.engine.cache import CacheManager, CacheMetadata


def test_build_result_skipped_uses_operation_label():
    cache = CacheManager()
    meta = CacheMetadata("dummy-template")
    meta.downloaded_files = []
    result = _build_result_skipped(
        cache,
        meta,
        "dummy-template",
        {"refdate": "2026-06-20"},
        0.1,
        operation="import",
    )
    assert result.operation == "import"


def test_run_acquisition_sets_operation_on_report(tmp_path):
    from functools import partial

    from brasa.downloaders import local_file_import
    from brasa.engine.api import _run_acquisition
    from brasa.engine.template import retrieve_template

    # A real file so local_file_import succeeds on its one iteration
    src = tmp_path / "x.zip"
    src.write_bytes(b"dummy")
    acq = partial(local_file_import, _import_path=str(src))

    template = retrieve_template("b3-cotahist-yearly")
    report = _run_acquisition(
        template,
        "b3-cotahist-yearly",
        {},  # KwargsIterator({}) yields one {}, no network needed
        operation="import",
        force=False,
        verbosity=brasa.Verbosity.QUIET,
        report_file=None,
        acquisition_function=acq,
        retry_attempts_override=0,
    )
    assert report.operation == "import"
