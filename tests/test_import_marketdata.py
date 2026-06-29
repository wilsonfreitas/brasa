import brasa
from brasa.engine.api import _build_result_skipped
from brasa.engine.cache import CacheManager, CacheMetadata
from brasa.util import DownloadArgs


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


def test_import_marketdata_end_to_end_copies_and_records_provenance(tmp_path):
    from brasa.engine.template import MarketDataTemplate, _template_cache

    tpl_yaml = tmp_path / "test-import-csv.yaml"
    tpl_yaml.write_text(
        "id: test-import-csv\n"
        "downloader:\n"
        "  function: brasa.downloaders.simple_download\n"
        "  url: http://example.invalid\n"
        "  format: csv\n"
        "  args:\n"
        "    refdate: ~\n"
    )
    _template_cache["test-import-csv"] = MarketDataTemplate(str(tpl_yaml))

    src = tmp_path / "src.csv"
    src.write_text("symbol,price\nPETR4,10\n")

    report = brasa.import_marketdata(
        "test-import-csv",
        path=str(src),
        refdate="2026-06-20",
        verbosity=brasa.Verbosity.QUIET,
    )

    assert report.operation == "import"
    assert any(r.status.name == "PASSED" for r in report.results)
    # copy-only: the source is untouched
    assert src.exists()

    # provenance recorded in meta.response
    cache = CacheManager()
    meta = CacheMetadata("test-import-csv")
    meta.extra_key = ""
    meta.download_args = DownloadArgs({"refdate": "2026-06-20"})
    cache.load_meta(meta)
    assert meta.response["acquisition"] == "import"
    assert meta.response["original_name"] == "src.csv"

    _template_cache.pop("test-import-csv", None)
