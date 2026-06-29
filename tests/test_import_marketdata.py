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
