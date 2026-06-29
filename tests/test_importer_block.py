from brasa.downloaders import local_file_import
from brasa.engine.template import MarketDataTemplate


def test_importer_block_parses_with_default_function(tmp_path):
    tpl_yaml = tmp_path / "imp.yaml"
    tpl_yaml.write_text(
        "id: imp\n"
        "importer:\n"
        "  path: /data/%Y-%m-%d.csv\n"
        "  format: csv\n"
        "  args:\n"
        "    refdate: ~\n"
    )
    tpl = MarketDataTemplate(str(tpl_yaml))

    assert tpl.has_downloader is True
    assert tpl.downloader.path == "/data/%Y-%m-%d.csv"
    assert tpl.downloader.format == "csv"
    assert tpl.downloader.download_function is local_file_import
