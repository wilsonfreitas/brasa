from brasa import cli


def test_import_subcommand_parses():
    args = cli.parser.parse_args(
        [
            "import",
            "b3-cotahist-yearly",
            "--path",
            "/tmp/x.csv",
            "--arg",
            "refdate=2026",
        ]
    )
    assert args.command == "import"
    assert args.template == ["b3-cotahist-yearly"]
    assert args.path == "/tmp/x.csv"
    assert args.arg == ["refdate=2026"]


def test_import_dispatch_calls_import_marketdata(monkeypatch):
    from brasa.engine import Verbosity
    from brasa.engine.reporting import TaskReport

    captured = {}

    def fake_import(template, **kw):
        captured["template"] = template
        captured["kw"] = kw
        report = TaskReport(
            operation="import", template_name=template, verbosity=Verbosity.QUIET
        )
        report.start(total=0)
        report.finish()
        return report

    monkeypatch.setattr(cli, "import_marketdata", fake_import)
    monkeypatch.setattr(
        "sys.argv",
        [
            "brasa",
            "import",
            "tpl",
            "--path",
            "/tmp/x.csv",
            "--arg",
            "refdate=2026-06-20",
            "-q",
        ],
    )
    cli.main()

    from datetime import datetime

    assert captured["template"] == "tpl"
    assert captured["kw"]["path"] == "/tmp/x.csv"
    # _parse_download_args converts date strings to datetime; single date → list
    assert captured["kw"]["refdate"] == [datetime(2026, 6, 20)]
