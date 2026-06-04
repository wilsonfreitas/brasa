"""Tests for the anbima-index-imab template (WIL-66)."""

from datetime import datetime

import pandas as pd

from brasa.engine.template import retrieve_template


def test_imab_downloader_has_extra_key_date():
    """Downloader uses extra-key: date so each day yields a distinct cache entry."""
    tpl = retrieve_template("anbima-index-imab")
    assert tpl.downloader._extra_key == "date"
    assert tpl.downloader.extra_key == datetime.now().isoformat()[:10]


def test_imab_writer_has_no_partitioning():
    """The single-index history is written flat (no index_name partitioning)."""
    tpl = retrieve_template("anbima-index-imab")
    assert tpl.writer.partitioning == []


def test_imab_pipeline_normalizes_index_name_label():
    """The reader pipeline normalizes 'IMA - Geral' to the canonical 'IMA-GERAL'."""
    tpl = retrieve_template("anbima-index-imab")
    str_replace_steps = [
        s
        for s in tpl.reader._pipeline.steps
        if s.__class__.__name__ == "StrReplaceStep"
    ]
    assert len(str_replace_steps) == 1
    step = str_replace_steps[0]

    df = pd.DataFrame({"index_name": ["IMA-GERAL", "IMA - Geral"]})
    result = step.execute(df, None)
    assert result["index_name"].unique().tolist() == ["IMA-GERAL"]
