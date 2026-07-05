"""Tests for the B3ReadBVBG086XmlStep reader.

B3's BVBG086 zip bundles occasionally contain more than one message for the
same download entry: a same-day retransmission (a later, corrected message
superseding an earlier one) and/or a redundant zip-compressed duplicate of a
message that `unzip_recursive` leaves un-extracted (a general zip-of-zips
limitation, not specific to this reader). The reader must:

- skip files that aren't parseable XML instead of crashing the pipeline
- when multiple valid messages exist, use only the most recent (by
  CreDtAndTm), not merge/concatenate them
- raise a clear error when no valid message is found at all

These cases were discovered via a real production error: BF3-derivatives... (
b3-bvbg086 refdate=2017-01-09) had two legitimate messages (18:30 and 20:16)
plus two redundant un-extracted zip duplicates, and the reader picked the
un-extracted duplicate (the last file in an arbitrary list order), crashing
with an XML parse error.
"""

import gzip

import pytest

from brasa.engine.pipeline.context import PipelineContext
from brasa.engine.pipeline.steps.b3_steps import B3ReadBVBG086XmlStep
from brasa.fieldsets import Field, Fieldset

XML_TEMPLATE = """<?xml version="1.0" encoding="utf-8"?><Document xmlns="urn:bvmf.052.01.xsd"><BizFileHdr><Xchg><BizGrpDesc><BizGrpDtls><CreDtAndTm>{creation_dt}</CreDtAndTm></BizGrpDtls></BizGrpDesc><BizGrp><Document xmlns="urn:bvmf.217.01.xsd">{pricrpts}</Document></BizGrp></Xchg></BizFileHdr></Document>"""

PRICRPT_TEMPLATE = """<PricRpt><SctyId><TckrSymb>{symbol}</TckrSymb></SctyId><FinInstrmAttrbts><LastPric>{price}</LastPric></FinInstrmAttrbts></PricRpt>"""


def _write_gzip_xml(path, content: str) -> str:
    with gzip.open(path, "wb") as f:
        f.write(content.encode("utf-8"))
    return str(path)


def _write_gzip_garbage(path) -> str:
    """Write a gzip file whose content is not valid XML (simulates an
    un-extracted zip-of-zips leftover)."""
    with gzip.open(path, "wb") as f:
        f.write(b"PK\x03\x04not-actually-xml-binary-zip-content")
    return str(path)


def _fields():
    fs = Fieldset()
    fs.add_fields(
        Field(
            name="symbol",
            description="Symbol",
            type_definition="character",
            tag="SctyId/TckrSymb",
        ),
        Field(
            name="close",
            description="Close price",
            type_definition="numeric",
            tag="FinInstrmAttrbts/LastPric",
        ),
    )
    return fs


def _context(all_files):
    class Ctx(PipelineContext):
        @property
        def all_downloaded_files(self):
            return all_files

    return Ctx(meta=None, reader_config={}, fields=_fields(), template_id="test")


class TestBVBG086Reader:
    def test_single_message_parses(self, tmp_path):
        xml = XML_TEMPLATE.format(
            creation_dt="2017-01-09T18:30:26",
            pricrpts=PRICRPT_TEMPLATE.format(symbol="PETR4", price="30.5"),
        )
        path = _write_gzip_xml(tmp_path / "msg.xml.gz", xml)

        step = B3ReadBVBG086XmlStep({"step": "b3_read_bvbg086_xml"})
        df = step.execute(None, _context([path]))

        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "PETR4"
        assert df.iloc[0]["close"] == "30.5"

    def test_skips_unparseable_leftover_and_still_succeeds(self, tmp_path):
        xml = XML_TEMPLATE.format(
            creation_dt="2017-01-09T18:30:26",
            pricrpts=PRICRPT_TEMPLATE.format(symbol="PETR4", price="30.5"),
        )
        valid = _write_gzip_xml(tmp_path / "valid.xml.gz", xml)
        garbage = _write_gzip_garbage(tmp_path / "leftover.ZIP.gz")

        step = B3ReadBVBG086XmlStep({"step": "b3_read_bvbg086_xml"})
        # Order matters for the regression: the un-parseable file is last,
        # matching the real-world case where `context.downloaded_file` (the
        # naive "last file" pick) used to select it and crash.
        df = step.execute(None, _context([valid, garbage]))

        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "PETR4"

    def test_multiple_messages_uses_most_recent_only(self, tmp_path):
        earlier = XML_TEMPLATE.format(
            creation_dt="2017-01-09T18:30:26",
            pricrpts=PRICRPT_TEMPLATE.format(symbol="PETR4", price="30.5"),
        )
        later = XML_TEMPLATE.format(
            creation_dt="2017-01-09T20:16:25",
            pricrpts=PRICRPT_TEMPLATE.format(symbol="PETR4", price="30.7"),
        )
        earlier_path = _write_gzip_xml(tmp_path / "earlier.xml.gz", earlier)
        later_path = _write_gzip_xml(tmp_path / "later.xml.gz", later)

        step = B3ReadBVBG086XmlStep({"step": "b3_read_bvbg086_xml"})
        # earlier listed last on purpose: selection must be by creation
        # timestamp, not by list position.
        df = step.execute(None, _context([later_path, earlier_path]))

        assert len(df) == 1
        assert df.iloc[0]["close"] == "30.7"

    def test_no_valid_message_raises(self, tmp_path):
        garbage = _write_gzip_garbage(tmp_path / "leftover.ZIP.gz")

        step = B3ReadBVBG086XmlStep({"step": "b3_read_bvbg086_xml"})
        with pytest.raises(ValueError, match="No parseable BVBG086"):
            step.execute(None, _context([garbage]))
