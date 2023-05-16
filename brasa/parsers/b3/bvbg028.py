
from datetime import datetime
import io
from lxml import etree
import pandas as pd
from brasa.cache import CacheManager

from brasa.templates import retrieve_template
from ..util import Parser


class BVBG028Parser(Parser):
    ATTRS = {
        "header": {
            "trade_date": "RptParams/RptDtAndTm/Dt",
            "security_id": "FinInstrmId/OthrId/Id",
            "security_proprietary": "FinInstrmId/OthrId/Tp/Prtry",
            "security_market": "FinInstrmId/OthrId/PlcOfListg/MktIdrCd",
            "instrument_asset": "FinInstrmAttrCmon/Asst",
            "instrument_asset_description": "FinInstrmAttrCmon/AsstDesc",
            "instrument_market": "FinInstrmAttrCmon/Mkt",
            "instrument_segment": "FinInstrmAttrCmon/Sgmt",
            "instrument_description": "FinInstrmAttrCmon/Desc",
        },
        "EqtyInf": {
            "security_category": "InstrmInf/EqtyInf/SctyCtgy",
            "isin": "InstrmInf/EqtyInf/ISIN",
            "distribution_id": "InstrmInf/EqtyInf/DstrbtnId",
            "cfi_code": "InstrmInf/EqtyInf/CFICd",
            "specification_code": "InstrmInf/EqtyInf/SpcfctnCd",
            "corporation_name": "InstrmInf/EqtyInf/CrpnNm",
            "symbol": "InstrmInf/EqtyInf/TckrSymb",
            "payment_type": "InstrmInf/EqtyInf/PmtTp",
            "allocation_lot_size": "InstrmInf/EqtyInf/AllcnRndLot",
            "price_factor": "InstrmInf/EqtyInf/PricFctr",
            "trading_start_date": "InstrmInf/EqtyInf/TradgStartDt",
            "trading_end_date": "InstrmInf/EqtyInf/TradgEndDt",
            "corporate_action_start_date": "InstrmInf/EqtyInf/CorpActnStartDt",
            "ex_distribution_number": "InstrmInf/EqtyInf/EXDstrbtnNb",
            "custody_treatment_type": "InstrmInf/EqtyInf/CtdyTrtmntTp",
            "trading_currency": "InstrmInf/EqtyInf/TradgCcy",
            "market_capitalisation": "InstrmInf/EqtyInf/MktCptlstn",
            "last_price": "InstrmInf/EqtyInf/LastPric",
            "first_price": "InstrmInf/EqtyInf/FrstPric",
            "governance_indicator": "InstrmInf/EqtyInf/GovnInd",
            "days_to_settlement": "InstrmInf/EqtyInf/DaysToSttlm",
            "right_issue_price": "InstrmInf/EqtyInf/RghtsIssePric",
        },
        "OptnOnEqtsInf": {
            "security_category": "InstrmInf/OptnOnEqtsInf/SctyCtgy",
            "isin": "InstrmInf/OptnOnEqtsInf/ISIN",
            "symbol": "InstrmInf/OptnOnEqtsInf/TckrSymb",
            "exercise_price": "InstrmInf/OptnOnEqtsInf/ExrcPric",
            "option_style": "InstrmInf/OptnOnEqtsInf/OptnStyle",
            "expiration_date": "InstrmInf/OptnOnEqtsInf/XprtnDt",
            "option_type": "InstrmInf/OptnOnEqtsInf/OptnTp",
            "underlying_security_id": "InstrmInf/OptnOnEqtsInf/UndrlygInstrmId/OthrId/Id",
            "underlying_security_proprietary": "InstrmInf/OptnOnEqtsInf/UndrlygInstrmId/OthrId/Tp/Prtry",
            "underlying_security_market": "InstrmInf/OptnOnEqtsInf/UndrlygInstrmId/OthrId/PlcOfListg/MktIdrCd",
            "protection_flag": "InstrmInf/OptnOnEqtsInf/PrtcnFlg",
            "premium_upfront_indicator": "InstrmInf/OptnOnEqtsInf/PrmUpfrntInd",
            "trading_start_date": "InstrmInf/OptnOnEqtsInf/TradgStartDt",
            "trading_end_date": "InstrmInf/OptnOnEqtsInf/TradgEndDt",
            "payment_type": "InstrmInf/OptnOnEqtsInf/PmtTp",
            "allocation_lot_size": "InstrmInf/OptnOnEqtsInf/AllcnRndLot",
            "price_factor": "InstrmInf/OptnOnEqtsInf/PricFctr",
            "trading_currency": "InstrmInf/OptnOnEqtsInf/TradgCcy",
            "days_to_settlement": "InstrmInf/OptnOnEqtsInf/DaysToSttlm",
            "delivery_type": "InstrmInf/OptnOnEqtsInf/DlvryTp",
            "automatic_exercise_indicator": "InstrmInf/OptnOnEqtsInf/AutomtcExrcInd",
        },
        "FutrCtrctsInf": {
            "security_category": "InstrmInf/FutrCtrctsInf/SctyCtgy",
            "expiration_date": "InstrmInf/FutrCtrctsInf/XprtnDt",
            "symbol": "InstrmInf/FutrCtrctsInf/TckrSymb",
            "expiration_code": "InstrmInf/FutrCtrctsInf/XprtnCd",
            "trading_start_date": "InstrmInf/FutrCtrctsInf/TradgStartDt",
            "trading_end_date": "InstrmInf/FutrCtrctsInf/TradgEndDt",
            "value_type_code": "InstrmInf/FutrCtrctsInf/ValTpCd",
            "isin": "InstrmInf/FutrCtrctsInf/ISIN",
            "cfi_code": "InstrmInf/FutrCtrctsInf/CFICd",
            "delivery_type": "InstrmInf/FutrCtrctsInf/DlvryTp",
            "payment_type": "InstrmInf/FutrCtrctsInf/PmtTp",
            "contract_multiplier": "InstrmInf/FutrCtrctsInf/CtrctMltplr",
            "asset_settlement_indicator": "InstrmInf/FutrCtrctsInf/AsstQtnQty",
            "allocation_lot_size": "InstrmInf/FutrCtrctsInf/AllcnRndLot",
            "trading_currency": "InstrmInf/FutrCtrctsInf/TradgCcy",
            "underlying_security_id": "InstrmInf/FutrCtrctsInf/UndrlygInstrmId/OthrId/Id",
            "underlying_security_proprietary": "InstrmInf/FutrCtrctsInf/UndrlygInstrmId/OthrId/Tp/Prtry",
            "underlying_security_market": "InstrmInf/FutrCtrctsInf/UndrlygInstrmId/OthrId/PlcOfListg/MktIdrCd",
            "withdrawal_days": "InstrmInf/FutrCtrctsInf/WdrwlDays",
            "working_days": "InstrmInf/FutrCtrctsInf/WrkgDays",
            "calendar_days": "InstrmInf/FutrCtrctsInf/ClnrDays",
        },
    }

    mode = "rb"

    def __init__(self, fname):
        self.fname = fname
        self.instruments = []
        self.missing = set()
        self.parse()

    def parse(self):
        tree = self._open(self.fname, etree.parse)
        exchange = tree.getroot()[0][0]
        ns = {None: "urn:bvmf.052.01.xsd"}
        td_xpath = etree.ETXPath("//{urn:bvmf.052.01.xsd}BizGrpDtls")
        td = td_xpath(exchange)
        if len(td) > 0:
            self.creation_date = td[0].find("CreDtAndTm", ns).text[:10]
        else:
            raise Exception("Invalid XML: tag BizGrpDtls not found")

        xs = exchange.findall(
            "{urn:bvmf.052.01.xsd}BizGrp/{urn:bvmf.100.02.xsd}Document/{urn:bvmf.100.02.xsd}Instrm"
        )
        for node in xs:
            self.parse_instrument_node(node)

    def parse_instrument_node(self, node):
        data = {"creation_date": self.creation_date}
        ns = {None: "urn:bvmf.100.02.xsd"}
        for attr in self.ATTRS["header"]:
            els = node.findall(self.ATTRS["header"][attr], ns)
            if len(els):
                data[attr] = els[0].text.strip()
        elm = node.findall("InstrmInf", ns)[0]
        # remove ns {urn:bvmf.100.02.xsd} = 21 chars
        tag = elm.getchildren()[0].tag[21:]
        if self.ATTRS.get(tag) is None:
            self.missing.add(tag)
            return
        for attr in self.ATTRS[tag]:
            els = node.findall(self.ATTRS[tag][attr], ns)
            if len(els):
                data[attr] = els[0].text.strip()
        data["instrument_type"] = tag
        self.instruments.append(data)

    @property
    def data(self):
        return self.instruments


def bvbg028_get(refdate: datetime, instrument_type: str = None) -> pd.DataFrame:
    tpl = retrieve_template("b3-bvbg028")
    args = dict(refdate=refdate, instrument_type=instrument_type)
    cache = CacheManager(tpl, args)
    return cache.process_with_checks(refdate)