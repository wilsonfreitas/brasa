import pandas as pd
from ..util import Parser
from lxml import etree


class BVBG086Parser(Parser):
    mode = "rb"

    def __init__(self, fname):
        self.fname = fname
        self.instruments = []
        self.missing = set()
        self.parse()
        self.__instrs_table = pd.DataFrame(self.instruments)

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
            "{urn:bvmf.052.01.xsd}BizGrp/{urn:bvmf.217.01.xsd}Document/{urn:bvmf.217.01.xsd}PricRpt"
        )
        for node in xs:
            self.parse_price_report_node(node)

    def parse_price_report_node(self, node):
        attrs = {
            "trade_date": "TradDt/Dt",
            "symbol": "SctyId/TckrSymb",
            "security_id": "FinInstrmId/OthrId/Id",  # SecurityId
            "security_proprietary": "FinInstrmId/OthrId/Tp/Prtry",
            "security_market": "FinInstrmId/PlcOfListg/MktIdrCd",
            "trade_quantity": "TradDtls/TradQty",  # Negócios
            "volume": "FinInstrmAttrbts/NtlFinVol",
            "open_interest": "FinInstrmAttrbts/OpnIntrst",
            "traded_contracts": "FinInstrmAttrbts/FinInstrmQty",
            "best_ask_price": "FinInstrmAttrbts/BestAskPric",
            "best_bid_price": "FinInstrmAttrbts/BestBidPric",
            "first_price": "FinInstrmAttrbts/FrstPric",
            "min_price": "FinInstrmAttrbts/MinPric",
            "max_price": "FinInstrmAttrbts/MaxPric",
            "average_price": "FinInstrmAttrbts/TradAvrgPric",
            "last_price": "FinInstrmAttrbts/LastPric",
            # Negócios na sessão regular
            "regular_transactions_quantity": "FinInstrmAttrbts/RglrTxsQty",
            # Contratos na sessão regular
            "regular_traded_contracts": "FinInstrmAttrbts/RglrTraddCtrcts",
            # Volume financeiro na sessão regular
            "regular_volume": "FinInstrmAttrbts/NtlRglrVol",
            # Negócios na sessão não regular
            "nonregular_transactions_quantity": "FinInstrmAttrbts/NonRglrTxsQty",
            # Contratos na sessão não regular
            "nonregular_traded_contracts": "FinInstrmAttrbts/NonRglrTraddCtrcts",
            # Volume financeiro na sessão nãoregular
            "nonregular_volume": "FinInstrmAttrbts/NtlNonRglrVol",
            "oscillation_percentage": "FinInstrmAttrbts/OscnPctg",
            "adjusted_quote": "FinInstrmAttrbts/AdjstdQt",
            "adjusted_tax": "FinInstrmAttrbts/AdjstdQtTax",
            "previous_adjusted_quote": "FinInstrmAttrbts/PrvsAdjstdQt",
            "previous_adjusted_tax": "FinInstrmAttrbts/PrvsAdjstdQtTax",
            "variation_points": "FinInstrmAttrbts/VartnPts",
            "adjusted_value_contract": "FinInstrmAttrbts/AdjstdValCtrct",
        }
        ns = {None: "urn:bvmf.217.01.xsd"}
        data = {"creation_date": self.creation_date}
        for attr in attrs:
            els = node.findall(attrs[attr], ns)
            if len(els):
                data[attr] = els[0].text
        self.instruments.append(data)

    @property
    def data(self):
        return self.__instrs_table
