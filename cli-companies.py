# brasa.download_marketdata(
#     "b3-company-details",
#     codeCVM=brasa.get_symbols("company-cvm-code"),
#     verbosity=Verbosity.VERBOSE,
# )

# brasa.download_marketdata(
#     "b3-company-info",
#     issuingCompany=brasa.get_symbols("company"),
#     verbosity=Verbosity.VERBOSE,
# )

# brasa.download_marketdata(
#     "b3-cash-dividends",
#     tradingName=brasa.get_symbols("company-trading-name"),
#     verbosity=Verbosity.VERBOSE,
# )

# brasa.process_marketdata("b3-company-info", reprocess=True)
# brasa.process_etl("b3-companies-info")
# # depends on b3-companies-info
# brasa.process_marketdata("b3-company-details", reprocess=True)
# brasa.process_etl("b3-companies-details")
# # depends on b3-companies-details
# brasa.process_marketdata("b3-cash-dividends", reprocess=True)
# brasa.process_etl("b3-companies-properties")
# brasa.process_etl("b3-equity-symbols-properties")
# brasa.process_etl("b3-companies-cash-dividends")
# brasa.process_etl("b3-companies-stock-dividends")
# brasa.process_etl("b3-companies-subscriptions")
