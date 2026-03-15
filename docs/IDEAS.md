# IDEAS

## Historical Exchange Rates (BCB Resolution nº 120)

<https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/boletim-diario/historico-de-taxas-de-cambio-resolucao-bcb-n-120/>

Compare with economic indicators.

## Historical CDI rate

template name: b3-historical-cdi-rate

ftp://ftp.cetip.com.br/MediaCDI/

## Implement pipeline processors

- [x] b3-otc-trade-information
- [x] b3-equity-options
- [x] b3-equity-options
- [x] b3-equities-volatility-surface
- [x] b3-economic-indicators-fwf
- [x] b3-trades-intraday

## ETLs to be reviewed

- b3/futures
- b3/curves
- b3/indexes
- b3/lending

## Other sources

- bcb
- anbima
- cvm

## Execution plan

Create an execution plan which considers: downloaders, processors and ELTs.

Processors and ETLs are dead ends of the execution pipeline.

## Documentation

Document the project with MKDocs.

## Kx dataset

Implement Kx datasets.

Implement the article.

## Create an MCP to explore the datalake

There are several nuances that should be addressed in this MCP, such as:

- what each dataset does
- the list of public datasets
- which are the best datasets for specific topics such as: returns, company data, ETFs, REITs, equity options, derivatives options, futures, ...
