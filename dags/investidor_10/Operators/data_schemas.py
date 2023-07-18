from pyspark.sql.types import DoubleType, LongType, StringType

data_sources_schemas = {
    "assets_liabilities": [
        {"name": "ticker", "rename": "ticker", "type": StringType()},
        {"name": "data-cost", "rename": "cost", "type": LongType()},
        {"name": "data-ebit", "rename": "ebit", "type": LongType()},
        {"name": "data-ebitda", "rename": "ebitda", "type": DoubleType()},
        {"name": "data-gross_profit", "rename": "gross_profit", "type": DoubleType()},
        {"name": "data-net_profit", "rename": "net_profit", "type": DoubleType()},
        {"name": "data-net_revenue", "rename": "net_revenue", "type": LongType()},
        {"name": "data-net_worth", "rename": "net_worth", "type": LongType()},
        {"name": "data-quarter", "rename": "quarter", "type": StringType()},
        {"name": "data-year", "rename": "year", "type": StringType()},
    ],
    "dividend_yield": [
        {"name": "ticker", "rename": "ticker", "type": StringType()},
        {"name": "data-price", "rename": "yield", "type": DoubleType()},
        {"name": "data-created_at", "rename": "created_at", "type": StringType()},
    ],
    "dividends": [
        {"name": "ticker", "rename": "ticker", "type": StringType()},
        {"name": "data-price", "rename": "value_per_stock", "type": DoubleType()},
        {"name": "data-created_at", "rename": "created_at", "type": StringType()},
    ],
    "historic_kpis": [
        {"name": "ticker", "rename": "ticker", "type": StringType()},
        {"name": "data-price", "rename": "value_per_stock", "type": DoubleType()},
        {"name": "data-created_at", "rename": "created_at", "type": StringType()},
    ],
    "net_income": [],
    "prices_profit": [],
    "ticker_prices": [],
    "tickers_ids": [],
}
