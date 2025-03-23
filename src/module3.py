import pandas as pd
import yfinance as yf
from datetime import timedelta
from src.module1 import StrategyParameters

def enrich_dataframe_with_prices(df: pd.DataFrame, params: StrategyParameters) -> pd.DataFrame:
    df["buy_date"] = df["ex_date"] - pd.to_timedelta(params.days_before_ex_div, unit="d")
    df["sell_date"] = df["ex_date"] + pd.to_timedelta(params.days_after_ex_div, unit="d")

    min_date = df["buy_date"].min() - timedelta(days=5)
    max_date = df["sell_date"].max() + timedelta(days=5)

    hist = yf.Ticker(params.ticker).history(start=min_date, end=max_date)

    df["price_buy"] = df["buy_date"].apply(lambda d: get_closest_price(hist, d))
    df["price_sell"] = df["sell_date"].apply(lambda d: get_closest_price(hist, d))

    return df

def get_closest_price(hist: pd.DataFrame, target_date) -> float:
    for offset in range(5):
        date = (target_date + timedelta(days=offset)).strftime('%Y-%m-%d')
        if date in hist.index:
            return hist.loc[date]['Close']
    return None