import yfinance as yf

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

@dataclass
class DividendEvent:
    ex_date: datetime
    dividend: float
    buy_date: Optional[datetime] = None
    sell_date: Optional[datetime] = None
    price_buy: Optional[float] = None
    price_sell: Optional[float] = None
    total_return: Optional[float] = None

def get_dividend_events(ticker: str, start: datetime, end: datetime) -> List[DividendEvent]:
    stock = yf.Ticker(ticker)
    dividends = stock.dividends
    dividends.index = dividends.index.tz_localize(None)
    filtered = dividends[(dividends.index >= start) & (dividends.index <= end)]

    return [
        DividendEvent(ex_date=date.to_pydatetime(), dividend=amount) for date, amount in filtered.items()
    ]