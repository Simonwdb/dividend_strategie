
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