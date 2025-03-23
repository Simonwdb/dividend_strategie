from dataclasses import dataclass
from datetime import datetime

@dataclass
class StrategyParameters:
    ticker: str
    days_before_threshold: int
    days_after_threshold: int
    start_date: datetime
    end_date: datetime

def get_default_params() -> StrategyParameters:
    return StrategyParameters(
        ticker='AAPL',
        days_before_threshold=3,
        days_after_threshold=3,
        start_date=datetime(2019, 1, 1),
        end_date=datetime(2023, 12, 31)
    )