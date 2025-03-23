from dataclasses import dataclass
from datetime import datetime

@dataclass
class StrategyParameters:
    ticker: str
    days_before_threshold: int
    days_after_threshold: int
    start_date: datetime
    end_date: datetime

