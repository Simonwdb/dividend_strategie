import json
import pandas as pd
from pathlib import Path
from typing import Set, List, Tuple


class CheckpointManager:
    def __init__(self, checkpoint_dir: str = '../data/checkpoint'):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.failed_tickers_path = self.checkpoint_dir / 'failed.json'
