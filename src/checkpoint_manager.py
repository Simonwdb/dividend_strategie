import json
import pandas as pd
from pathlib import Path
from typing import Set, List, Tuple


class CheckpointManager:
    def __init__(self, checkpoint_dir: str = '../data/checkpoint'):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.failed_tickers_path = self.checkpoint_dir / 'failed.json'

    def save_chuk(self, df: pd.DataFrame, chunk_id: int, chunk_size: int) -> None:
        if not isinstance(df, pd.DataFrame) or df.empty:
            raise ValueError('Invalid DataFrame for storage')
        
        path = self.checkpoint_dir / f'chunk_{chunk_id:04d}_{chunk_size}.parquet'
        df.to_parquet(path)
    
    def load_chunks(self) -> pd.DataFrame:
        chunk_files = sorted(self.checkpoint_dir.glob('chunk_*.parquet'))
        if not chunk_files:
            return pd.DataFrame()
        temp_df = pd.concat([pd.read_parquet(f) for f in chunk_files], ignore_index=True)
        return temp_df

    def load_failed_tickers(self) -> Set[str]:
        if not self.failed_tickers_path.exists():
            return set()
        
        with open(self.failed_tickers_path, 'r') as f:
            return set(json.load(f))
        
    def save_failed_tickers(self, failed_tickers: Set[str]) -> None:
        existing = self.load_failed_tickers()
        combined = existing.union(failed_tickers)

        with open(self.failed_tickers_path, 'w') as f:
            json.dump(list(combined), f)