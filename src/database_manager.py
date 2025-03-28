import sqlite3
import pandas as pd
from pathlib import Path
from typing import Union, Set


class DatabaseManager:
    def __init__(self, 
                 db_path: Union[str, Path] = '../data.nosync/database/stock_data.db',
                 parquet_dir: Union[str, Path] = '../data.nosync/database/parquet'):
        self.db_path = Path(db_path)
        self.parquet_dir = Path(parquet_dir)

        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def load_table(self, table_name: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT * FROM {table_name}
            """)
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=[x[0] for x in cursor.description])

    def load_parquet(self, ticker: str) -> pd.DataFrame:
        path = self.parquet_dir / f'{ticker}.parquet'
        parq_df = pd.read_parquet(path)
        return parq_df
    
    def remove_table(self, table_name: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
            conn.commit()
    
    def save_data(self, data: Union[str, set[str], pd.DataFrame], table_name: str, if_exists: str = 'append') -> None:
        with sqlite3.connect(self.db_path) as conn:
            if isinstance(data, (str, set)):
                if table_name != 'failed_tickers':
                    raise ValueError("Tickers can only be stored in the 'failed_tickers' table")
                
                tickers_data = {data} if isinstance(data, str) else data
                conn.executemany(
                    "INSERT OR IGNORE INTO failed_tickers (ticker) VALUES (?)",
                    [(ticker,) for ticker in tickers_data]
                )

            elif isinstance(data, pd.DataFrame):
                data.to_sql(table_name, conn, if_exists=if_exists, index=False)
            
            else:
                raise TypeError('Invalid datatype, expected str, set[str] or pd.DataFrame')
    
    def save_parquet(self, df: pd.DataFrame, ticker: str, compression: str = 'zstd') -> None:
        path = self.parquet_dir / f'{ticker}.parquet'
        df.to_parquet(
            path=path,
            engine='pyarrow',
            compression=compression
        )
    
    def save_failed_tickers(self, failed_tickers: Union[str, set[str]]) -> None:
        self.save_data(failed_tickers, table_name='failed_tickers')

    def save_dataframe(self, 
                       df: pd.DataFrame, 
                       ticker: str, 
                       addition_name: str = '', 
                       storage_type: str = 'sqlite') -> None:
        table_name = ticker + addition_name
        if storage_type.lower() == 'sqlite':
            self.save_data(data=df, table_name=table_name)
        elif storage_type.lower() == 'parquet':
            self.save_parquet(df, ticker)
        else:
            raise ValueError('storage_type must be "parquet" or "sqlite"')