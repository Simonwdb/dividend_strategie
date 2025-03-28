import sqlite3
import pandas as pd
from typing import Union


class DatabaseManager:
    def __init__(self, db_path: str = '../data.nosync/database/stock_data.db'):
        self.db_path = db_path

    def load_table(self, table_name: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT * FROM {table_name}
            """)
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=[x[0] for x in cursor.description])

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
    
    def save_failed_tickers(self, failed_tickers: Union[str, set[str]]) -> None:
        self.save_data(failed_tickers, table_name='failed_tickers')

    def save_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        self.save_data(data=df, table_name=table_name)