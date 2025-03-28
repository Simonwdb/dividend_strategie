import sqlite3
import pandas as pd


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
