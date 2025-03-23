import pandas as pd
import numpy as np

def calculate_returns(df: pd.DataFrame) -> pd.DataFrame:
    df['total_return'] = (df['price_sell'] - df['price_buy']) + df['dividend']
    df['total_return'] = np.floor(df['total_return'] * 100) / 100
    return df