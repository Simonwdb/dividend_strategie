import pandas as pd

def analyze_resulst(df: pd.DataFrame) -> dict:
    valid_df = df[df['total_return'].notna()]
    if valid_df.empty:
        return {'message': 'No valid trades to analyze.'}
    
    return {
        'total_trades': len(valid_df),
        'average_return': round(valid_df['total_return'].mean(), 3),
        'total_return': round(valid_df['total_return'].sum(), 3),
        'positive_trades': (valid_df['total_return'] > 0).sum(),
        'negative_trades': (valid_df['total_return'] <= 0).sum(),
        'profit_percentage': round((valid_df['total_return'] > 0).mean() * 100, 2)
    }
