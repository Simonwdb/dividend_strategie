import os
import time
import logging
import pandas as pd
import yfinance as yf
from tqdm import tqdm
from joblib import Memory
from datetime import datetime
from typing import Optional, List, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cache configuration
CACHE_DIR = '.data/cache/yfinance_cache'
os.makedirs(CACHE_DIR, exist_ok=True)
memory = Memory(CACHE_DIR, verbose=0)

def clear_old_cache(days: int = 7):
    now = time.time()
    for f in os.listdir(CACHE_DIR):
        f_path = os.path.join(CACHE_DIR, f)
        if os.stat(f_path).st_mtime < now - days * 86400:
            os.remove(f_path)

# Logging configuration
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("./tests/stock_data.log"),
            logging.StreamHandler()
        ]
    )
logger = logging.getLogger(__name__)

# Core functions
@memory.cache
def get_single_ticker_date(ticker: str, relevant_keys: List[str], retries: int = 2) -> Optional[List[dict]]:
    for attempt in range(retries + 1):
        try:
            yf_ticker = yf.Ticker(ticker=ticker)
            info = yf_ticker.info

            # Build dict with fallback for missing keys
            data = {
                key: info.get(key, None)
                for key in relevant_keys
            }

            try:
                target_price = yf_ticker.get_analyst_price_targets()['current']
                data['priceTarget'] = target_price
            except Exception as e:
                data['priceTarget'] = None
                logger.debug(f'Price target niet beschikbaar voor {ticker}: {e}')

            data['ticker'] = ticker
            data['lastUpdated'] = datetime.now().isoformat()

            return data
        
        except Exception as e:
            if attempt == retries:
                logger.error(f'Error with {ticker} (attempt {attempt + 1}/{retries}): {str(e)}')
                return None
            continue


def fetch_tickers(tickers: List[str], relevant_keys: List[str], max_workers: int = 5, batch_size: int = 100) -> pd.DataFrame:
    all_results = []
    total_tickers = len(tickers)
    with tqdm(total=total_tickers, desc='Process Tickers') as pbar:
        for i in range(0, total_tickers, batch_size):
            batch = tickers[i:i + batch_size]

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_ticker = {
                    executor.submit(
                        get_single_ticker_date,
                        ticker,
                        relevant_keys
                    ): ticker for ticker in batch
                }

                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        result = future.result()
                        if result:
                            all_results.append(result)
                    except Exception as e:
                        logger.error(f'Unexpected error with {ticker}: {str(e)}')
                    finally:
                        pbar.update(1)

    return pd.DataFrame(all_results)


# Data cleaning functions
def clean_and_format_data(df: pd.DataFrame, column_order: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    
    temp_df = df.copy()

    date_cols = [col for col in temp_df.columns if any(x in col.lower() for x in ['date', 'time'])]
    for col in date_cols:
        try:
            temp_df[col] = pd.to_datetime(temp_df[col], unit='s', errors='coerce')
        except Exception as e:
            logger.warning(f'Unable to convert {col}: {str(e)}')

    
    remaining_columns = [col for col in temp_df.columns if col not in column_order]
    temp_df = temp_df[column_order + remaining_columns]

    return temp_df


def get_stock_data(
        tickers: Union[List[str], str], 
        relevant_keys: List[str],
        column_order: List[str], 
        max_workers: int = 5, 
        batch_size: int = 100, 
        clear_cache: bool = False
) -> pd.DataFrame:
    if clear_cache:
        memory.clear()
        logger.info('Cache deleted')

    if isinstance(tickers, str):
        tickers = [tickers]
    
    if not tickers:
        logger.warning('No tickers are given')
        return pd.DataFrame()
    
    logger.info(f'Start with extracting data for {len(tickers)} tickers')

    try:
        df = fetch_tickers(
            tickers=tickers,
            relevant_keys=relevant_keys, 
            max_workers=max_workers,
            batch_size=batch_size
        )

        df = clean_and_format_data(df=df, column_order=column_order)

        logger.info(f'Successfully added {len(df)} records')
        return df

    except Exception as e:
        logger.error(f'Error in get_stock_data: {str(e)}')
        return pd.DataFrame()