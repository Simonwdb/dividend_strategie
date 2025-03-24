import os
import logging
import yfinance as yf
from joblib import Memory
from datetime import datetime
from typing import Optional, List

# Cache configuration
CACHE_DIR = '.data/cache/yfinance_cache'
os.makedirs(CACHE_DIR, exist_ok=True)
memory = Memory(CACHE_DIR, verbose=0)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stock_data.log"),
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