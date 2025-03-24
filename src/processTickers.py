import os
import logging
from joblib import Memory

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