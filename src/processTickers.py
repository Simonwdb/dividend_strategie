import os
from joblib import Memory

# Cache configuration
CACHE_DIR = '.data/cache/yfinance_cache'
os.makedirs(CACHE_DIR, exist_ok=True)
memory = Memory(CACHE_DIR, verbose=0)

    