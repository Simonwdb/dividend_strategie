import logging
import sqlite3
import logging
import pandas as pd
import yfinance as yf
from typing import List
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


class StockDataProcessor:
    RELEVANT_KEYS_STOCK_DATA = ['city', 'state', 'zip', 'country', 'industry', 'sector', 'fullTimeEmployees', 'auditRisk', 'boardRisk', 'compensationRisk', 'shareHolderRightsRisk', 
                 'overallRisk', 'compensationAsOfEpochDate', 'priceHint', 'previousClose', 'open', 'dayLow', 'dayHigh', 'regularMarketPreviousClose', 'regularMarketOpen', 
                 'regularMarketDayLow', 'regularMarketDayHigh', 'dividendRate', 'dividendYield', 'exDividendDate', 'payoutRatio', 'fiveYearAvgDividendYield', 'beta', 'trailingPE', 
                 'forwardPE', 'volume', 'regularMarketVolume', 'averageVolume', 'averageVolume10days', 'averageDailyVolume10Day', 'bid', 'ask', 'bidSize', 'askSize', 'marketCap', 
                 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'priceToSalesTrailing12Months', 'fiftyDayAverage', 'twoHundredDayAverage', 'trailingAnnualDividendRate', 'trailingAnnualDividendYield', 
                 'currency', 'enterpriseValue', 'profitMargins', 'floatShares', 'sharesOutstanding', 'sharesShort', 'sharesShortPriorMonth', 'sharesShortPreviousMonthDate', 'dateShortInterest', 
                 'sharesPercentSharesOut', 'heldPercentInsiders', 'heldPercentInstitutions', 'shortRatio', 'impliedSharesOutstanding', 'bookValue', 'priceToBook', 'lastFiscalYearEnd', 'nextFiscalYearEnd', 
                 'mostRecentQuarter', 'earningsQuarterlyGrowth', 'netIncomeToCommon', 'trailingEps', 'forwardEps', 'lastSplitFactor', 'lastSplitDate', 'enterpriseToRevenue', 'enterpriseToEbitda', 
                 '52WeekChange', 'SandP52WeekChange', 'lastDividendValue', 'lastDividendDate', 'quoteType', 'currentPrice', 'totalCash', 'totalCashPerShare', 'ebitda', 'totalDebt', 'quickRatio', 
                 'currentRatio', 'totalRevenue', 'debtToEquity', 'revenuePerShare', 'returnOnAssets', 'returnOnEquity', 'grossProfits', 'freeCashflow', 'operatingCashflow', 'earningsGrowth', 'revenueGrowth', 
                 'grossMargins', 'ebitdaMargins', 'operatingMargins', 'financialCurrency', 'symbol', 'region', 'typeDisp', 'exchange', 'exchangeTimezoneName', 'exchangeTimezoneShortName', 'market', 
                 'regularMarketTime', 'longName', 'marketState', 'regularMarketChangePercent', 'regularMarketPrice', 'regularMarketChange', 'regularMarketDayRange', 'fullExchangeName', 
                 'averageDailyVolume3Month', 'fiftyTwoWeekLowChange', 'fiftyTwoWeekLowChangePercent', 'fiftyTwoWeekHighChange', 'fiftyTwoWeekHighChangePercent', 'fiftyTwoWeekChangePercent', 'dividendDate', 
                 'earningsTimestamp', 'earningsTimestampStart', 'earningsTimestampEnd', 'earningsCallTimestampStart', 'earningsCallTimestampEnd', 'epsTrailingTwelveMonths', 'epsForward', 'epsCurrentYear', 
                 'priceEpsCurrentYear', 'fiftyDayAverageChange', 'fiftyDayAverageChangePercent', 'twoHundredDayAverageChange', 'twoHundredDayAverageChangePercent', 'sourceInterval', 'cryptoTradeable']


    FAV_COLS_STOCK_DATA = [
        'ticker', 'longName', 'open', 'dayLow', 'dayHigh', 'previousClose', 'priceTarget', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'fiftyDayAverageChange', 
        'lastDividendValue', 'lastDividendDate', 'dividendYield', 'exDividendDate', 'dividendDate', 'fiveYearAvgDividendYield', 'trailingAnnualDividendRate', 
        'trailingAnnualDividendYield', 'earningsQuarterlyGrowth', 'revenueGrowth'
    ]

    def __init__(self):
        self._setup_logging()
    
    def _setup_logging(self):
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler("../tests/stock_data.log"),
                    logging.StreamHandler()
                ]
            )
        self.logger = logging.getLogger(__name__)

    def get_single_ticker_data(self, ticker: str) -> dict:
        ticker_yf = yf.Ticker(ticker=ticker)
        try:
            info = ticker_yf.info
            info = {key: info.get(key, None) for key in self.RELEVANT_KEYS_STOCK_DATA}
        except AttributeError as e:
            self.logger.debug(f'Not being able to retrieve info from {ticker}: {str(e)}')
            return dict()

        price_target = None        
        # try:
        #     price_target = ticker_yf.get_analyst_price_targets()['current']
        # except KeyError as e:
        #     price_target = None
        #     self.logger.debug(f'Price target not available for {ticker}: {str(e)}')
        
        info['priceTarget'] = price_target
        info['ticker'] = ticker
        info['lastUpdated'] = datetime.now().isoformat()

        return info
    
    def fetch_tickers_data(self, ticker_list: List[str], max_workers: int = 5, batch_size: int = 100) -> pd.DataFrame:
        all_results = []
        
        for i in range(0, len(ticker_list), batch_size):
            batch = ticker_list[i:i + batch_size]
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_ticker = {
                    executor.submit(self.get_single_ticker_data, ticker): ticker 
                    for ticker in batch
                }

                for future in as_completed(future_to_ticker):
                    ticker = future_to_ticker[future]
                    try:
                        result = future.result()
                        if result:
                            all_results.append(result)
                    except Exception as e:
                        self.logger.error(f'Unexpected error with {ticker}: {str(e)}')
        
        return pd.DataFrame(all_results)
    
    def get_stock_data(self, ticker_list: List[str]) -> pd.DataFrame:
        results = []
        for ticker in ticker_list:
            try:
                temp_dict = self.get_single_ticker_data(ticker)
                results.append(temp_dict)
            except AttributeError as e:
                self.logger.debug(f'No data available on yfinance for {ticker}: {str(e)}')
                continue
        
        result_df = pd.DataFrame(results)
        self.logger.info(f'Successfully added {len(result_df)} records')
        return result_df
    
    @staticmethod
    def convert_timestamps(df: pd.DataFrame) -> pd.DataFrame:
        temp_df = df.copy()
        date_cols = [col for col in temp_df.columns 
                    if any(word in col.lower() for word in ['date', 'timestamp'])]
        
        for col in date_cols:
            if temp_df[col].dtype != object:
                for unit in ['s', 'ms', 'us', 'ns']:
                    try:
                        temp_df[col] = pd.to_datetime(temp_df[col], unit=unit, errors='coerce')
                        if not temp_df[col].isnull().all():
                            break
                    except (FloatingPointError, OverflowError):
                        continue
        
        return temp_df
    
    def rearrange_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        temp_df = df.copy()
        existing_cols = temp_df.columns
        remaining_cols = [col for col in existing_cols if col not in self.FAV_COLS_STOCK_DATA]
        column_order = self.FAV_COLS_STOCK_DATA + remaining_cols
        return temp_df[column_order]
