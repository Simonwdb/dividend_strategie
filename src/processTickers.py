import logging
import sqlite3
import logging
import numpy as np
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

    def __init__(self, start_date: datetime = datetime(2010, 1, 1), end_date: datetime = datetime(2024, 12, 30)):
        self.start_date = start_date
        self.end_date = end_date
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
    
    def get_historical_data(self, ticker: str) -> pd.DataFrame:
        base_cols = ['Open', 'Close', 'Dividends']
        
        try:
            ticker_yf = yf.Ticker(ticker)
            hist_df = ticker_yf.history(start=self.start_date, end=self.end_date)

            for col in base_cols:
                if col not in hist_df.columns:
                    hist_df[col] = np.nan
            
            hist_df.index = hist_df.index.strftime('%Y-%m-%d')
            hist_df = hist_df.reset_index()
            hist_df['Ticker'] = ticker

            hist_df.columns = hist_df.columns.str.capitalize()
            hist_df.rename(columns={'Date': 'Date'}, inplace=True)

            return hist_df[['Date', 'Open', 'Close', 'Dividends', 'Ticker']]

        except AttributeError as e:
            return pd.DataFrame(columns=['Date', 'Open', 'Close', 'Dividends', 'Ticker'])

        # ticker_yf = yf.Ticker(ticker)
        # try:
        #     hist_df = ticker_yf.history(start=self.start_date, end=self.end_date)
        #     hist_df.index = hist_df.index.strftime('%Y-%m-%d')
        # except AttributeError as e:
        #     return pd.DataFrame(columns=['Date', 'Open', 'Close', 'Dividends', 'ticker'])
        # hist_df.index = pd.to_datetime(hist_df.index, errors='coerce')
        # hist_df[['Close', 'Open']] = round(hist_df[['Close', 'Open']], 2)
        # hist_df = hist_df.reset_index()
        # hist_df['ticker'] = ticker

        # return hist_df[['Date', 'Open', 'Close', 'Dividends', 'ticker']]
    
    def fetch_historical_data(self, ticker_list: List[str], max_workers: int = 5, batch_size: int = 100) -> pd.DataFrame:
        all_results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self.get_historical_data,
                    t
                ): t for t in ticker_list
            }

            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    all_results.append(result)
        
        non_empty_results = [df for df in all_results if not df.empty]
        
        if non_empty_results:
            return pd.concat(all_results, ignore_index=True)
        else:      
            return pd.concat()

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
