import sqlite3
import yfinance as yf
import pandas as pd
from datetime import datetime
from typing import List

# global variables
RELEVENT_KEYS = ['city', 'state', 'zip', 'country', 'industry', 'sector', 'fullTimeEmployees', 'auditRisk', 'boardRisk', 'compensationRisk', 'shareHolderRightsRisk', 
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


FAV_COLS = [
        'ticker', 'longName', 'open', 'dayLow', 'dayHigh', 'previousClose', 'priceTarget', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'fiftyDayAverageChange', 
        'lastDividendValue', 'lastDividendDate', 'dividendYield', 'exDividendDate', 'dividendDate', 'fiveYearAvgDividendYield', 'trailingAnnualDividendRate', 
        'trailingAnnualDividendYield', 'earningsQuarterlyGrowth', 'revenueGrowth'
    ]

DB_PATH = '../../data/database/stock_data.db'


#  initial functions
def get_single_data(ticker: str) -> dict:
    ticker_yf = yf.Ticker(ticker=ticker)
    info = ticker_yf.info

    info = {key: info.get(key, None) for key in RELEVENT_KEYS}

    try:
        price_target = ticker_yf.get_analyst_price_targets()['current']
    except KeyError as e:
        price_target = None
    
    info['priceTarget'] = price_target
    info['ticker'] = ticker
    info['lastUpdated'] = datetime.now().isoformat()

    return info


def get_stock_data(ticker_list: List[str]) -> pd.DataFrame:
    results = []
    for ticker in ticker_list:
        try:
            temp_dict = get_single_data(ticker=ticker_list)
            results.append(temp_dict)
        except AttributeError as e:
            continue
    
    result_df = pd.DataFrame(results)
    return result_df


def convert_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    temp_df = df.copy()
    date_cols = [col for col in temp_df.columns if any(word in col.lower() for word in ['date', 'timestamp']) and temp_df[col].dtype == 'int64']
    for col in date_cols:
        temp_df[col] = pd.to_datetime(temp_df[col], unit='s', errors='coerce')
    
    return temp_df

def rearrange_columns(df: pd.DataFrame) -> pd.DataFrame:
    temp_df = df.copy()
    existing_cols = temp_df.columns
    remaining_cols = [col for col in existing_cols if col not in FAV_COLS]
    column_order = FAV_COLS + remaining_cols
    temp_df = temp_df[column_order]
    return temp_df


def save_to_sqlite(df: pd.DataFrame, table_name: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False)