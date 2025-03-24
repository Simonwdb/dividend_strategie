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