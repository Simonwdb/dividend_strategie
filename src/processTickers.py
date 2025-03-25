import logging


class StockDataProcessor:
    RELEVANT_KEYS = ['city', 'state', 'zip', 'country', 'industry', 'sector', 'fullTimeEmployees', 'auditRisk', 'boardRisk', 'compensationRisk', 'shareHolderRightsRisk', 
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

    def __init__(self, db_path: str = '../data/database/stock_data.db'):
        self.db_path = db_path
    
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