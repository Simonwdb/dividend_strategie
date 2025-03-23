import streamlit as st
import pandas as pd
from datetime import datetime
from module1 import StrategyParameters
from module2 import (
    get_dividend_events,
    events_to_dataframe
)
from module3 import enrich_dataframe_with_prices
from module4 import calculate_returns
from module5 import analyze_resulst

st.set_page_config(page_title='Dividend Strategy Dashboard', layout='wide')
st.title('Dividend Capture Strategy Dashboard')

# Tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    'Strategy Setup',
    'Results',
    'Analyze Results',
    'Visuals (individual)',
    'Annual Analysis',
    'Visuals (comparison)'
])

with tab1:
    st.header('Strategy Setup')

    st.subheader('Tickers Input')
    upload_option = st.radio('Select input form', ['Manual input', 'Upload Excel list'], horizontal=True)
    tickers = []

    if upload_option == 'Manual input':
        ticker_input = st.text_area('Enter tickers (comma or line separated)', 'AAPL\nMSFT\nPG')
        if ticker_input:
            tickers = [tick.strip() for tick in ticker_input.replace(',', '\n').splitlines() if tick.strip()]
    elif upload_option == 'Upload Excel list':
        uploaded_file = st.file_uploader('Upload Excel file with column "Ticker"', type=['xlsx'])
        if upload_option is not None:
            try:
                uploaded_df = pd.read_excel(uploaded_file)
                tickers = uploaded_df['Ticker'].dropna().astype(str).str.upper().to_list()
                st.success(f'{len(tickers)} tickers loaded.')
            except Exception as e:
                st.error(f'Error processing file: {e}')
    else:
        None
    
    with st.form('strategy_form'):
        col1, col2 = st.columns(2)
        with col1:
            x_days = st.number_input('Days before ex-dividend date', min_value=0, value=3)
            y_days = st.number_input('Days after ex-dividend date', min_value=0, value=3)
        with col2:
            start_date = st.date_input('Start date', value=datetime(2019, 1, 1))
            end_date = st.date_input('End date', value=datetime(2024, 12, 31))
            benchmark_ticker = st.text_input('Select Benchmark (e.q. SPY)', 'SPY')
        
        submit_button = st.form_submit_button('Execute Strategy')
    
    if submit_button and tickers:
        all_results = []

        for ticker in tickers:
            params = StrategyParameters(
                ticker=ticker,
                days_before_threshold=int(x_days),
                days_after_threshold=int(y_days),
                start_date=datetime.combine(start_date, datetime.min.time()),
                end_date=datetime.combine(end_date, datetime.min.time())
            )
        
            events = get_dividend_events(params.ticker, params.start_date, params.end_date)
            df = events_to_dataframe(events=events)
            df = enrich_dataframe_with_prices(df=df, params=params)
            df = calculate_returns(df=df)
            df['ticker'] = ticker
            stats = analyze_resulst(df=df)

            all_results.append((df, stats, params))
        
        if all_results:
            full_df = pd.concat([x[0] for x in all_results], ignore_index=True)
            st.session_state['df'] = full_df
            st.session_state['params'] = params
            st.session_state['benchmark'] = benchmark_ticker
            st.session_state['multi'] = len(tickers) > 1
            st.success('Strategy executed successfully')




with tab2:
    st.header('Dividend Events & Trades')

with tab3:
    st.header('Summarize and Reports')

with tab4:
    st.header('Visuals per Ticker')

with tab5:
    st.header('Annual Analysis')

with tab6:
    st.header('Comparison multiple Tickers')
