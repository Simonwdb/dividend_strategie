import streamlit as st


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
