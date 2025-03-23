import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import yfinance as yf
from datetime import timedelta

def plot_returns_over_time(df: pd.DataFrame) -> px.line:
    fig = px.line(df, x='ex_date', y='total_return', title='Return over time', markers=True)
    return fig

def plot_return_distribution(df: pd.DataFrame) -> px.bar:
    fig = px.bar(
        df.sort_values('ex_date'),
        x='ex_date',
        y='total_return',
        color='total_return',
        color_continuous_scale='RdYlGn',
        title='Return per Event'
    )
    return fig

def plot_cumulative_return(df: pd.DataFrame) -> px.line:
    df = df.sort_values('ex_date').copy()
    df['cumulative_return'] = df['total_return'].cumsum()
    fig = px.line(df, x='ex_date', y='cumulative_return', title='Cumulative Return')
    return fig

def plot_dividend_vs_return(df: pd.DataFrame) -> px.scatter:
    fig = px.scatter(df, x='dvidend', y='total_return', trendline='ols', title='Dividend vs Return')
    return fig

def plot_return_histogram(df: pd.DataFrame) -> px.histogram:
    fig = px.histogram(df, x='total_return', nbins=20, title='Distribution of Returns')
    return fig

def plot_vs_benchmark(df: pd.DataFrame, strategy_ticker: str, benchmark_ticker: str = 'SPY') -> go.Figure:
    df = df.sort_values('ex_date').copy()
    df['cumulative_return'] = df['total_return'].cumsum()

    start = df['buy_date'].min() - timedelta(days=5)
    end = df['sell_date'].max() + timedelta(days=5)

    hist = yf.Ticker(benchmark_ticker).history(start=start, end=end)
    hist['cum_pct_return'] = (hist['Close'] / hist['Close'].iloc[0]) * df['cumulative_return'].iloc[0]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df['ex_date'],
        y=df['cumulative_return'],
        mode='lines+markers',
        name=strategy_ticker
    ))
    fig.add_trace(go.Scatter(
        x=hist.index,
        y=hist['cum_pct_return'],
        mode='lines',
        name=benchmark_ticker
    ))

    fig.update_layout(title=f'Comparison: {strategy_ticker} versus {benchmark_ticker}')
    return fig