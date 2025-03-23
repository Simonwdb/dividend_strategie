import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

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
