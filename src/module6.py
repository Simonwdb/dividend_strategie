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