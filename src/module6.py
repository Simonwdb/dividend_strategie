import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def plot_returns_over_time(df: pd.DataFrame) -> px.line:
    fig = px.line(df, x='ex_date', y='total_return', title='Return over time', markers=True)
    return fig