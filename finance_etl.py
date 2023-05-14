import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt

def run_finance_etl():
    tickers_df_1month = yf.download("RELIANCE.NS", start = "2000-01-01", end = "2017-12-31")
    tickers_df_10years = yf.download("RELIANCE.NS", start = "2000-01-01", end = "2010-12-31")
    fiveDaydf = yf.download("RELIANCE.NS", period='5d')
    oneHourdf = yf.download("RELIANCE.NS", period='7d',interval="1h")

    df = pd.DataFrame(tickers_df_1month)
    df.to_csv('refined_stocks.csv')

