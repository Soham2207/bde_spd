import yfinance as yf
import pandas as pd


def run_finance_etl():
    oneHourdf = yf.download("MANU", period='7d',interval="1h")

    df = pd.DataFrame(oneHourdf)
    df['Datetime'] = df['Datetime'].dt.date
    df.to_csv('s3://deltabase/bde_temp_storage/refined_stocks.csv')