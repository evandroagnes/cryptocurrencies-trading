import pandas as pd
from binance_utils import update_historical_data
from technical_indicator_utils import sma
from message_utils import telegram_bot_sendtext

def initialize_ohlc_df():
    df = pd.DataFrame(columns=[
        'OpenTime', 
        'OpenPrice', 
        'HighPrice', 
        'LowPrice', 
        'ClosePrice', 
        'Volume'])

    return df

def get_historical_data(csv_file='BTCUSDT-binance.csv', symbol='BTCUSDT', interval='1m'):
    #csv_file = 'BTCUSDT-binance.csv'
    df = pd.read_csv('data/' + csv_file)
    df['OpenTime'] = pd.to_datetime(df['OpenTime'])
    df.set_index('OpenTime', inplace=True)

    df = update_historical_data(df, symbol, interval)
    df.to_csv('data/' + csv_file)

    return df

def add_row(df, row):
    df.loc[pd.to_datetime(row['OpenTime'])] = [pd.to_numeric(row['OpenPrice']),
        pd.to_numeric(row['HighPrice']),
        pd.to_numeric(row['LowPrice']),
        pd.to_numeric(row['ClosePrice']),
        pd.to_numeric(row['Volume'])]
 
    return df

def resample_data(df, time_resample):
    df_resample = df.copy()
    summaries = {'OpenPrice': 'first', 'HighPrice': 'max', 'LowPrice': 'min', 'ClosePrice': 'last', 'Volume': 'sum'}

    df_resample = df.resample(time_resample).agg(summaries)
    df_resample.dropna(inplace=True)
    df_resample = df_resample[:-1]

    return df_resample

def generate_technical_indicators(df):
    df['SMA50'] = sma(df['ClosePrice'], 50)

    return df

def update_signal_by_strategy(df):
    df = generate_technical_indicators(df)

    signal = df[['ClosePrice']].copy()
    signal[df['ClosePrice'] > df['SMA50']] = 1.0
    signal[df['ClosePrice'] <= df['SMA50']] = -1.0
    #TODO update NaN and 0 to last signal 1 or -1
    #signal[signal.isnull()] = 0.0

    df['signal'] = signal

    return df

def process_candle(df, new_row):
    df = add_row(df, new_row)
    df_trade = update_signal_by_strategy(df.copy())

    if df_trade['signal'][-2] != df_trade['signal'][-1]:
        if df_trade['signal'][-1] == 1:
            message = 'Price cross above SMA 50, BUY'
        else:
            message = 'Price cross below SMA 50, SELL'
        
        telegram_bot_sendtext(message)

    return df
