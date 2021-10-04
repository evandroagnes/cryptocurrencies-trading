import numpy as np
import pandas as pd

def get_cross_signal(short_data, long_data):
    # the column names must be the same
    short_data.columns = ['value']
    long_data.columns = ['value']

    signal = long_data.copy()
    signal[short_data > long_data] = 1.0
    signal[short_data <= long_data] = -1.0
    signal[long_data.isnull()] = 0.0

    return signal

def get_macd_signal(signal_macd, macd_value):
    signal_macd.columns = ['value']
    macd_value.columns = ['value']
    
    signal = macd_value.copy()

    signal[signal_macd < macd_value] = 1.0
    signal[signal_macd >= macd_value] = -1.0

    return signal 

def get_rsi_signal(signal, overbought_value=70.0, oversold_value=30.0):
    signal[signal < oversold_value] = 1.0
    signal[signal > overbought_value] = -1.0

    signal[(signal != 1.0) & (signal != -1.0)] = np.nan
    signal.fillna(method="ffill", inplace=True)
    signal[signal.isnull()] = 0.0
    #signal[(signal <= overbought_value) & (signal >= oversold_value)] = 0.0
    #signal[signal.isnull()] = 0.0

    return signal

def get_rsi_adx_signal(signal, adx, overbought_value=70.0, oversold_value=30.0, adx_value=25.0):
    signal.columns = ['value']
    adx.columns = ['value']

    signal[(signal < oversold_value) & (adx > adx_value)] = 1.0
    signal[(signal > overbought_value) & (adx > adx_value)] = -1.0

    #signal[(signal != 1.0) & (signal != -1.0)] = np.nan
    #signal.fillna(method="ffill", inplace=True)
    signal[(signal <= overbought_value) & (signal >= oversold_value)] = 0.0
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0
    signal[signal.isnull()] = 0.0

    return signal

def get_5_minute_signal(price_data, macd_history, ema):
    """
    https://www.investopedia.com/articles/forex/08/five-minute-momo.asp
    """
    price_data.columns = ['value']
    macd_history.columns = ['value']
    ema.columns = ['value']

    signal = price_data.copy()
    long = False

    for i in range(price_data.size):
        if price_data.iloc[i]['value'] > ema.iloc[i]['value'] and macd_history.iloc[i]['value'] > 0:
            signal.iloc[i] = 1.0
            long = True
        
        if price_data.iloc[i]['value'] < ema.iloc[i]['value'] and long:
            signal.iloc[i] = -1.0

    signal[(signal != 1.0) & (signal != -1.0)] = 0.0

    return signal

def get_sma_macd_signal(price_data, short_data, long_data, macd_data):
    """ 
    - Wait for price data to be above SMA 50 (short) and SMA 100 (long);
    - If MACD is positive at least for the last 5 bars only (not more because in this case, the signal can be weak): LONG;
    - Otherwise wait for the next positive MACD signal.
    - TODO Stop on the minor price of the last 5 bars;
    - TODO Exit of half investment on 2x the difference from entry to stop
    - Exit on price_data below SMA 50 (short)
    """
    price_data.columns = ['value']
    short_data.columns = ['value']
    long_data.columns = ['value']
    macd_data.columns = ['value']

    signal = price_data.copy()
    long = False
    for i in range(price_data.size):
        if price_data.iloc[i]['value'] > short_data.iloc[i]['value'] and price_data.iloc[i]['value'] > long_data.iloc[i]['value']:
            num_positive_candles = 0
            pos = i
            while (pos > 0 and macd_data.iloc[pos]['value'] > 0):
                pos-=1
                num_positive_candles+=1
            
            if num_positive_candles < 6:
                signal.iloc[i] = 1.0
                long = True
        
        if price_data.iloc[i]['value'] < short_data.iloc[i]['value'] and long:
            signal.iloc[i] = -1.0
    
    #signal[(signal != 1.0) & (signal != -1.0)] = 0.0
    signal[(signal != 1.0) & (signal != -1.0)] = np.nan
    signal.fillna(method="ffill", inplace=True)

    return signal

def get_rsi_plus_signal(signal):
    """ 
    - 
    """
    signal = signal.copy()
    signal.columns = ['value']
    long = False
    for i in range(1, signal.size):
        if signal.iloc[i-1]['value'] > 70 and signal.iloc[i]['value'] <= 70 and long:
            signal.iloc[i] = -1.0
            long = False
        
        if signal.iloc[i-1]['value'] < 30 and signal.iloc[i]['value'] >= 30 and not long:
            signal.iloc[i] = 1.0
            long = True
    
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0

    return signal

def get_adx_macd_signal(macd, di_plus, di_minus, adx):
    """
    Buy Entry Rules:
    MACD trading above zero
    +Di signal line crosses higher than the D- line
    ADX line is higher than 20 and rises upwards
    Make a buy where the three conditions meet on the candlestick

    Sell Entry Rules:
    MACD trading below zero
    D- line crosses higher than D+ line
    ADX line rises above 20
    Open a short trade where these conditions meet
    """
    macd.columns = ['value']
    di_plus.columns = ['value']
    di_minus.columns = ['value']
    adx.columns = ['value']

    signal = macd.copy()

    signal[(macd > 0) & (di_plus > di_minus) & (adx > 20)] = 1.0
    signal[(macd <= 0) & (di_plus <= di_minus) & (adx > 20)] = -1.0
    signal[signal.isnull()] = 0.0
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0

    return signal