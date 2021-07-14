import numpy as np
import pandas as pd
import bt

def buy_and_hold_strategy(price_data, name='benchmark'):
    # Define the benchmark strategy
    bt_strategy = bt.Strategy(name, 
                              [bt.algos.RunOnce(),
                               bt.algos.SelectAll(),
                               bt.algos.WeighEqually(),
                               bt.algos.Rebalance()])
    # Return the backtest
    return bt.Backtest(bt_strategy, price_data)

def signal_above_strategy(price_data, price_signal, name):
    # the column names must be the same
    price_data.columns = ['value']
    price_signal.columns = ['value']

    bt_strategy = bt.Strategy(name, 
                              [bt.algos.SelectWhere(price_data > price_signal),
                               bt.algos.WeighEqually(),
                               bt.algos.Rebalance()])
    # Return the backtest
    return bt.Backtest(bt_strategy, price_data)

def signal_strategy(price_data, signal, name):
    # the column names must be the same
    price_data.columns = ['value']
    signal.columns = ['value']

    s = bt.Strategy(name,
                    [bt.algos.WeighTarget(signal), 
                    bt.algos.Rebalance()])

    return bt.Backtest(s, price_data)

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

def get_rsi_signal(signal):
    signal[signal > 70] = 1.0
    signal[signal < 30] = -1.0
    signal[(signal <= 70) & (signal >= 30)] = 0.0
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

    signal[(signal != 1.0) & (signal != 1.0)] = 0.0

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
    
    signal[(signal != 1.0) & (signal != 1.0)] = 0.0

    return signal