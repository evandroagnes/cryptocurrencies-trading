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
    price_data.columns = ['price']
    price_signal.columns = ['price']

    bt_strategy = bt.Strategy(name, 
                              [bt.algos.SelectWhere(price_data > price_signal),
                               bt.algos.WeighEqually(),
                               bt.algos.Rebalance()])
    # Return the backtest
    return bt.Backtest(bt_strategy, price_data)

def signal_strategy(price_data, signal, name):
    # the column names must be the same
    price_data.columns = ['price']
    signal.columns = ['price']

    s = bt.Strategy(name,
                    [bt.algos.WeighTarget(signal), 
                    bt.algos.Rebalance()])

    return bt.Backtest(s, price_data)

def get_cross_signal(short_data, long_data):
    # the column names must be the same
    short_data.columns = ['price']
    long_data.columns = ['price']

    signal = long_data.copy()
    signal[short_data > long_data] = 1.0
    signal[short_data <= long_data] = -1.0
    signal[long_data.isnull()] = 0.0

    return signal

def get_macd_signal(signal_macd, macd_value):
    signal_macd.columns = ['macd']
    macd_value.columns = ['macd']
    
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

