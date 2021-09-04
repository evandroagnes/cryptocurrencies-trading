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
