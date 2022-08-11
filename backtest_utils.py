import numpy as np
import pandas as pd
import bt

def buy_and_hold_strategy(price_data, name='benchmark', _initial_capital=1000000.0):
    # Define the benchmark strategy
    s = bt.Strategy(name,
                    [bt.algos.RunOnce(),
                     bt.algos.SelectAll(),
                     bt.algos.WeighEqually(),
                     bt.algos.Rebalance()])
    # Return the backtest
    return bt.Backtest(strategy=s, data=price_data, integer_positions=False, initial_capital=_initial_capital)

def signal_above_strategy(price_data, price_signal, name, _initial_capital=1000000.0):
    # the column names must be the same
    price_data.columns = ['value']
    price_signal.columns = ['value']

    s = bt.Strategy(name, 
                    [bt.algos.SelectWhere(price_data > price_signal),
                     bt.algos.WeighEqually(),
                     bt.algos.Rebalance()])
    # Return the backtest
    return bt.Backtest(s, price_data, integer_positions=False, initial_capital=_initial_capital)

def signal_strategy(price_data, signal, name, _initial_capital=1000000.0):
    weight = convert_signal_to_weight(signal.copy())

    # the column names must be the same
    price_data.columns = ['value']
    weight.columns = ['value']

    s = bt.Strategy(name,
                    [bt.algos.WeighTarget(weight), 
                     bt.algos.Rebalance()])

    return bt.Backtest(strategy=s, data=price_data, integer_positions=False, initial_capital=_initial_capital)

def convert_signal_to_weight(signal):
    return signal.replace(0, np.nan).replace(-1, 0).ffill().replace(np.nan, 0)