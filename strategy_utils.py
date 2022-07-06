import numpy as np
import pandas as pd

def remove_repeated_values(values):
    return values.where(values.diff().ne(0)).fillna(0.0)

def remove_repeated_signal(signal, column_name):
    signal = signal.copy()
    signal[column_name] = np.where(signal[column_name] == 0.0, np.nan, signal[column_name])
    signal[column_name] = signal[column_name].ffill()

    return remove_repeated_values(signal)

def get_cross_signal(short_data, long_data, buy_first=True):
    # the column names must be the same
    short_data.columns = ['value']
    long_data.columns = ['value']

    signal = long_data.copy()
    signal[short_data > long_data] = 1.0
    signal[short_data <= long_data] = -1.0

    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal

def get_macd_signal(signal_macd, macd_value, buy_first=True):
    signal_macd.columns = ['value']
    macd_value.columns = ['value']
    
    signal = macd_value.copy()

    signal[signal_macd < macd_value] = 1.0
    signal[signal_macd >= macd_value] = -1.0

    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal 

def get_macd_rvi_signal(signal_macd, macd_value, signal_rvi, rvi_value, buy_first=True):
    """
    Reference: https://forexwot.com/super-macd-rvi-trading-strategy-for-day-trading-crypto-forex-stocks-high-winrate-strategy.html
    """
    signal_macd.columns = ['value']
    macd_value.columns = ['value']
    signal_rvi.columns = ['value']
    rvi_value.columns = ['value']
    
    signal = macd_value.copy()
 
    #long
    signal[(signal_macd < macd_value) & (signal_rvi < rvi_value)] = 1.0
    #exit
    #signal[(signal_macd > macd_value) & (signal.shift() == 1.0)] = -1.0

    #short
    signal[(signal_macd > macd_value) & (signal_rvi > rvi_value)] = -1.0
    #exit
    #signal[(signal_macd < macd_value) & (signal.shift() == -1.0)] = 1.0

    signal[(signal != 1.0) & (signal != -1.0)] = 0.0
    
    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal 

def get_rsi_signal(signal, overbought_value=70.0, oversold_value=30.0, buy_first=True, remove_repeated_signals=True):
    signal.columns = ['value']
    signal[signal < oversold_value] = 1.0
    signal[signal > overbought_value] = -1.0

    signal[(signal != 1.0) & (signal != -1.0)] = 0.0
    
    if buy_first:
        signal.iloc[0]['value'] = 1.0

    if remove_repeated_signals:
        signal = remove_repeated_signal(signal, 'value')

    return signal

def get_rsi_enter_signal(signal, overbought_value=70.0, oversold_value=30.0, buy_first=True):
    signal.columns = ['value']
    signal[(signal.shift() > oversold_value) & (signal <= oversold_value)] = 1.0
    signal[(signal.shift() < overbought_value) & (signal >= overbought_value)] = -1.0
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0
    
    if buy_first:
        signal.iloc[0]['value'] = 1.0

    return signal

def get_rsi_return_signal(signal, overbought_value=70.0, oversold_value=30.0, buy_first=True):
    signal.columns = ['value']
    signal[(signal.shift() < oversold_value) & (signal >= oversold_value)] = 1.0
    signal[(signal.shift() > overbought_value) & (signal <= overbought_value)] = -1.0
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0

    if buy_first:
        signal.iloc[0]['value'] = 1.0

    return signal

def get_inverted_rsi_signal(signal, overbought_value=70.0, oversold_value=30.0, buy_first=True):
    """
    Buy when overbought begins (signal >= overbought_value) and sell when signal returns to a value below overbought_value.
    Sell when oversold begins (signal <= oversold_value) and buy when signal returns to a value above oversold_value.
    """

    signal.columns = ['value']
    signal[(signal >= overbought_value) & (signal.shift() < overbought_value)] = 1.0
    signal[(signal < overbought_value) & (signal.shift() >= overbought_value)] = -1.0

    signal[(signal <= oversold_value) & (signal.shift() > oversold_value)] = -1.0
    signal[(signal > oversold_value) & (signal.shift() <= oversold_value)] = 1.0

    signal[(signal != 1.0) & (signal != -1.0)] = 0.0
    
    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal

def get_rsi_adx_signal(signal, adx, di_plus, di_minus, overbought_value=70.0, oversold_value=30.0, adx_value=25.0, buy_first=True):
    """
    https://usethebitcoin.com/how-to-trade-pullbacks-using-rsi-and-adx/
    """
    signal.columns = ['value']
    adx.columns = ['value']
    di_plus.columns = ['value']
    di_minus.columns = ['value']

    signal[(signal < oversold_value) & (adx > adx_value) & (di_minus > di_plus)] = 1.0
    signal[(signal > overbought_value) & (adx > adx_value) & (di_minus < di_plus)] = -1.0
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0
    
    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal

def get_5_minute_signal(price_data, macd_history, ema, buy_first=True):
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

    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal

def get_sma_macd_signal(price_data, short_data, long_data, macd_data, buy_first=True):
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
    
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0

    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal

def get_adx_macd_signal(macd, di_plus, di_minus, adx, buy_first=True):
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
    signal[(macd < 0) & (di_plus < di_minus) & (adx > 20)] = -1.0
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0
    
    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal

def get_bbands_signal(price_data, upper_band, lower_band, buy_first=True):
    price_data.columns = ['value']
    upper_band.columns = ['value']
    lower_band.columns = ['value']

    signal = price_data.copy()
    signal[(price_data.shift() < lower_band) & (price_data >= lower_band)] = 1.0
    signal[(price_data.shift() > upper_band) & (price_data <= upper_band)] = -1.0
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0
    
    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal

def get_rsi_bbands_signal(price_data, upper_band, lower_band, rsi, buy_first=True):
    """
    https://www.linkedin.com/pulse/algorithmic-trading-mean-reversion-using-python-bryan-chen/
    """
    price_data.columns = ['value']
    upper_band.columns = ['value']
    lower_band.columns = ['value']
    rsi.columns = ['value']

    signal = price_data.copy()
    signal[(rsi < 30) & (price_data < lower_band)] = 1.0
    signal [(rsi > 70) & (price_data > upper_band)] = -1.0
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0

    #buy/sell next trading day
    #signal = signal.shift()
    #signal = signal.fillna(0)

    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal

def get_dmi_signal(di_plus, di_minus, adx, adx_value=25.0, buy_first=True):
    di_plus.columns = ['value']
    di_minus.columns = ['value']
    adx.columns = ['value']

    signal = adx.copy()
    signal[(di_plus > di_minus) & (adx >= adx_value) & (di_plus > di_plus.shift())] = 1.0
    signal[(di_plus < di_minus) & (adx >= adx_value) & (di_minus > di_minus.shift())] = -1.0
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0

    if buy_first:
        signal.iloc[0]['value'] = 1.0

    signal = remove_repeated_signal(signal, 'value')

    return signal

def get_ema_atr_signal(price_data, rsi_value, ema_value, atr_value, oversold_value=30):
    """
    entry: rsi < oversold_value & price > ema200
    stop-loss: ema 200 - (atr/2)
    profit: price + ((price - stop-loss) * 2)
    """
    price_data.columns = ['value']
    rsi_value.columns = ['value']
    ema_value.columns = ['value']
    atr_value.columns = ['value']

    signal = price_data.copy()
    long = False
    stop_loss = 0.0
    profit = 0.0
    for i in range(price_data.size):
        #entry
        if price_data.iloc[i]['value'] > ema_value.iloc[i]['value'] and rsi_value.iloc[i]['value'] < oversold_value and not long:
            signal.iloc[i] = 1.0
            stop_loss = ema_value.iloc[i]['value'] - (atr_value.iloc[i]['value'] / 2)
            profit = price_data.iloc[i]['value'] + ((price_data.iloc[i]['value'] - stop_loss) * 2)
            long = True
        
        #stop-loss
        if price_data.iloc[i]['value'] < stop_loss and long:
            signal.iloc[i] = -1.0
            long = False

        #profit
        if price_data.iloc[i]['value'] > profit and long:
            signal.iloc[i] = -1.0
            long = False
            
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0

    return signal

def get_rsi_atr_signal(price_data, rsi_value, atr_value, oversold_value=30):
    """
    entry: rsi < oversold_value
    stop-loss: price - (atr * 2)
    profit: price + ((price - stop-loss) * 2)
    """
    price_data.columns = ['value']
    rsi_value.columns = ['value']
    atr_value.columns = ['value']

    signal = price_data.copy()
    long = False
    stop_loss = 0.0
    profit = 0.0
    for i in range(price_data.size):
        #entry
        if rsi_value.iloc[i]['value'] < oversold_value and not long:
            signal.iloc[i] = 1.0
            stop_loss = price_data.iloc[i]['value'] - (atr_value.iloc[i]['value'] * 2)
            profit = price_data.iloc[i]['value'] + ((price_data.iloc[i]['value'] - stop_loss) * 2)
            long = True
        
        #stop-loss
        if price_data.iloc[i]['value'] < stop_loss and long:
            signal.iloc[i] = -1.0
            long = False

        #profit
        if price_data.iloc[i]['value'] > profit and long:
            signal.iloc[i] = -1.0
            long = False
            
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0

    return signal