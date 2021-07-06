from os import close
import numpy as np
import pandas as pd

def sma(close_price, period):
    return close_price.rolling(period).mean()

def ema(close_price, period):
    return close_price.ewm(span=period, adjust=False).mean()

def macd(close_price):
    #Calculate the MACD and Signal Line indicators
    #Calculate the Short Term Exponential Moving Average
    ShortEMA = ema(close_price, 12)

    #Calculate the Long Term Exponential Moving Average
    LongEMA = ema(close_price, 26)

    #Calculate the Moving Average Convergence/Divergence (MACD)
    MACD = ShortEMA - LongEMA

    #Calcualte the signal line
    signal = ema(MACD, 9)

    #Histogram
    macd_histogram = MACD - signal
 
    return MACD, signal, macd_histogram

def rsi(close_price, period = 14):

    delta = close_price.diff()

    up = delta.copy()
    up[up < 0] = 0
    up = pd.Series.ewm(up, alpha=1/period).mean()

    down = delta.copy()
    down[down > 0] = 0
    down *= -1
    down = pd.Series.ewm(down, alpha=1/period).mean()

    rsi = np.where(up == 0, 0, np.where(down == 0, 100, 100 - (100 / (1 + up / down))))

    return np.round(rsi, 2)

def wilder_smoothing(data, periods):
    start = np.where(~np.isnan(data))[0][0] #Check if nans present in beginning
    wilder = np.array([np.nan]*len(data))
    wilder[start+periods-1] = data[start:(start+periods)].mean() #Simple Moving Average
    
    for i in range(start+periods,len(data)):
        wilder[i] = (wilder[i-1]*(periods-1) + data[i])/periods #Wilder Smoothing

    return(wilder)

def smoothing(data, periods):
    start = np.where(~np.isnan(data))[0][0] #Check if nans present in beginning
    smooth = np.array([np.nan]*len(data))
    smooth[start+periods-1] = data[start:(start+periods)].sum()

    for i in range(start+periods,len(data)):
        smooth[i] = (smooth[i-1] - smooth[i-1]/periods) + data[i] #Smoothing
        
    return(smooth)

def adx(high_price, low_price, close_price, period=14):
    """ https://school.stockcharts.com/doku.php?id=technical_indicators:average_directional_index_adx
        https://blog.quantinsti.com/adx-indicator-python/ """

    df_adx = pd.DataFrame(columns=[
        'H-L', 
        'ABS(Hi-Ci-1)', 
        'ABS(Li-Ci-1)', 
        'TR', 
        'DM+', 
        'DM-', 
        'SmoothedDM+', 
        'SmoothedDM-', 
        'SmoothedTR', 
        'DI+', 
        'DI-', 
        'DX',
        'ADX'
    ])

    df_adx['H-L'] = high_price - low_price
    df_adx['ABS(Hi-Ci-1)'] = abs(high_price - close_price.shift())
    df_adx['ABS(Li-Ci-1)'] = abs(low_price - close_price.shift())
    df_adx['TR'] = df_adx[['H-L', 'ABS(Hi-Ci-1)', 'ABS(Li-Ci-1)']].max(axis=1, skipna=False)

    #If (Today's high - Yesterday's High) > (Yesterday's Low - Today's Low), then
    #DM+ = (Today's high - Yesterday's High)
    #Otherwise, it's 0.
    df_adx['DM+'] = np.where(
        (high_price - high_price.shift()) > (low_price.shift() - low_price), #condition
        np.where(high_price - high_price.shift() > 0, high_price - high_price.shift(), 0), #then
        0) #else

    #If (Yesterday's Low - Today's Low) >(Today's high - Yesterday's High), then
    #DM- = (Yesterday's Low - Today's Low)
    #Otherwise, it's 0.
    df_adx['DM-'] = np.where(
        (low_price.shift() - low_price) > (high_price - high_price.shift()), 
        np.where(low_price.shift() - low_price > 0, low_price.shift() - low_price, 0), 
        0)

    #if yesterday's value is NaN, DM must be NaN too
    df_adx['DM+'] = np.where(high_price.shift().isna(), high_price.shift(), df_adx['DM+'])
    df_adx['DM-'] = np.where(low_price.shift().isna(), low_price.shift(), df_adx['DM-'])

    df_adx['SmoothedDM+'] = wilder_smoothing(df_adx['DM+'], period)
    df_adx['SmoothedDM-'] = wilder_smoothing(df_adx['DM-'], period)
    df_adx['SmoothedTR'] = wilder_smoothing(df_adx['TR'], period)

    df_adx['DI+'] = (df_adx['SmoothedDM+']/df_adx['SmoothedTR'])*100
    df_adx['DI-'] = (df_adx['SmoothedDM-']/df_adx['SmoothedTR'])*100
    df_adx['DX'] = abs((df_adx['DI+'] - df_adx['DI-'])/(df_adx['DI+'] + df_adx['DI-']))*100
    df_adx['ADX'] = wilder_smoothing(df_adx['DX'], period)
    
    return df_adx['DI+'], df_adx['DI-'], df_adx['ADX']

def fibonacci_retracement_levels(price_max, price_min):
    diff = price_max - price_min
    level1 = price_max - 0.236 * diff
    level2 = price_max - 0.382 * diff
    level3 = price_max - 0.618 * diff

    return {"price_max": price_max, "level1": level1, "level2": level2, "level3": level3, "price_min": price_min}

def bbands(close_price, period=20, multiplier=2):
    upper = close_price.rolling(period).mean() + close_price.rolling(period).std() * multiplier
    midi = close_price.rolling(period).mean()
    lower = close_price.rolling(period).mean() - close_price.rolling(period).std() * multiplier

    return upper, midi, lower

def fibonacci_retracement_levels(price_max, price_min):
    diff = price_max - price_min
    level1 = price_max - 0.236 * diff
    level2 = price_max - 0.382 * diff
    level3 = price_max - 0.618 * diff

    return {"price_max": price_max, "level1": level1, "level2": level2, "level3": level3, "price_min": price_min}
