import numpy as np
import pandas as pd
from hurst import compute_Hc

def get_sma(close_price, period):
    return close_price.rolling(period).mean()

def get_ema(close_price, period):
    return close_price.ewm(span=period, adjust=False).mean()

def get_macd(close_price):
    #Calculate the MACD and Signal Line indicators
    #Calculate the Short Term Exponential Moving Average
    ShortEMA = get_ema(close_price, 12)

    #Calculate the Long Term Exponential Moving Average
    LongEMA = get_ema(close_price, 26)

    #Calculate the Moving Average Convergence/Divergence (MACD)
    MACD = ShortEMA - LongEMA

    #Calcualte the signal line
    signal = get_ema(MACD, 9)

    #Histogram
    macd_histogram = MACD - signal
 
    return MACD, signal, macd_histogram

def get_rsi(close_price, period = 14):

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

def get_wilder_smoothing(data, periods):
    start = np.where(~np.isnan(data))[0][0] #Check if nans present in beginning
    wilder = np.array([np.nan]*len(data))
    wilder[start+periods-1] = float(data[start:(start+periods)].mean()) #Simple Moving Average
    
    for i in range(start+periods,len(data)):
        wilder[i] = (wilder[i-1]*(periods-1) + data.iloc[i])/periods #Wilder Smoothing

    return(wilder)

def get_smoothing(data, periods):
    start = np.where(~np.isnan(data))[0][0] #Check if nans present in beginning
    smooth = np.array([np.nan]*len(data))
    smooth[start+periods-1] = data[start:(start+periods)].sum()

    for i in range(start+periods,len(data)):
        smooth[i] = (smooth[i-1] - smooth[i-1]/periods) + data[i] #Smoothing
        
    return(smooth)

def get_adx(high_price, low_price, close_price, period=14):
    """ https://school.stockcharts.com/doku.php?id=technical_indicators:average_directional_index_adx
        https://blog.quantinsti.com/adx-indicator-python/ """

    df_adx = pd.DataFrame(columns=[
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

    df_adx['TR'] = get_tr(high_price, low_price, close_price)

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

    df_adx['SmoothedDM+'] = get_wilder_smoothing(df_adx['DM+'], period)
    df_adx['SmoothedDM-'] = get_wilder_smoothing(df_adx['DM-'], period)
    df_adx['SmoothedTR'] = get_wilder_smoothing(df_adx['TR'], period)

    df_adx['DI+'] = (df_adx['SmoothedDM+']/df_adx['SmoothedTR'])*100
    df_adx['DI-'] = (df_adx['SmoothedDM-']/df_adx['SmoothedTR'])*100
    df_adx['DX'] = abs((df_adx['DI+'] - df_adx['DI-'])/(df_adx['DI+'] + df_adx['DI-']))*100
    df_adx['ADX'] = get_wilder_smoothing(df_adx['DX'], period)
    
    return df_adx['DI+'], df_adx['DI-'], df_adx['ADX']

def get_tr(high_price, low_price, close_price):
    high_low = high_price - low_price
    high_close = abs(high_price - close_price.shift())
    low_close = abs(low_price - close_price.shift())

    return pd.concat([high_low, high_close, low_close], axis=1).max(axis=1, skipna=False)

def get_atr(high_price, low_price, close_price, period=14):

    return get_tr(high_price, low_price, close_price).ewm(alpha=1/period, adjust=False).mean()

def get_fibonacci_retracement_levels(price_max, price_min):
    diff = price_max - price_min
    level1 = price_max - 0.236 * diff
    level2 = price_max - 0.382 * diff
    level3 = price_max - 0.618 * diff

    return {"price_max": price_max, "level1": level1, "level2": level2, "level3": level3, "price_min": price_min}

def get_bbands(close_price, period=20, multiplier=2):
    upper = close_price.rolling(period).mean() + close_price.rolling(period).std() * multiplier
    midi = close_price.rolling(period).mean()
    lower = close_price.rolling(period).mean() - close_price.rolling(period).std() * multiplier
    bbw = (upper - lower) / close_price.rolling(period).mean()

    return upper, midi, lower, bbw

def get_momentum(close_price, n):
    return close_price / close_price.shift(n) - 1

def get_sma_ratio(close_price, n):
    return close_price / close_price.rolling(n).mean() - 1

def get_sharpe_ratio(returns):
    return returns.mean() / returns.std()

def get_bbands_ratio(close_price, n=20):
    return (close_price - close_price.rolling(n).mean()) / (2 * close_price.rolling(n).std())

def normalize(data):
    #return (data.values - data.values.mean()) / data.values.std()
    return (data - data.mean()) / data.std()

def bband_width(upper_band, lower_band, midi_band):
    """
    This technical indicator provides an easy way to visualize consolidation before price movements (low bandwidth values) or 
    periods of higher volatility (high bandwidth values).
    The Bollinger Band Width uses the same two parameters as the Bollinger Bands: 
    -a simple moving-average period (for the middle band) and 
    -the number of standard deviations by which the upper and lower bands should be offset from the middle band.
    """

    return (upper_band - lower_band) / midi_band

def bband_b(close_price, upper_band, lower_band):
    return ((close_price - lower_band) / (upper_band - lower_band)) * 100

def get_hurst(price_data, window=200):
    """
    Hurst = 0.5 -> Brownian Motion (random walk)
    Hurst < 0.5 -> Mean reversion
    Hurst > 0.5 -> Momentum/Trend

    Reference: https://www.coursera.org/learn/machine-learning-trading-finance
    """
    hurst = np.zeros((len(price_data), 1))
    for i in range(0, len(price_data)):
        if i < window:
            hurst[i] = np.nan
        else:
            H, c, val = compute_Hc(price_data[i - window:i], kind='price')
            hurst[i] = H

    return hurst

def get_hurst_signal(hurst):
    """
    Indicate the persistent nature of the market.
    """
    signal = hurst.copy()
    signal[(hurst > 0.65)] = 1.0
    signal[(hurst < 0.35)] = -1.0
    signal[(signal != 1.0) & (signal != -1.0)] = 0.0

    return signal

def get_rvi(open_price, close_price, low_price, high_price, period=10):
    """
    Reference: https://kaabar-sofien.medium.com/the-relative-vigor-index-coding-trading-in-python-29af776f57cc
    """
    a = close_price - open_price
    b = 2 * (close_price.shift(2) - open_price.shift(2))
    c = 2 * (close_price.shift(3) - open_price.shift(3))
    d = 2 * (close_price.shift(4) - open_price.shift(4))

    e = high_price - low_price
    f = 2 * (high_price.shift(2) - low_price.shift(2))
    g = 2 * (high_price.shift(3) - low_price.shift(3))
    h = 2 * (high_price.shift(4) - low_price.shift(4))

    numerator = (a + b + c + d) / 6
    denominator = (e + f + g + h) / 6

    rvi = get_sma(numerator, period) / get_sma(denominator, period)

    rvi1 = 2 * rvi.shift()
    rvi2 = 2 * rvi.shift(2)
    rvi3 = rvi.shift(3)

    signal = (rvi + rvi1 + rvi2 + rvi3) / 6

    return rvi, signal

def get_stochastic_oscillator(close_price, low_price, high_price, period=3):
    """
    Reference: https://www.investopedia.com/terms/s/stochasticoscillator.asp
    """

    sok = ((close_price - low_price.rolling(14).min()) / (high_price.rolling(14).max() - low_price.rolling(14).min())) * 100
    sod = sok.rolling(period).mean()

    return sok, sod

def get_fma(close_price):
    
    # Adding Columns
    data = close_price.copy()

    # Calculating Different Moving Averages
    data['ema1'] = get_ema(close_price, 5)    
    data['ema2'] = get_ema(close_price, 8)    
    data['ema3'] = get_ema(close_price, 13)    
    data['ema4'] = get_ema(close_price, 21)    
    data['ema5'] = get_ema(close_price, 34)    
    data['ema6'] = get_ema(close_price, 55)    
    data['ema7'] = get_ema(close_price, 89)    
    data['ema8'] = get_ema(close_price, 144)    
    data['ema9'] = get_ema(close_price, 233)    
    data['ema10'] = get_ema(close_price, 377)    
    data['ema11'] = get_ema(close_price, 610)    
    data['ema12'] = get_ema(close_price, 987)    
    data['ema13'] = get_ema(close_price, 1597) 
    data['ema14'] = get_ema(close_price, 2584) 
    data['ema15'] = get_ema(close_price, 4181) 
    data['ema16'] = get_ema(close_price, 6765) 
    
    # Calculating the High FMA
    data['fma_high'] = data[['ema1', 
                        'ema2', 
                        'ema3', 
                        'ema4', 
                        'ema5', 
                        'ema6', 
                        'ema7', 
                        'ema8', 
                        'ema9', 
                        'ema10', 
                        'ema11', 
                        'ema12', 
                        'ema13', 
                        'ema14', 
                        'ema15', 
                        'ema16']].sum(axis=1) / 16

    # Calculating Different Moving Averages
    data['ema1'] = get_ema(data[['fma_high']], 5)    
    data['ema2'] = get_ema(data[['fma_high']], 8)    
    data['ema3'] = get_ema(data[['fma_high']], 13)    
    data['ema4'] = get_ema(data[['fma_high']], 21)    
    data['ema5'] = get_ema(data[['fma_high']], 34)    
    data['ema6'] = get_ema(data[['fma_high']], 55)    
    data['ema7'] = get_ema(data[['fma_high']], 89)    
    data['ema8'] = get_ema(data[['fma_high']], 144)    
    data['ema9'] = get_ema(data[['fma_high']], 233)    
    data['ema10'] = get_ema(data[['fma_high']], 377)    
    data['ema11'] = get_ema(data[['fma_high']], 610)    
    data['ema12'] = get_ema(data[['fma_high']], 987)    
    data['ema13'] = get_ema(data[['fma_high']], 1597) 
    data['ema14'] = get_ema(data[['fma_high']], 2584) 
    data['ema15'] = get_ema(data[['fma_high']], 4181) 
    data['ema16'] = get_ema(data[['fma_high']], 6765) 
    
    # Calculating the High FMA
    data['fma_low'] = data[['ema1', 
                        'ema2', 
                        'ema3', 
                        'ema4', 
                        'ema5', 
                        'ema6', 
                        'ema7', 
                        'ema8', 
                        'ema9', 
                        'ema10', 
                        'ema11', 
                        'ema12', 
                        'ema13', 
                        'ema14', 
                        'ema15', 
                        'ema16']].sum(axis=1) / 16

    return data[['fma_high', 'fma_low']]
