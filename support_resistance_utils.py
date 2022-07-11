from machine_learning_utils import get_kmeans_clusters
import pandas as pd
import numpy as np

"""
Based on:
https://medium.com/@judopro/using-machine-learning-to-programmatically-determine-stock-support-and-resistance-levels-9bb70777cf8e
https://realpython.com/k-means-clustering-python/
https://towardsdatascience.com/detection-of-price-support-and-resistance-levels-in-python-baedc44c34c9
https://medium.datadriveninvestor.com/how-to-detect-support-resistance-levels-and-breakout-using-python-f8b5dac42f21
https://www.thefinanalytics.com/post/support-and-resistance-levels
"""

def get_kmeans_levels(df):
    lows = pd.DataFrame(data=df, index=df.index, columns=['LowPrice'])
    highs = pd.DataFrame(data=df, index=df.index, columns=['HighPrice'])

    low_clusters, _ = get_kmeans_clusters(lows)
    low_centers = low_clusters.cluster_centers_
    low_centers = np.sort(low_centers, axis=0)

    high_clusters, _ = get_kmeans_clusters(highs)
    high_centers = high_clusters.cluster_centers_
    high_centers = np.sort(high_centers, axis=0)

    levels = []
    for low in low_centers:
        if (low[0] < df['ClosePrice'][-1]):
            levels.append((0, low[0]))

    for high in high_centers:
        if (high[0] > df['ClosePrice'][-1]):
            levels.append((0, high[0]))

    return levels

def is_support(df, i):
  cond1 = df['LowPrice'][i] < df['LowPrice'][i-1] 
  cond2 = df['LowPrice'][i] < df['LowPrice'][i+1] 
  cond3 = df['LowPrice'][i+1] < df['LowPrice'][i+2] 
  cond4 = df['LowPrice'][i-1] < df['LowPrice'][i-2]
  return (cond1 and cond2 and cond3 and cond4)

def is_resistance(df, i):
  cond1 = df['HighPrice'][i] > df['HighPrice'][i-1] 
  cond2 = df['HighPrice'][i] > df['HighPrice'][i+1] 
  cond3 = df['HighPrice'][i+1] > df['HighPrice'][i+2] 
  cond4 = df['HighPrice'][i-1] > df['HighPrice'][i-2]
  return (cond1 and cond2 and cond3 and cond4)

def get_fractal_levels(df):
  levels = []
  for i in range(2 ,len(df) - 2):
    if is_support(df, i):
      low = df['LowPrice'][i]
      if is_far_from_level(low, levels, df):
        levels.append((i, low))
    elif is_resistance(df, i):
      high = df['HighPrice'][i]
      if is_far_from_level(high, levels, df):
        levels.append((i, high))
  
  return levels

def get_window_shifting_levels(df, window=5):
    levels = []
    max_list = []
    min_list = []
    for i in range(window, len(df) - window):
        high_range = df['HighPrice'][i - window:i + window]
        current_max = high_range.max()

        if current_max not in max_list:
            max_list = []

        max_list.append(current_max)
        if len(max_list) == window and is_far_from_level(current_max, levels, df):
            levels.append((df.index.get_loc(high_range.idxmax()), current_max))
        
        low_range = df['LowPrice'][i - window:i + window]
        current_min = low_range.min()

        if current_min not in min_list:
            min_list = []

        min_list.append(current_min)
        if len(min_list) == window and is_far_from_level(current_min, levels, df):
            levels.append((df.index.get_loc(low_range.idxmin()), current_min))

    return levels

def is_far_from_level(value, levels, df):
    ave =  np.mean(df['HighPrice'] - df['LowPrice'])
    return np.sum([abs(value - level) < ave for _, level in levels]) == 0

def has_breakout(levels, previous_candle, last_candle):
  for _, level in levels:
    cond1 = (previous_candle['OpenPrice'] < level) # to make sure previous candle is below the level
    cond2 = (last_candle['OpenPrice'] > level) and (last_candle['LowPrice'] > level)
  return (cond1 and cond2)
