from machine_learning_utils import get_kmeans_clusters
from sklearn.cluster import AgglomerativeClustering
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

def get_agglomerative_clustering(df, rolling_wave_length, num_clusters):
    df = df.copy()
    df.reset_index(inplace=True)

    # Create min and max waves
    max_waves_temp = df['HighPrice'].rolling(rolling_wave_length).max().rename('waves')
    min_waves_temp = df['LowPrice'].rolling(rolling_wave_length).min().rename('waves')
    max_waves = pd.concat([max_waves_temp, pd.Series(np.zeros(len(max_waves_temp)) + 1)], axis=1)
    min_waves = pd.concat([min_waves_temp, pd.Series(np.zeros(len(min_waves_temp)) + -1)], axis=1)

    #  Remove dups
    max_waves.drop_duplicates('waves', inplace=True)
    min_waves.drop_duplicates('waves', inplace=True)
    #  Merge max and min waves
    waves = max_waves.append(min_waves).sort_index()
    waves = waves[waves[0] != waves[0].shift()].dropna()

    # Find Support/Resistance with clustering using the rolling stats
    # Create [x,y] array where y is always 1
    x = np.concatenate((waves.waves.values.reshape(-1, 1),
                        (np.zeros(len(waves)) + 1).reshape(-1, 1)), axis=1)

    # Initialize Agglomerative Clustering
    cluster = AgglomerativeClustering(n_clusters=num_clusters, metric='euclidean', linkage='ward')
    cluster.fit_predict(x)
    waves['clusters'] = cluster.labels_

    # Get index of the max wave for each cluster
    waves2 = waves.loc[waves.groupby('clusters')['waves'].idxmax()]
    waves2.waves.drop_duplicates(keep='first', inplace=True)

    levels = []
    for index, row in waves2.iterrows():
      levels.append((0, row['waves']))

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

def get_all_pivot_point_levels(df):
  pivot_point = (df['HighPrice'] + df['LowPrice'] + df['ClosePrice']) / 3
  support_l1 = (pivot_point * 2) - df['HighPrice']
  support_l2 = pivot_point - (df['HighPrice'] - df['LowPrice'])
  support_l3 = df['LowPrice'] - 2 * (df['HighPrice'] - pivot_point)
  resistance_l1 = (pivot_point * 2) - df['LowPrice']
  resistance_l2 = pivot_point + (df['HighPrice'] - df['LowPrice'])
  resistance_l3 = df['HighPrice'] + 2 * (pivot_point - df['LowPrice'])

  return pivot_point, support_l1, support_l2, support_l3, resistance_l1, resistance_l2, resistance_l3

def get_pivot_point_levels(df):
  _, support_l1, support_l2, support_l3, resistance_l1, resistance_l2, resistance_l3 = get_all_pivot_point_levels(df)
  levels = []
  levels.append((0, support_l1[-1]))
  levels.append((0, support_l2[-1]))
  levels.append((0, support_l3[-1]))
  levels.append((0, resistance_l1[-1]))
  levels.append((0, resistance_l2[-1]))
  levels.append((0, resistance_l3[-1]))

  return levels

def is_far_from_level(value, levels, df):
    ave =  np.mean(df['HighPrice'] - df['LowPrice'])
    return np.sum([abs(value - level) < ave for _, level in levels]) == 0

def has_breakout(levels, previous_candle, last_candle):
  for _, level in levels:
    cond1 = (previous_candle['OpenPrice'] < level) # to make sure previous candle is below the level
    cond2 = (last_candle['OpenPrice'] > level) and (last_candle['LowPrice'] > level)
  return (cond1 and cond2)
