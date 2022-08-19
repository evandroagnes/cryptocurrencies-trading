import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller
from sklearn.cluster import KMeans
from kneed import KneeLocator

from strategy_utils import remove_repeated_signal

def get_labels_future_returns(price_data, return_threshold=0.0):
    """
    Binary labelling.
 
    If future returns > return_threshold then signal = 1 (buy)
    If future returns < 0 and long then signal = -1 (sell)
    Otherwise signal = 0 (do nothing).
 
    Parameters
    ----------
    price_data : asset's close price
    return_threshold: threshold for long signal (buy) in percent
     
    Returns
    -------
    labs : A pandas dataframe containing the labels for each return
    """
    price_data = price_data.copy()

    # Create a column 'FutureReturns' with the calculation of percentage change
    price_data['FutureReturns'] = price_data.pct_change().shift(-1)

    # Create the signal column with BUY signals
    signal = np.where(price_data['FutureReturns'] >= return_threshold, 1.0, 0)

    # SELL signals
    long = False
    for i in range(price_data.shape[0]):
        if signal[i] == 1.0:
            long = True
        
        if price_data.iloc[i]['FutureReturns'] < 0 and long:
            signal[i] = -1.0
            long = False

    price_data['signal'] = signal
    return remove_repeated_signal(price_data[['signal']], 'signal')

def get_labels_fixed_time_horizon(price_data, threshold):
    """
    Fixed-time horizon labelling.
 
    Compute the financial labels using the fixed-time horizon procedure.
 
    Parameters
    ----------
    price_data : pandas.DataFrame or pandas.Series
        The data from which the labels are to be calculated.
      
    threshold : int
        The predefined constant threshold to compute the labels.
 
    Returns
    -------
    labs : pandas.DataFrame
        A pandas dataframe containing the returns and the labels for each 
        return.
    
    References
    ----------
    https://quantdare.com/4-simple-ways-to-label-financial-data-for-machine-learning/
    """
    price_data = price_data.copy()
    price_data['Return'] = price_data.pct_change()
    # labs to store labels
    labs = pd.DataFrame(index=price_data.index, columns=['Return', 'Label'])
 
    # get indices for each label
    idx_lower = price_data[price_data['Return'] < -threshold].index
    idx_middle = price_data[abs(price_data['Return']) <= threshold].index
    idx_upper = price_data[price_data['Return'] > threshold].index
 
    # assign labels depending on indices
    labs['Return'] = price_data['Return']
    labs.loc[idx_lower, 'Label'] = -1
    labs.loc[idx_middle, 'Label'] = 0
    labs.loc[idx_upper, 'Label'] = 1

    return labs

def is_stationary(series):
    """
    Function to check if the series is stationary or not.
    """
    result = adfuller(series)
    if(result[1] < 0.05):
        return True
    else:
        return False

def get_pair_above_threshold(X, threshold):
    """
    Function to return the pairs with correlation above threshold.
    """
    # Calculate the correlation matrix
    correl = X.corr()

    # Unstack the matrix
    correl = correl.abs().unstack()

    # Recurring & redundant pair
    pairs_to_drop = set()
    cols = X.corr().columns
    for i in range(0, X.corr().shape[1]):
        for j in range(0, i+1):
            pairs_to_drop.add((cols[i], cols[j]))

    # Drop the recurring & redundant pair
    correl = correl.drop(labels=pairs_to_drop).sort_values(ascending=False)

    return correl[correl > threshold].index

def split_sequence(sequence, n_steps):
    X = []
    y = []

    for i in range(n_steps, len(sequence)):
        X.append(sequence[i - n_steps:i])
        y.append(sequence[i, -1])

    return np.array(X), np.array(y)

def get_kmeans_clusters(df, k=0):
    '''
    :param df: dataframe with only one data column, usually low or high prices
    :param k: if k is setted by a value different from zero, it is used as number of clusters
    :return: clusters with the best K centers, inertia

    This method uses elbow method to find the best number of K clusters
    '''
    wcss = []
    k_models = []

    size = min(11, len(df.index))
    for i in range(1, size):
        kmeans = KMeans(n_clusters = i, init = 'k-means++', max_iter = 300, n_init = 10, random_state = 42)
        kmeans.fit(df)
        wcss.append(kmeans.inertia_)
        k_models.append(kmeans)

    if (k == 0):
        kl = KneeLocator(range(1, 11), wcss, curve = 'convex', direction = 'decreasing')
        k = kl.elbow + 1

    clusters = k_models[k - 1]

    return clusters, wcss
