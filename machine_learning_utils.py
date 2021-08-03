import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller

def get_labels_future_returns(price_data):
    """Binary labelling.
 
    If future returns > 0 then signal = 1 (buy) otherwise = -1 (sell).
 
    Parameters
    ----------
    price_data : asset's close price
     
    Returns
    -------
    labs : A pandas dataframe containing the labels for each return.
    """
    price_data = price_data.copy()

    # Create a column 'FutureReturns' with the calculation of percentage change
    price_data['FutureReturns'] = price_data.pct_change().shift(-1)

    # Create the signal column
    price_data['signal'] = np.where(price_data['FutureReturns'] > 0, 1, 0)

    return price_data[['signal']].copy()

def get_labels_fixed_time_horizon(price_data, threshold):
    """Fixed-time horizon labelling.
 
    Compute the financial labels using the fixed-time horizon procedure.
 
    Parameters
    ----------
    data : pandas.DataFrame or pandas.Series
        The data from which the labels are to be calculated. The data should be
        returns and not prices.
    name : str, optional, default: 'Close'
        Column to extract the labels from.        
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
    """Function to check if the series is stationary or not.
    """
    result = adfuller(series)
    if(result[1] < 0.05):
        return True
    else:
        return False

def get_pair_above_threshold(X, threshold):
    """Function to return the pairs with correlation above threshold.
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
