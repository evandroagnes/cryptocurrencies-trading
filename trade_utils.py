import pandas as pd
from binance_utils import update_historical_data, get_asset_balance, create_market_order, get_round_value
from technical_indicator_utils import sma
from message_utils import telegram_bot_sendtext

def initialize_ohlc_df():
    df = pd.DataFrame(columns=[
        'OpenTime', 
        'OpenPrice', 
        'HighPrice', 
        'LowPrice', 
        'ClosePrice', 
        'Volume'])
    
    df.set_index('OpenTime', inplace=True)

    return df

def add_row(df, row):
    df.loc[pd.to_datetime(row['OpenTime'])] = [pd.to_numeric(row['OpenPrice']),
        pd.to_numeric(row['HighPrice']),
        pd.to_numeric(row['LowPrice']),
        pd.to_numeric(row['ClosePrice']),
        pd.to_numeric(row['Volume'])]
 
    return df

def resample_data(df, time_resample):
    df_resample = df.copy()
    summaries = {'OpenPrice': 'first', 'HighPrice': 'max', 'LowPrice': 'min', 'ClosePrice': 'last', 'Volume': 'sum'}

    df_resample = df.resample(time_resample).agg(summaries)
    df_resample.dropna(inplace=True)
    # remove incomplete candle
    df_resample = df_resample[:-1]

    return df_resample

def generate_technical_indicators(df):
    df['SMA50'] = sma(df['ClosePrice'], 50)

    return df

def update_signal_by_strategy(df):
    df = generate_technical_indicators(df)

    signal = df[['ClosePrice']].copy()
    signal[df['ClosePrice'] > df['SMA50']] = 1.0
    signal[df['ClosePrice'] <= df['SMA50']] = -1.0
    #TODO update NaN and 0 to last signal 1 or -1
    #signal[signal.isnull()] = 0.0

    df['signal'] = signal

    return df

def process_candle(client, df, new_row, base_asset, quote_asset, trade_info_dict, create_orders=False):
    df = add_row(df, new_row)
    symbol = base_asset + quote_asset
    fee = trade_info_dict['exchange_fee']

    # Read every call because the strategy can be changed in the file.
    df_strategies = pd.read_csv('data/trade-strategies.csv')

    for index, strategy in df_strategies.iterrows():
        interval = strategy['Interval']
        message_strategy = strategy['Message']
        create_orders = bool(strategy['CreateOrders'])

        if is_candle_closed(df, interval):
            df_trade = resample_data(df, interval)
            df_trade = update_signal_by_strategy(df_trade)

            if df_trade['signal'][-2] != df_trade['signal'][-1]:
                if df_trade['signal'][-1] == 1:
                    side = 'BUY'
                    if create_orders:
                        # TODO extract method with buy/sell rules
                        ### BUY ORDER
                        # Get quote_asset balance
                        quote_balance, _ = get_asset_balance(client, quote_asset)
                        quote_balance = get_round_value(quote_balance, float(trade_info_dict['min_price']))

                        if quote_balance > float(trade_info_dict['quote_asset_min_value']):
                            order = create_market_order(client, symbol, side, quote_balance)
                            message = 'Buy order sent: ' + str(order)
                            print(message)
                        else:
                            message = 'Unable to BUY, ' + quote_asset + ' without balance: ' + str(quote_balance)
                    else:
                        message = side + ' (' + interval + ' Trade): ' + message_strategy + '!'
                else:
                    side = 'SELL'
                    if create_orders:
                        ### SELL ORDER
                        # get total balance asset
                        balance, balance_locked = get_asset_balance(client, base_asset)
                        #balance = balance * (1.0 - fee)
                        qty = get_round_value(balance, float(trade_info_dict['base_asset_min_qty']))

                        if balance > 0:
                            order = create_market_order(client, symbol, side, qty)
                            message = 'Sell order sent: ' + str(order)
                            print(message)
                        else:
                            message = 'Unable to SELL, ' + base_asset + ' without balance: ' + str(balance)
                    else:
                        message = side + ' (' + interval + ' Trade): ' + message_strategy + '!'
                
                telegram_bot_sendtext(message)

    return df

def get_data(client, pair, interval, save=True):
    try:
        df = pd.read_csv('data/' + pair + '-1m-binance-all.csv')
        df['OpenTime'] = pd.to_datetime(df['OpenTime'])
        df.set_index('OpenTime', inplace=True)
    except FileNotFoundError:
        try:
            df = pd.read_csv('data/' + pair + '-1m-binance.csv')
            df['OpenTime'] = pd.to_datetime(df['OpenTime'])
            df.set_index('OpenTime', inplace=True)
        except FileNotFoundError:
            df = initialize_ohlc_df()

    df = update_historical_data(client, df, pair, '1m')
    
    if save:
        # save all data
        filename = 'data/' + pair + '-1m-binance-all.csv'
        df.to_csv(filename)

        # create data file with data from 2020 until now for share (github)
        df_from_2020 = df['2020-1-1':]
        filename = 'data/' + pair + '-1m-binance.csv'
        df_from_2020.to_csv(filename)
    
    # valid intervals - 1min, 3min, 5min, 15min, 30min, 1H, 2H, 4H, 6H, 8H, 12H, 1D, 3D, 1W, 1M
    # TODO validate input
    if interval == '1min' or interval == '1m':
        return df
    else:
        df = resample_data(df, interval)
        return df

def is_candle_closed(df, interval):
    # valid strategy intervals - 1min, 3min, 5min, 15min, 30min, 1H, 2H, 4H, 6H, 8H, 12H, 1D, 3D, 1W, 1M
    if interval == '1min':
        return True
    elif interval == '3min':
        if df.index.minute[-1] % 3 == 0:
            return True
    elif interval == '5min':
        if df.index.minute[-1] % 5 == 0:
            return True
    elif interval == '15min':
        if df.index.minute[-1] % 15 == 0:
            return True
    elif interval == '30min':
        if df.index.minute[-1] % 30 == 0:
            return True
    elif interval == '1H':
        if df.index.hour[-2] != df.index.hour[-1]:
            return True
    elif interval == '2H':
        if (df.index.hour[-2] != df.index.hour[-1]) and (df.index.hour[-2] % 2 == 0):
            return True
    elif interval == '4H':
        if (df.index.hour[-2] != df.index.hour[-1]) and (df.index.hour[-2] % 4 == 0):
            return True
    elif interval == '6H':
        if (df.index.hour[-2] != df.index.hour[-1]) and (df.index.hour[-2] % 6 == 0):
            return True
    elif interval == '8H':
        if (df.index.hour[-2] != df.index.hour[-1]) and (df.index.hour[-2] % 8 == 0):
            return True
    elif interval == '12H':
        if (df.index.hour[-2] != df.index.hour[-1]) and (df.index.hour[-2] % 12 == 0):
            return True
    elif interval == '1D':
        if df.index.day[-2] != df.index.day[-1]:
            return True
    elif interval == '3D':
        delta = df.index[-1] - df.index[0]
        # delta.days -1 because the last day is incomplete
        if (delta.days - 1) % 3 == 0:
            return True
    elif interval == '1W':
        delta = df.index[-1] - df.index[0]
        # delta.days -1 because the last day is incomplete
        if (delta.days - 1) % 7 == 0:
            return True
    elif interval == '1M':
        if df.index.month[-2] != df.index.month[-1]:
            return True
    
    return False