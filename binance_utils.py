import pandas as pd
import yaml
from datetime import datetime
from binance.client import Client
from binance import ThreadedWebsocketManager
from binance.exceptions import BinanceAPIException, BinanceOrderException

""" To read a cfg file
import configparser

# Loading keys from config file
config = configparser.ConfigParser()
config.read_file(open('<path-to-your-config-file>'))
actual_api_key = config.get('BINANCE', 'ACTUAL_API_KEY')
actual_secret_key = config.get('BINANCE', 'ACTUAL_SECRET_KEY')
 """

def get_credentials():
    with open("config.yml", 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    api_key = cfg['api_creds']['binance1_access_code']
    api_secret = cfg['api_creds']['binance1_secret_code']
    
    return api_key,api_secret

def init():
    api_key, api_secret = get_credentials()

    return Client(api_key, api_secret)

def get_last_timestamp_from(df):
    if df.size > 0:
        last_timestamp_from_df = df.index[-1]
        last_timestamp_from_df = int(datetime.timestamp(last_timestamp_from_df))
    else:
        last_timestamp_from_df = 0
    
    return last_timestamp_from_df * 1000

def update_historical_data(client, df, pair, interval):
    last_timestamp_from_df = get_last_timestamp_from(df)

    # get timestamp of earliest date data is available
    timestamp = client._get_earliest_valid_timestamp(pair, interval)

    if last_timestamp_from_df > timestamp:
        timestamp = last_timestamp_from_df

    df_binance = get_crypto_data(client, pair, interval, timestamp)
    # remove incomplete candle
    df_binance = df_binance[:-1]

    if df.size > 0:
        df_binance_before = df_binance[df_binance.index < df.index[0]]
        df_binance_after = df_binance[df_binance.index > df.index[-1]]

        df = pd.concat([df_binance_before, df, df_binance_after])
        df.drop_duplicates(inplace=True)

        return df
    else:
        return df_binance

def get_twm():
    api_key, api_secret = get_credentials()

    return ThreadedWebsocketManager(api_key = api_key, api_secret = api_secret)

def get_crypto_data(client, pair, interval, timestamp):
    # request historical candle (or klines) data
    data = client.get_historical_klines(pair, interval, timestamp, limit=1000)

    df_crypto = pd.DataFrame(data, columns=[
        'OpenTime', 
        'OpenPrice', 
        'HighPrice', 
        'LowPrice', 
        'ClosePrice', 
        'Volume', 
        'CloseTime', 
        'QuoteVolume', 
        'Trades', 
        'TakerBuyBaseVolume', 
        'TakerBuyQuoteVolume', 
        'Ignore'
    ])

    df_crypto['OpenTime'] = pd.to_datetime(df_crypto['OpenTime'], unit='ms')
    df_crypto.set_index('OpenTime', inplace=True)

    df_crypto = df_crypto[[ 
        'OpenPrice', 
        'HighPrice', 
        'LowPrice', 
        'ClosePrice', 
        'Volume']]
    
    df_crypto['OpenPrice'] = df_crypto['OpenPrice'].astype('float')
    df_crypto['HighPrice'] = df_crypto['HighPrice'].astype('float')
    df_crypto['LowPrice'] = df_crypto['LowPrice'].astype('float')
    df_crypto['ClosePrice'] = df_crypto['ClosePrice'].astype('float')
    df_crypto['Volume'] = df_crypto['Volume'].astype('float')

    return df_crypto

""" Some of the helper functions from python-binance api:
    order_limit_buy()
    order_limit_sell()
    order_market_buy()
    order_market_sell()
    order_oco_buy()
    order_oco_sell() """
def create_market_order(client, symbol, side, quantity, live=False):
    """ Create a market order in binance.

    Parameters
    - client: binance.client.Client
    - symbol: pair to create order, example: 'BTCUSDT' or 'BTCEUR' or 'ETHBTC' ...
    - side: must be 'BUY' or 'SELL'
    - quantity: quantity of coins to create a order
    - live: False (default) for test operations, True for live operations (real coins), be carefull

    Return: order created
    """    
    if not live:
        print('test order')
        return client.create_test_order(symbol=symbol, side=side, type='MARKET', quantity=quantity)
    else:
        print('live order, be carefull!!!')
        try:
            return client.create_order(symbol=symbol, side=side, type='MARKET', quantity=quantity)
        except BinanceAPIException as e:
            # error handling
            print(e)
        except BinanceOrderException as e:
            # error handling
            print(e)
 
def get_asset_balance(client, asset):
    return client.get_asset_balance(asset=asset)

