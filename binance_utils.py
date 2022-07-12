import pandas as pd
import numpy as np
import yaml
from datetime import datetime
from urllib3.exceptions import ReadTimeoutError
from pathlib import Path
from binance.client import Client
from binance import ThreadedWebsocketManager
from binance.exceptions import BinanceAPIException, BinanceOrderException
from binance.enums import *

""" To read a cfg file
import configparser

# Loading keys from config file
config = configparser.ConfigParser()
config.read_file(open('<path-to-your-config-file>'))
actual_api_key = config.get('BINANCE', 'ACTUAL_API_KEY')
actual_secret_key = config.get('BINANCE', 'ACTUAL_SECRET_KEY')
 """

def get_credentials(test=False):
    path = Path(__file__).parent
    filename = path / 'config.yml'
    with open(filename, 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    
    if test:
        api_key = cfg['api_test_creds']['binance_test_access_code']
        api_secret = cfg['api_test_creds']['binance_test_secret_code']
    else:
        api_key = cfg['api_creds']['binance1_access_code']
        api_secret = cfg['api_creds']['binance1_secret_code']
    
    return api_key, api_secret

def init():
    api_key, api_secret = get_credentials()

    return Client(api_key, api_secret, {"timeout": 15})

def init_test():
    api_key, api_secret = get_credentials(test=True)

    client = Client(api_key, api_secret)
    client.API_URL = 'https://testnet.binance.vision/api'

    return client

def get_trunc_value(value, precision):
    """
    Trunc value at decimal point passed as parameter. Not round.
    Ex.: trunc(5.999, 0.01) returns 5.99, not 6.00.
    """
    value = '{:0.8f}'.format(value)
    precision = '{:0.8f}'.format(precision)

    if (float(precision) >= 1) | (precision.split('.')[1].find('1') < 0):
        return float(value.split('.')[0])
    else:
        decimal_points = int(precision.split('.')[1].find('1')) + 1
        return float(value.split('.')[0] + "." + value.split('.')[1][0:decimal_points])

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

def create_market_order(client, symbol, side, quantity):
    """ Create a market order in binance.

    Parameters
    - client: binance.client.Client
    - symbol: pair to create order, example: 'BTCUSDT' or 'BTCEUR' or 'ETHBTC' ...
    - side: must be 'BUY' or 'SELL'
    - quantity: quantity to create a order.
                - to BUY: quantity of quote_asset;
                - to SELL: quantity of base_asset. 

    Return: order created
    """
    order = {}

    try:
        if side == 'BUY':
            order = client.order_market_buy(symbol=symbol, quoteOrderQty=quantity)
        elif side == 'SELL':
            order = client.order_market_sell(symbol=symbol, quantity=quantity)
        else:
            # TODO handle wrong side
            print('Wrong side, must be \'BUY\' or \'SELL\'! ' + side)
    except BinanceAPIException as e:
        # error handling
        print(e)
        raise
    
    return order

def create_oco_order(client, symbol, side, quantity, stop_price, stop_limit_price, price):
    order = {}
    try:
        order = client.create_oco_order(
            symbol=symbol,
            side=side,
            stopLimitTimeInForce=TIME_IN_FORCE_GTC,
            quantity=quantity,
            stopPrice='{:0.8f}'.format(stop_price),
            stopLimitPrice='{:0.8f}'.format(stop_limit_price),
            price='{:0.8f}'.format(price))
    except BinanceAPIException as e:
        # error handling
        print(e)
        raise

    #order = 'Create an oco order (quantity: ' + str(quantity) + ', stop: ' + str(stop_price) + ', price: ' + str(price) + ')'
    
    return order

def cancel_order(client, symbol, order_id):
    result = {}
    try:
        result =  client.cancel_order(symbol=symbol, orderId=order_id)
    except BinanceAPIException as e:
        # error handling
        print(e)
        raise

    #result = 'Order canceled: ' + str(order_id)
    return result

def get_asset_balance(client, asset):
    balance = client.get_asset_balance(asset=asset)
    
    if balance is None:
        return float(0.0), float(0.0)

    return float(balance['free']), float(balance['locked'])

def get_trade_info(client, symbol):
    info = client.get_symbol_info(symbol)

    base_asset_precision = info['baseAssetPrecision']
    quote_asset_precision = info['quoteAssetPrecision']

    # filters
    for filter in info['filters']:
        if filter['filterType'] == 'PRICE_FILTER':
            min_price = filter['minPrice']
            max_price = filter['maxPrice']
            tick_size = filter['tickSize']
        elif filter['filterType'] == 'LOT_SIZE':
            base_asset_min_qty = filter['minQty']
            base_asset_max_qty = filter['maxQty']
            base_asset_step_size = filter['stepSize']
        elif filter['filterType'] == 'MIN_NOTIONAL':
            quote_asset_min_value = filter['minNotional']
        elif filter['filterType'] == 'MARKET_LOT_SIZE':
            market_min_qty = filter['minQty']
            market_max_qty = filter['maxQty']

    return {'base_asset_precision': base_asset_precision, 
            'quote_asset_precision': quote_asset_precision,
            'min_price': min_price,
            'max_price': max_price,
            'tick_size': tick_size,
            'base_asset_min_qty': base_asset_min_qty,
            'base_asset_max_qty': base_asset_max_qty,
            'base_asset_step_size': base_asset_step_size,
            'quote_asset_min_value': quote_asset_min_value,
            'market_min_qty': market_min_qty,
            'market_max_qty': market_max_qty,
            'exchange_fee': 0.00075}

def get_lastest_price(client, symbol):
    """
    Get symbol lastest price.
    """
    btc_price = client.get_symbol_ticker(symbol=symbol)
    
    return float(btc_price['price'])

def get_all_symbols(client):
    """
    Returns a DataFrame with all pair symbols in the exchange with TRADING status:
    Status	    Can trade?	Can cancel order?	Market data generated?
    TRADING		yes		    yes			        yes
    END_OF_DAY	no		    yes			        no
    HALT		no		    yes			        yes
    BREAK		no		    yes			        no

    https://dev.binance.vision/t/explanation-on-symbol-status/118
    """
    exchange_info = client.get_exchange_info()
    symbols_df = pd.DataFrame(exchange_info['symbols'])
    symbols_df = symbols_df[symbols_df['status'] == 'TRADING']
    return symbols_df[['symbol', 'baseAsset', 'quoteAsset', 'status']].set_index('symbol')

def get_all_lastest_price(client):
    """
    Get lastest price from all tickers.
    """
    prices_df = pd.DataFrame(client.get_all_tickers())
    prices_df.set_index('symbol', inplace=True)
    #prices_df['price'] = prices_df['price'].astype('float')
    return prices_df

def get_open_orders(client, symbol):
    try:
        #client.get_open_orders(symbol=symbol, requests_params={'timeout': 5})
        return client.get_open_orders(symbol=symbol)
    except ReadTimeoutError as e:
        print(e)
        raise

def get_all_open_orders(client):
    try:
        #client.get_open_orders(symbol=symbol, requests_params={'timeout': 5})
        return client.get_open_orders()
    except ReadTimeoutError as e:
        print(e)
        raise

def get_trades(client, symbol):
    """
    Get trades by symbol.
    """
    df_orders = pd.DataFrame(client.get_my_trades(symbol=symbol))
    df_orders['time'] = pd.to_datetime(df_orders['time'], unit='ms')
    df_orders['price'] = df_orders['price'].astype('float')
    df_orders['qty'] = df_orders['qty'].astype('float')
    df_orders['quoteQty'] = df_orders['quoteQty'].astype('float')
    df_orders = df_orders.groupby(['time', 'symbol', 'orderId', 'isBuyer'])[['price', 'qty', 'quoteQty']].agg({'price': 'mean', 'qty': 'sum', 'quoteQty': 'sum'})
    df_orders.reset_index(inplace=True)
    df_orders['side'] = np.where(df_orders['isBuyer'], 'BUY', 'SELL')
    df_orders = df_orders.rename(columns={'price' : 'avgPrice'})

    return df_orders[['time', 'symbol', 'orderId', 'side', 'avgPrice', 'qty', 'quoteQty']].copy()
