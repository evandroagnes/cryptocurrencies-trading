import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from binance_utils import update_historical_data, get_asset_balance, create_market_order, get_round_value, get_trade_info
from technical_indicator_utils import get_sma, get_macd, get_rsi, get_adx
from message_utils import telegram_bot_sendtext
from strategy_utils import get_cross_signal, get_macd_signal, get_rsi_signal, get_rsi_adx_signal, get_sma_macd_signal

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
    df['SMA30'] = get_sma(df['ClosePrice'], 30)
    df['SMA50'] = get_sma(df['ClosePrice'], 50)
    df['SMA100'] = get_sma(df['ClosePrice'], 100)
    df['SMA200'] = get_sma(df['ClosePrice'], 200)
    df['MACD'], df['MACDSignal'], df['MACDHist'] = get_macd(df['ClosePrice'])
    df['RSI'] = get_rsi(df['ClosePrice'])
    df['DI+'], df['DI-'], df['ADX'] = get_adx(df['HighPrice'], df['LowPrice'], df['ClosePrice'])

    return df

def update_signal_by_strategy(df, signal_column):
    df = generate_technical_indicators(df)

    if signal_column == 'Signal30SMAStrategy':
        df['Signal30SMAStrategy'] = get_cross_signal(df[['ClosePrice']].copy(), df[['SMA30']].copy())
    elif signal_column == 'Signal50SMAStrategy':
        df['Signal50SMAStrategy'] = get_cross_signal(df[['ClosePrice']].copy(), df[['SMA50']].copy())
    elif signal_column == 'SignalMACDStrategy':
        df['SignalMACDStrategy'] = get_macd_signal(df[['MACDSignal']].copy(), df[['MACD']].copy())
    elif signal_column == 'SignalRSIStrategy':
        df['SignalRSIStrategy'] = get_rsi_signal(df[['RSI']].copy())
    elif signal_column == 'SignalRSIADXStrategy':
        df['SignalRSIADXStrategy'] = get_rsi_adx_signal(df[['RSI']].copy(), 
                                                        df[['ADX']].copy(), 
                                                        df[['DI+']].copy(), 
                                                        df[['DI-']].copy(), 
                                                        overbought_value=70.0, 
                                                        oversold_value=30.0)
    elif signal_column == 'SignalSMAMACDStrategy':
        df['SignalSMAMACDStrategy'] = get_sma_macd_signal(df[['ClosePrice']].copy(), 
                                                          df[['SMA50']].copy(), 
                                                          df[['SMA100']].copy(), 
                                                          df[['MACD']].copy())

    return df

def process_candle(client, symbol, df, new_row, base_asset, quote_asset):
    df = add_row(df, new_row)
    symbol_order = base_asset + quote_asset

    # Read every call because the strategy can be changed in the file.
    df_strategies = pd.read_csv('trade-strategies.csv')
    df_strategies = df_strategies[df_strategies['Symbol'] == symbol]

    for index, strategy in df_strategies.iterrows():
        interval = strategy['Interval']
        message_strategy = strategy['Message']
        signal_column = strategy['SignalColumnName']
        create_orders = bool(strategy['CreateOrders'])
        buy_amount = float(strategy['BuyAmount'])
        sell_amount = float(strategy['SellAmount'])

        if is_candle_closed(df, interval):
            df_trade = resample_data(df, interval)
            df_trade = update_signal_by_strategy(df_trade, signal_column)

            if df_trade[signal_column][-2] != df_trade[signal_column][-1]:
                # get info about create orders (minimum, maximum, ...)
                trade_info_dict = get_trade_info(client, symbol_order)
                fee = trade_info_dict['exchange_fee']

                if df_trade[signal_column][-1] == 1:
                    side = 'BUY'
                    if create_orders:
                        # TODO extract method with buy/sell rules
                        ### BUY ORDER
                        # Get quote_asset balance
                        quote_balance, _ = get_asset_balance(client, quote_asset)
                        quote_balance = quote_balance * buy_amount
                        quote_balance = get_round_value(quote_balance, float(trade_info_dict['tick_size']))
                        print(symbol_order + ' quantity to buy: ' + str(quote_balance))

                        if quote_balance > float(trade_info_dict['quote_asset_min_value']):
                            order = create_market_order(client, symbol_order, side, quote_balance)
                            message = 'Buy order sent: ' + str(order)
                            print(message)
                        else:
                            message = 'Unable to BUY, ' + quote_asset + ' without balance: ' + str(quote_balance)
                    else:
                        message = side + ' ' + symbol + ' (' + interval + ' Trade): ' + message_strategy + '!'
                elif df_trade[signal_column][-1] == -1:
                    side = 'SELL'
                    if create_orders:
                        ### SELL ORDER
                        # get total balance asset
                        balance, _ = get_asset_balance(client, base_asset)
                        #balance = balance * (1.0 - fee)
                        qty = balance * sell_amount
                        qty = get_round_value(qty, float(trade_info_dict['base_asset_step_size']))
                        if qty > balance:
                            qty = get_round_value(qty - float(trade_info_dict['base_asset_step_size']), float(trade_info_dict['base_asset_step_size']))
                            
                        print(symbol_order + ' quantity to sell: ' + str(qty))

                        if qty > float(trade_info_dict['base_asset_min_qty']):
                            order = create_market_order(client, symbol_order, side, qty)
                            message = 'Sell order sent: ' + str(order)
                            print(message)
                        else:
                            message = 'Unable to SELL, ' + base_asset + ' without balance: ' + str(balance)
                    else:
                        message = side + ' ' + symbol + ' (' + interval + ' Trade): ' + message_strategy + '!'
                
                telegram_bot_sendtext(message)

    return df

def get_data(client, pair, interval, save=True):
    try:
        table = pq.read_table('data/' + pair + '-1m-binance-all.parquet')
        df = table.to_pandas()
    except FileNotFoundError:
        try:
            table = pq.read_table('data/' + pair + '-1m-binance.parquet')
            df = table.to_pandas()
        except FileNotFoundError:
            df = initialize_ohlc_df()

    df = update_historical_data(client, df, pair, '1m')
    
    if save:
        # save all data
        filename = 'data/' + pair + '-1m-binance-all.parquet'
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)

        # create data file with data from 2020 until now for share (github)
        df_from_2020 = df['2020-1-1':]
        filename = 'data/' + pair + '-1m-binance.parquet'
        table_from_2020 = pa.Table.from_pandas(df_from_2020)
        pq.write_table(table_from_2020, filename)
    
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