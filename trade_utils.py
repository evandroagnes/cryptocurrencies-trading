import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from binance_utils import *
from technical_indicator_utils import get_sma, get_macd, get_rsi, get_adx, get_rvi, get_bbands, get_atr
from message_utils import telegram_bot_sendtext
from strategy_utils import *

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
    df['SMA10'] = get_sma(df['ClosePrice'], 10)
    df['SMA20'] = get_sma(df['ClosePrice'], 20)
    df['SMA30'] = get_sma(df['ClosePrice'], 30)
    df['SMA50'] = get_sma(df['ClosePrice'], 50)
    df['SMA100'] = get_sma(df['ClosePrice'], 100)
    df['SMA200'] = get_sma(df['ClosePrice'], 200)
    df['MACD'], df['MACDSignal'], df['MACDHist'] = get_macd(df['ClosePrice'])
    df['RSI'] = get_rsi(df['ClosePrice'])
    df['DI+'], df['DI-'], df['ADX'] = get_adx(df['HighPrice'], df['LowPrice'], df['ClosePrice'])
    df['RVI'], df['RVISignal'] = get_rvi(df['OpenPrice'], df['ClosePrice'], df['LowPrice'], df['HighPrice'])
    df['UpperBBand'], df['MidiBBand'], df['LowerBBand'] = get_bbands(df['ClosePrice'])
    df['ATR'] = get_atr(df['HighPrice'], df['LowPrice'], df['ClosePrice'])

    return df

def update_signal_by_strategy(df, signal_column):
    df = generate_technical_indicators(df)

    if signal_column == 'Signal10SMAStrategy':
        df['Signal10SMAStrategy'] = get_cross_signal(df[['ClosePrice']].copy(), df[['SMA10']].copy())
    elif signal_column == 'Signal20SMAStrategy':
        df['Signal20SMAStrategy'] = get_cross_signal(df[['ClosePrice']].copy(), df[['SMA20']].copy())
    elif signal_column == 'Signal30SMAStrategy':
        df['Signal30SMAStrategy'] = get_cross_signal(df[['ClosePrice']].copy(), df[['SMA30']].copy())
    elif signal_column == 'Signal50SMAStrategy':
        df['Signal50SMAStrategy'] = get_cross_signal(df[['ClosePrice']].copy(), df[['SMA50']].copy())
    elif signal_column == 'Signal100SMAStrategy':
        df['Signal100SMAStrategy'] = get_cross_signal(df[['ClosePrice']].copy(), df[['SMA100']].copy())
    elif signal_column == 'SignalMACDStrategy':
        df['SignalMACDStrategy'] = get_macd_signal(df[['MACDSignal']].copy(), df[['MACD']].copy())
    ### RSI
    # overbought 80 oversold [40, 35, 30, 25, 20]
    elif signal_column == 'SignalRSIStrategy80_40':
        df['SignalRSIStrategy80_40'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=80, oversold_value=40)
    elif signal_column == 'SignalRSIStrategy80_35':
        df['SignalRSIStrategy80_35'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=80, oversold_value=35)
    elif signal_column == 'SignalRSIStrategy80_30':
        df['SignalRSIStrategy80_30'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=80, oversold_value=30)
    elif signal_column == 'SignalRSIStrategy80_25':
        df['SignalRSIStrategy80_25'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=80, oversold_value=25)
    elif signal_column == 'SignalRSIStrategy80_20':
        df['SignalRSIStrategy80_20'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=80, oversold_value=20)
    # overbought 75  oversold [40, 35, 30, 25, 20]
    elif signal_column == 'SignalRSIStrategy75_40':
        df['SignalRSIStrategy75_40'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=75, oversold_value=40)
    elif signal_column == 'SignalRSIStrategy75_35':
        df['SignalRSIStrategy75_35'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=75, oversold_value=35)
    elif signal_column == 'SignalRSIStrategy75_30':
        df['SignalRSIStrategy75_30'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=75, oversold_value=30)
    elif signal_column == 'SignalRSIStrategy75_25':
        df['SignalRSIStrategy75_25'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=75, oversold_value=25)
    elif signal_column == 'SignalRSIStrategy75_20':
        df['SignalRSIStrategy75_20'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=75, oversold_value=20)
    # overbought 70 oversold [40, 35, 30, 25, 20]
    elif signal_column == 'SignalRSIStrategy70_40':
        df['SignalRSIStrategy70_40'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=70, oversold_value=40)
    elif signal_column == 'SignalRSIStrategy70_35':
        df['SignalRSIStrategy70_35'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=70, oversold_value=35)
    elif signal_column == 'SignalRSIStrategy70_30':
        df['SignalRSIStrategy70_30'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=70, oversold_value=30)
    elif signal_column == 'SignalRSIStrategy70_25':
        df['SignalRSIStrategy70_25'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=70, oversold_value=25)
    elif signal_column == 'SignalRSIStrategy70_20':
        df['SignalRSIStrategy70_20'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=70, oversold_value=20)
    # overbought 65 oversold [40, 35, 30, 25, 20]
    elif signal_column == 'SignalRSIStrategy65_40':
        df['SignalRSIStrategy65_40'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=65, oversold_value=40)
    elif signal_column == 'SignalRSIStrategy65_35':
        df['SignalRSIStrategy65_35'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=65, oversold_value=35)
    elif signal_column == 'SignalRSIStrategy65_30':
        df['SignalRSIStrategy65_30'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=65, oversold_value=30)
    elif signal_column == 'SignalRSIStrategy65_25':
        df['SignalRSIStrategy65_25'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=65, oversold_value=25)
    elif signal_column == 'SignalRSIStrategy65_20':
        df['SignalRSIStrategy65_20'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=65, oversold_value=20)
    # overbought 60 oversold [40, 35, 30, 25, 20]
    elif signal_column == 'SignalRSIStrategy60_40':
        df['SignalRSIStrategy60_40'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=60, oversold_value=40)
    elif signal_column == 'SignalRSIStrategy60_35':
        df['SignalRSIStrategy60_35'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=60, oversold_value=35)
    elif signal_column == 'SignalRSIStrategy60_30':
        df['SignalRSIStrategy60_30'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=60, oversold_value=30)
    elif signal_column == 'SignalRSIStrategy60_25':
        df['SignalRSIStrategy60_25'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=60, oversold_value=25)
    elif signal_column == 'SignalRSIStrategy60_20':
        df['SignalRSIStrategy60_20'] = get_rsi_signal(df[['RSI']].copy(), overbought_value=60, oversold_value=20)
    ###
    elif signal_column == 'SignalRSIADXStrategy':
        df['SignalRSIADXStrategy'] = get_rsi_adx_signal(df[['RSI']].copy(), 
                                                        df[['ADX']].copy(), 
                                                        df[['DI+']].copy(), 
                                                        df[['DI-']].copy(), 
                                                        overbought_value=70.0, 
                                                        oversold_value=30.0)
    elif signal_column == 'SignalSMAMACDStrategy':
        df['SignalSMAMACDStrategy'] = get_sma_macd_signal(df[['ClosePrice']].copy(), 
                                                          df[['SMA30']].copy(), 
                                                          df[['SMA100']].copy(), 
                                                          df[['MACD']].copy())
    elif signal_column == 'SignalMACDRVIStrategy':
        df['SignalMACDRVIStrategy'] = get_macd_rvi_signal(df[['MACDSignal']].copy(), 
                                                          df[['MACD']].copy(), 
                                                          df[['RVISignal']].copy(), 
                                                          df[['RVI']].copy())
    elif signal_column == 'SignalBBandsStrategy':
        df['SignalBBandsStrategy'] = get_bbands_signal(df[['ClosePrice']].copy(), 
                                                       df[['UpperBBand']].copy(), 
                                                       df[['LowerBBand']].copy())
    elif signal_column == 'SignalInvertedRSIStrategy':
        df['SignalInvertedRSIStrategy'] = get_inverted_rsi_signal(df[['RSI']].copy())
    elif signal_column == 'SignalDMIStrategy':
        df['SignalDMIStrategy'] = get_dmi_signal(df[['DI+']].copy(), df[['DI-']].copy(), df[['ADX']].copy())

    return df

def roll_oco_orders(client):
    order = {}
    orders = get_all_open_orders(client)

    if len(orders) > 0:
        # eliminate duplicates with set
        order_list_set = set([order['orderListId'] for order in orders if order['orderListId'] != -1])
        for order_list_id in order_list_set:
            orders_by_list_id = [order for order in orders if order['orderListId'] == order_list_id]
            # test if open orders is an oco order
            if len(orders_by_list_id) == 2:
                # get current symbol price
                symbol = [order['symbol'] for order in orders_by_list_id if order['type'] == 'STOP_LOSS_LIMIT'][0]
                current_price = get_lastest_price(client, symbol)

                order_id_stop = [order['orderId'] for order in orders_by_list_id if order['type'] == 'STOP_LOSS_LIMIT'][0]
                quantity = float([order['origQty'] for order in orders_by_list_id if order['type'] == 'STOP_LOSS_LIMIT'][0])
                stop_price = float([order['stopPrice'] for order in orders_by_list_id if order['type'] == 'STOP_LOSS_LIMIT'][0])
                limit_stop_price = float([order['price'] for order in orders_by_list_id if order['type'] == 'STOP_LOSS_LIMIT'][0])
                limit_price = float([order['price'] for order in orders_by_list_id if order['type'] == 'LIMIT_MAKER'][0])
                side = [order['side'] for order in orders_by_list_id if order['type'] == 'STOP_LOSS_LIMIT'][0]
                trade_info = get_trade_info(client, symbol)

                roll = False
                new_limit_price = 0.0
                new_stop_price = 0.0
                new_stop_limit_price = 0.0
                if side == 'SELL':
                    increment = (limit_price - limit_stop_price) / 3
                    increment = get_trunc_value(increment, float(trade_info['min_price']))
                    roll = current_price > (limit_price - increment)
                    #SELL = stop > limit_stop
                    new_limit_price = limit_price + increment
                    new_stop_price = (stop_price + increment) * 1.001
                    new_stop_limit_price = stop_price + increment
                elif side == 'BUY':
                    increment = (limit_stop_price - limit_price) / 3
                    increment = get_trunc_value(increment, float(trade_info['min_price']))
                    roll = current_price < (limit_price + increment)
                    new_limit_price = limit_price - increment
                    #BUY = limit_stop > stop
                    new_stop_price = stop_price - increment
                    new_stop_limit_price = (stop_price - increment) * 1.001

                if roll:
                    # cancel old orders
                    result = cancel_order(client=client,
                        symbol=symbol,
                        order_id=order_id_stop)
                    print(result)

                    # recreate order with increment value
                    order = create_oco_order(
                        client=client,
                        symbol=symbol,
                        side=side,
                        quantity=quantity,
                        stop_price=get_trunc_value(new_stop_price, float(trade_info['min_price'])),
                        stop_limit_price=get_trunc_value(new_stop_limit_price, float(trade_info['min_price'])),
                        price=new_limit_price)
                    
                    print(order)
                    telegram_bot_sendtext('OCO orders are rolled: ' + str(order))
    
    return

def process_candle(client, symbol, df, new_row, base_asset, quote_asset):
    df = add_row(df, new_row)
    symbol_order = base_asset + quote_asset

    # Read every call because the strategy can be changed in the file.
    path = Path(__file__).parent
    filename = path / 'trading-strategies.csv'
    df_strategies = pd.read_csv(filename)
    df_strategies = df_strategies[df_strategies['Symbol'] == symbol]

    for index, strategy in df_strategies.iterrows():
        interval = strategy['Interval']
        message_strategy = strategy['Message']
        signal_column = strategy['SignalColumnName']
        create_orders = bool(strategy['CreateOrders'])
        is_percent_buy = bool(strategy['IsPercentBuy'])
        buy_amount = float(strategy['BuyAmount'])
        is_percent_sell = bool(strategy['IsPercentSell'])
        sell_amount = float(strategy['SellAmount'])
        oco_strategy = bool(strategy['OCOStrategy'])

        message = ''

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
                        if is_percent_buy:
                            quote_balance = quote_balance * buy_amount
                        elif (not is_percent_buy and buy_amount < quote_balance):
                            quote_balance = buy_amount
                        
                        quote_balance = get_trunc_value(quote_balance, float(trade_info_dict['tick_size']))
                        print(symbol_order + ' quantity to buy: ' + str(quote_balance))

                        if quote_balance > float(trade_info_dict['quote_asset_min_value']):
                            order = create_market_order(client, symbol_order, side, quote_balance)
                            message = 'Buy order sent: ' + str(order)
                            print(message)

                            if oco_strategy:
                                    # TODO create a param to this
                                    # number of candles to get min price
                                    num_candles_min_price = 5
                                    # get buy value
                                    buy_value = get_trunc_value(pd.DataFrame(order['fills'])['price'].astype('float').mean(), 
                                        float(trade_info_dict['min_price']))
                                    quantity = float(order['executedQty'])
                                    # stop = get min low value of last 5 candles
                                    stop_value = df_trade[-num_candles_min_price:]['LowPrice'].min()
                                    # use average true range * 2 as threshold
                                    stop_value = stop_value - (df_trade['ATR'][-1] * 2)
                                    stop_value = get_trunc_value(stop_value, float(trade_info_dict['min_price']))
                                    price_value = buy_value + (2 * (buy_value - stop_value))
                                    price_value = get_trunc_value(price_value, float(trade_info_dict['min_price']))
                                    # create OCO order to sell
                                    oco_order = create_oco_order(
                                        client=client,
                                        symbol=symbol_order,
                                        side='SELL',
                                        quantity=quantity,
                                        stop_price=stop_value * 1.001,
                                        stop_limit_price=stop_value,
                                        price=price_value)

                                    print('OCO order sent: ' + str(oco_order))
                                    telegram_bot_sendtext('OCO order sent: ' + str(oco_order))
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
                        if is_percent_sell:
                            qty = balance * sell_amount
                        elif (not is_percent_sell and sell_amount < balance):
                            qty = sell_amount

                        qty = get_trunc_value(qty, float(trade_info_dict['base_asset_step_size']))
                        if qty > balance:
                            qty = get_trunc_value(qty - float(trade_info_dict['base_asset_step_size']), float(trade_info_dict['base_asset_step_size']))
                            
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
    save_all = False
    path = Path(__file__).parent
    filename = path / str('data/' + pair + '-1m-binance.parquet')
    filename_all =  path / str('data/' + pair + '-1m-binance-all.parquet')

    try:
        table = pq.read_table(filename_all)
        df = table.to_pandas()
        save_all = True
    except FileNotFoundError:
        try:
            table = pq.read_table(filename)
            df = table.to_pandas()
        except FileNotFoundError:
            df = initialize_ohlc_df()
            save_all = True

    df = update_historical_data(client, df, pair, '1m')
    
    if save:
        # save all data
        if save_all:
            table = pa.Table.from_pandas(df)
            pq.write_table(table, filename_all)

        # create data file with data from 2020 until now for share (github)
        df_from_2021 = df['2021-1-1':]
        table_from_2021 = pa.Table.from_pandas(df_from_2021)
        pq.write_table(table_from_2021, filename)
    
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

def get_num_daily_bars(df):
    NUM_SECONDS_IN_A_DAY=86400

    delta = df.index[-1] - df.index[-2]

    return int(NUM_SECONDS_IN_A_DAY / ((NUM_SECONDS_IN_A_DAY * delta.days) + delta.seconds))