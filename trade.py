import sys
import pandas as pd
import yaml
from datetime import datetime
from time import sleep

from binance_utils import get_twm, init, init_test, get_trade_info
from trade_utils import get_data, process_candle

# Set to True to print debug messages from code
debug = False

# Trade Parameters
with open("config.yml", 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

mode = cfg['params']['mode']
symbol = cfg['params']['symbol']
interval = cfg['params']['interval']
base_asset_order = cfg['params']['base_asset_order']
quote_asset_order = cfg['params']['quote_asset_order']
live_trade = bool(cfg['params']['live_trade'])
create_orders = bool(cfg['params']['create_orders'])
# End trade parameters

# create binance client and a instance of ThreadedWebsocketManager
if live_trade:
    client = init()
    print('LIVE TRADE!!!')
else:
    client = init_test()
    base_asset_order = 'BTC'
    quote_asset_order = 'USDT'

    print('Test Trade...')

symbol_trade = base_asset_order + quote_asset_order
trade_info_dict = get_trade_info(client, symbol_trade)

twm = get_twm()
df = pd.DataFrame()

def handle_socket_message(msg):
    #print(f"message type: {msg['e']}")
    #print(msg)

    global df
    if msg['e'] == 'error':
        print(msg)
        # close and restart the socket
        twm.stop()
        sleep(3)
        twm.start_kline_socket(callback=handle_socket_message, symbol=symbol, interval=interval)
    else:
        candle = msg['k']
        timestamp = candle['t'] / 1000
        timestamp = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

        timestamp_close = candle['T'] / 1000
        timestamp_close = datetime.fromtimestamp(timestamp_close).strftime('%Y-%m-%d %H:%M:%S')

        if debug:
            print(timestamp, timestamp_close, candle)

        is_candle_closed = candle['x']

        if is_candle_closed:
            new_row = {
                'OpenTime':timestamp, 
                'OpenPrice':candle['o'], 
                'HighPrice':candle['h'], 
                'LowPrice':candle['l'], 
                'ClosePrice':candle['c'], 
                'Volume':candle['v']}
            
            if debug:
                print(new_row)

            if df.size == 0:
                df = get_data(client, symbol, interval, save=False)
            
            # process data
            df = process_candle(client, df, new_row, base_asset_order, quote_asset_order, trade_info_dict, create_orders)

            if debug:
                print(df.tail())

def main(mode):
    try:
        twm.start()
        print('Trade started...')

        twm.start_kline_socket(callback=handle_socket_message, symbol=symbol, interval=interval)
    
        while 1:
            if mode == 1:
                selection = input('Your selection? (h for help) ')
                
                if len(selection) > 1:
                    print('Enter one character only.')
                elif selection == 'h':
                    print('Select only one character option from available list:')
                    print('\n\t h : help')
                    print('\n\t e : exit')
                    print('\n\t p : print last candles')
                elif selection == 'e':
                    print('Exiting the program and stopping all processes.')
                    # close connection
                    twm.stop()
                    sys.exit('Finished and exiting.')
                elif selection == 'p':
                    print(df.tail())
                else:
                    print('Unknown option.')
    except KeyboardInterrupt:
        print(df.tail())
        print('Exiting the program and stopping all processes.')
        # close connection
        twm.stop()
        sys.exit('Finished and exiting.')

if __name__ == "__main__":   
    main(mode)
