import sys
import pandas as pd
import yaml
from datetime import datetime
from time import sleep

from binance_utils import get_twm, init, init_test
from trade_utils import get_data, process_candle

# Set to True to print debug messages from code
debug = False

# Trade Parameters
with open("config.yml", 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

mode = cfg['params']['mode']
symbol_list = cfg['params']['symbol']
base_asset_order_list = cfg['params']['base_asset_order']
quote_asset_order_list = cfg['params']['quote_asset_order']
interval = cfg['params']['interval']
live_trade = bool(cfg['params']['live_trade'])
# End trade parameters

# create a dictionary with the symbol and it respective order asset
base_asset_order_dic = {}
quote_asset_order_dic = {}
for i in range(len(symbol_list)):
    base_asset_order_dic[symbol_list[i]] = base_asset_order_list[i]
    quote_asset_order_dic[symbol_list[i]] = quote_asset_order_list[i]

# create binance client and a instance of ThreadedWebsocketManager
if live_trade:
    client = init()
    print('LIVE TRADE!!!')
else:
    client = init_test()
    print('Test Trade...')

twm = get_twm()
symbol_data = {}

def handle_socket_message(msg):
    #print(f"message type: {msg['e']}")
    #print(msg)

    global symbol_data
    symbol = msg['s']

    if msg['e'] == 'error':
        print(msg)
        # close and restart the socket
        twm.stop()
        sleep(3)

        for symbol in symbol_list:
            twm.start_kline_socket(callback=handle_socket_message, symbol=symbol, interval=interval)
    else:
        #symbol = msg['s']
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
            
            if symbol not in symbol_data:
                symbol_data[symbol] = get_data(client, symbol, interval, save=False)
            
            # process data
            symbol_data[symbol] = process_candle(client,
                    symbol,
                    symbol_data[symbol], 
                    new_row, 
                    base_asset_order_dic[symbol], 
                    quote_asset_order_dic[symbol])

            if debug:
                print(symbol_data[symbol].tail())

def print_last_candle():
    for key in symbol_data:
        print('{}: {:%Y-%m-%d %H:%M:%S} ClosePrice: {:0.8f}'.format(key, symbol_data[key].index[-1], symbol_data[key]['ClosePrice'][-1]))

def main(mode):
    try:
        twm.start()
        print('Trade started...')

        for symbol in symbol_list:
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
                    print_last_candle()
                else:
                    print('Unknown option.')
    except KeyboardInterrupt:
        print_last_candle()
        
        print('Exiting the program and stopping all processes.')
        # close connection
        twm.stop()
        sys.exit('Finished and exiting.')

if __name__ == "__main__":   
    main(mode)
