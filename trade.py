import sys
import pandas as pd
from datetime import datetime
from time import sleep

from binance_utils import get_twm, init
from trade_utils import get_historical_data, process_candle

# Set to True to print debug messages from code
debug = False

symbol = 'BTCUSDT'
# valid intervals - 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
interval = '1m'

# Asset to create an order
base_asset_order = 'BTC'
quote_asset_order = 'EUR'

df = pd.DataFrame()

# create binance client and a instance of ThreadedWebsocketManager
client = init()
twm = get_twm()

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
                df = get_historical_data(client)
            
            # process data
            df = process_candle(client, df, new_row, base_asset_order, quote_asset_order)

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
    n = len(sys.argv)
    mode = 0
    if n > 2:
        print("You must inform zero or one argument: 1 for interactive or 2 from process!")
        sys.exit('Finished and exiting.')
    elif n == 1:
        mode = 1
    elif (sys.argv[1] == '1' or sys.argv[1] == '2'):
        mode = int(sys.argv[1])
    else:
        print("Argument invalid, use: 1 for interactive or 2 from process!")
        sys.exit('Finished and exiting.')
    
    main(mode)
