# Cryptocurrencies trade bot
The goal of this project is to build a parametrized bot to automate trading based on indicators, statistics and machine/deep/reinforce learning.

**This is a work in progress project, not production ready.**

## Setup
**Install Python dependencies:**

Run the following line in the terminal: `pip install -r requirements.txt`.

**Create a Binance account and api keys:**

(Currently the project only support Binance, but you might create the functions needed to other exchanges.)

- You could use [my referral](https://accounts.binance.com/en/register?ref=43137026) link to create a binance account, if you want.
- Create a new api key. Read [here](https://www.binance.com/en/support/faq/360002502072) for help.

It is recommended to use the Binance Spot Test Network before to avoid mistakes and lose your funds. You could follow [these](https://testnet.binance.vision/) instructions to do this.

**Create a bot in Telegram:**

To receive messages or alerts you need a bot in Telegram. Follow [these](https://medium.com/@ManHay_Hong/how-to-create-a-telegram-bot-and-send-messages-with-python-4cf314d9fa3e) instructions.

**Create user configuration:**

Create a .cfg file named `config.yml` based on `config.yml.example`, then update with your own data. The configuration file consists of the following fields:

- params:

    **mode**: 1 - interactive, 2 - process, when you want to run as a service or in background;

    **symbol**: a list of pairs to trade. Example: ['BTCUSDT', 'ETHUSDT']

    **interval**: an interval to trade. Valid intervals are: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M

    **base_asset_order**: List of the base assets to create na order. Sometimes you want analyze and compute data from a different symbol. The list must be the same lenght as 'symbol'. Example: ['BTC', 'ETH']

    **quote_asset_order**: List of quote assets to create orders. The logic is the same as 'base_asset_order'. Example: ['USDT', 'USDT']

    **live_trade**: live or test trade: 0 - False, 1 - True. Be careful with this parameter.
    
    **roll_oco_orders**: recreate oco orders with new values when they are going to come closer to the limit price: 0 - False, 1 - True.

- api_creds:

    **binance1_access_code**: Binance API key generated in the Binance account setup stage.

    **binance1_secret_code**: Binance secret key generated in the Binance account setup stage.

- api_test_creds:

    **binance_test_access_code**: Binance Spot Test Network api key.

    **binance_test_secret_code**: Binance Spot Test Network secret key.

- telegram:

    **bot_token**: token from telegram bot

    **bot_chatID**: chat id


**Create trade strategies:**

Update `data\trade-strategies.csv` with your strategies. Be careful with `CreateOrders` column!

- Symbol: symbol to trade and calculate statistics/strategy;
- Interval: strategy interval;
- SignalColumnName: column name in the dataframe that has the signal to buy or sell. See the function `update_signal_by_strategy` in the `trade_utils.py` file what strategies can be used.
- CreateOrders: 0 - only send a message, 1 - create orders also.
- BuyAmount: percent of value to buy 1.0 = 100%.
- SellAmount: percent of value to sell 1.0 = 100%.
- Message: message that was sent when the signal to sell or buy is reached.

**Run**

```shell
python trade.py
```

## Jupyter notebooks files

There are also some jupyter notebooks to perform analysis, backtesting and predictions:

- cryptocurrencies_analysis: price analysis and backtesting on strategies;
- cryptocurrencies_ml: a base machine learning model;
- cryptocurrencies_alts: comparative altcoins analysis;
