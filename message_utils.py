import requests
import yaml

# To create your Telegram bot read this:
# https://medium.com/@ManHay_Hong/how-to-create-a-telegram-bot-and-send-messages-with-python-4cf314d9fa3e

def get_telegram_bot_ids():
    with open("config.yml", 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)

    bot_token = cfg['telegram']['bot_token']
    bot_chatID = cfg['telegram']['bot_chatID']
    
    return bot_token, bot_chatID

def telegram_bot_sendtext(bot_message):
    
    bot_token, bot_chatID = get_telegram_bot_ids()
    
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message

    response = requests.get(send_text)

    return response.json()
