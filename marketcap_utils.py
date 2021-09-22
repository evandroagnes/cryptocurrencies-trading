import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import json

# https://bryanf.medium.com/web-scraping-crypto-prices-with-python-41072ea5b5bf

def get_coinmarketcap_data(num_pages=1):
    url = 'https://coinmarketcap.com/'
    df_coinmarketcap = pd.DataFrame(columns=['symbol', 
                                            'slug', 
                                            'cmcRank', 
                                            'quote.USD.marketCap', 
                                            'quote.USD.volume24h', 
                                            'quote.USD.percentChange1h', 
                                            'quote.USD.percentChange24h', 
                                            'quote.USD.percentChange7d'])

    for i in range(1, num_pages + 1):
        if i != 1:
            url = 'https://coinmarketcap.com/?page=' + str(i)

        # Fetch a web page
        cmc = requests.get(url)
        soup = BeautifulSoup(cmc.content, 'html.parser')

        data = soup.find('script', id='__NEXT_DATA__', type='application/json')
        # remove script data 
        data = json.loads(data.contents[0])
        # get only cryptocurrencies data
        coin_columns = data['props']['initialState']['cryptocurrency']['listingLatest']['data'][0]['keysArr']
        coin_data = np.array(data['props']['initialState']['cryptocurrency']['listingLatest']['data'][1:])[:,:-1]
        df = pd.DataFrame(coin_data, columns=coin_columns)

        df = df[['symbol', 
                'slug', 
                'cmcRank', 
                'quote.USD.marketCap', 
                'quote.USD.volume24h', 
                'quote.USD.percentChange1h', 
                'quote.USD.percentChange24h',
                'quote.USD.percentChange7d']]

        df_coinmarketcap = df_coinmarketcap.append(df)

    return df_coinmarketcap
