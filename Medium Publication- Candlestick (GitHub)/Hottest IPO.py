
'''
0. Import Libraries and Set Parameter

'''

import pandas as pd
import numpy as np 
import os 
import requests
from datetime import datetime q
import time

API_KEY= 'Your API Key From Alpha Vantage'
ALLOWANCE_PER_MIN= 75.00 #i.e. how many API requests are allowed per minute, https://www.alphavantage.co/premium/



'''
1. Import Ticker Listing

'''

ipo_df= pd.read_excel('2020 IPO List.xlsx') #Reference: https://stockanalysis.com/ipos/2020-list/
all_tickers= ipo_df.Symbol.tolist()

''' 
2. Create Function to Extract Adjusted Close using Aplha Vantage API

'''

def get_price_from(ticker,api):
    url= f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&apikey={api}'
    r= requests.get(url)
    try:
        data= r.json()
        all_dates= list(data['Time Series (Daily)'].keys())
        current_price= data['Time Series (Daily)'][all_dates[0]]['5. adjusted close']
        return float(current_price)
    except:
        return None

'''
3. Examine How Many Seconds Does One Request Take
'''

start_timer= datetime.now()
price= get_price_from(all_tickers[0],API_KEY)
end_timer= datetime.now()
time_passed= end_timer- start_timer # time taken for a request to complete

    #calculate sleep time required after each extraction so that the 
    #API request limit is not breached
    sleep_time= max(0,60.00/ALLOWANCE_PER_MIN - time_passed.seconds) 

'''
4. Extract Adjusted Close using Aplha Vantage API for All Tickers

'''
priceList= [] #placeholder to store prices

for t in all_tickers:
    price= get_price_from(t,API_KEY)
    priceList.append(price)
    time.sleep(sleep_time)
    print(f'{t} extracted...')
    
ipo_df['Latest Adj Close']= priceList 
ipo_df.dropna(inplace=True)

'''
5. Calculate Return on Investment from IPO Date and Select Top 50 Tickers with Highest Return since IPO

'''

ipo_df['Return']= (ipo_df['Latest Adj Close']- ipo_df['IPO Price'])/ipo_df['IPO Price']
ipo_df.sort_values(by='Return',ascending=False,inplace=True)
ipo_df_50= ipo_df.head(50)
ipo_df_50['IPO Date']= ipo_df_50['IPO Date'].map(lambda x: datetime.strptime(x,'%b %d, %Y'))
ipo_df_50['Ticker Display']= ipo_df_50.apply(lambda x: x['Symbol'] + ' - ' + x['Name'],axis=1)
ipo_df_50.sort_values(by='Ticker Display',ascending=True,inplace=True)
top_50_tickers= ipo_df_50.Symbol.tolist()

'''
6. Create Function to Get The Company Description for These 50 Companies 
   with Highest Return Recorded Since IPO

'''
def get_comp_desc(ticker,api):
    url= f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={api}'
    r= requests.get(url)
    try:
        data= r.json()
        desc= data['Description']
        return desc
    except:
        return 'Description not available'

'''
7. Extract Company Information

'''

descList= [] #placeholder to store company descriptions

for t in top_50_tickers:
    desc= get_comp_desc(t,API_KEY)
    descList.append(desc)
    time.sleep(sleep_time)
    print(f'{t} extracted...')
    
ipo_df_50['Description']= descList

'''
8. Export Data to JSON File

'''

ipo_df_50.set_index('Symbol',inplace=True)
ipo_df_50.to_json('2020 Top 50 IPO.json',orient='columns')


