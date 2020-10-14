import numpy as np
import pandas as pd
import os
import json
import urllib.request
import time
import re
import datetime

#Create a backup file
master_path= os.path.join(os.getcwd(),'Data','Equity Prices')
#print(master_path)

if os.path.isdir(master_path):
    pass
else:
    os.mkdir(master_path)

eq_price_data= os.path.join(master_path,'eq_master_price_all.csv')
eq_price_backup= os.path.join(master_path,'eq_master_price_all_backup.csv')
eq_hist_price_data= os.path.join(master_path,'eq_hist_master_price_all.csv')
eq_hist_price_backup= os.path.join(master_path,'eq_hist_master_price_all_backup.csv')

if os.path.isfile(eq_price_data):
    current_master_intraday= pd.read_csv(eq_price_data)
    current_master_intraday.to_csv(eq_price_backup,index=False)
else:
    pass

if os.path.isfile(eq_hist_price_data):
    current_master_historical= pd.read_csv(eq_hist_price_data)
    current_master_historical.to_csv(eq_hist_price_backup,index=False)
else:
    pass

today= str(pd.to_datetime('today').strftime('%Y_%m_%d'))

###################################################################################################################################
##########                                  1. Daily Equity Prices                                                  ###############
###################################################################################################################################

eq_listing= 'Finviz Filtered Counters.xlsx'
api_key= '2RN97HFFVE23ZP1S'
eq_ts_daily= pd.DataFrame()
eq_master_list= pd.read_excel(eq_listing)
eq_ticker_list= eq_master_list.Ticker.tolist()

for tic_iter in range(len(eq_ticker_list)):

    tic= eq_ticker_list[tic_iter]
    eq_api= 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=' + tic + '&outputsize=full&datatype=csv&apikey=' + api_key
    eq_df= pd.read_csv(eq_api)
    eq_df['ticker']= tic
    eq_df['timestamp']= pd.to_datetime(eq_df.timestamp,format="%Y-%m-%d %H:%M:%S")
    eq_df['year']= eq_df.timestamp.map(lambda x: x.year)
    eq_df['month']= eq_df.timestamp.map(lambda x: x.month)
    eq_df['day'] = eq_df.timestamp.map(lambda x: x.day)
    eq_df['hour'] = eq_df.timestamp.map(lambda x: x.hour)
    eq_df['min'] = eq_df.timestamp.map(lambda x: x.minute)
    eq_df['hour_min_id']= eq_df['hour']*60 + eq_df['min']
    eq_df.sort_values(by=['year','month','day','hour_min_id'],ascending=True,inplace=True)
    eq_df['gap']= eq_df.close - eq_df.open

    eq_df['close_lag']= eq_df.close.shift(1)
    eq_df['return']= np.log(eq_df.close/eq_df.close_lag)

    eq_ts_daily= eq_ts_daily.append(eq_df)

    if os.path.isfile(eq_hist_price_data):
        in_folder_eq_csv= pd.read_csv(eq_hist_price_data)
        in_folder_eq_csv.to_csv(eq_hist_price_backup,index=False)

    eq_ts_daily.to_csv(eq_hist_price_data,index=False)
    print(tic + ' completed!')

    time.sleep(15)

###################################################################################################################################
##########                                  2. Intraday Equity Prices                                               ###############
###################################################################################################################################
eq_ts_daily= pd.DataFrame()

for tic_iter in range(len(eq_ticker_list)):

    tic= eq_ticker_list[tic_iter]
    eq_api= 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=' + tic + '&interval=5min&outputsize=full&datatype=csv&apikey=' + api_key
    eq_df= pd.read_csv(eq_api)
    eq_df['ticker']= tic
    eq_df['timestamp']= pd.to_datetime(eq_df.timestamp,format="%Y-%m-%d %H:%M:%S")
    eq_df['year']= eq_df.timestamp.map(lambda x: x.year)
    eq_df['month']= eq_df.timestamp.map(lambda x: x.month)
    eq_df['day'] = eq_df.timestamp.map(lambda x: x.day)
    eq_df['hour'] = eq_df.timestamp.map(lambda x: x.hour)
    eq_df['min'] = eq_df.timestamp.map(lambda x: x.minute)
    eq_df['hour_min_id']= eq_df['hour']*60 + eq_df['min']
    eq_df.sort_values(by=['year','month','day','hour_min_id'],ascending=True,inplace=True)
    eq_df['gap']= eq_df.close - eq_df.open

    eq_df['close_lag']= eq_df.close.shift(1)
    eq_df['return']= np.log(eq_df.close/eq_df.close_lag)

    eq_ts_daily= eq_ts_daily.append(eq_df)

    if os.path.isfile(eq_price_data):
        in_folder_eq_csv= pd.read_csv(eq_price_data)
        in_folder_eq_csv.to_csv(eq_price_backup,index=False)

    eq_ts_daily.to_csv(eq_price_data,index=False)
    print(tic + ' completed!')

    time.sleep(15)










