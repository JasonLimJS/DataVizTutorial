import numpy as np
import pandas as pd
import os
import json
import urllib.request
import time
import re
import datetime
import pyodbc
import sys
server = 'SERVER'
database = 'DATABASE'
username = 'USERNAME'
password = 'PASSWORD'
driver= '{ODBC Driver 17 for SQL Server}'

os.chdir(r'C:\Users\jason\Documents\FRMViz\Web App\backend')
connect_stat= 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

def adjust_NA(var):
    if pd.isna(var):
        return 'NULL'
    else:
        return var
    
def return_sql_stat(dt,openPrice,highPrice,lowPrice,closePrice,adjClosePrice,volume,ticker,yearDt, monthDt, dayDt, hourDt, minDt, hour_min,gap,close_lag,returnPrice):
    
    sql_stat= """
    insert into equity_market_price_test_4
    (timestamp, openPrice, highPrice, lowPrice, closePrice, adjClosePrice, volume, ticker, yearDt, monthDt, dayDt, hourDt, minDt, hour_min, gap, close_lag, returnPrice)
    values(
    	'{dt}', 
    	{openPrice},
    	{highPrice},
    	{lowPrice},
    	{closePrice},
        {adjClosePrice},
    	{volume},
    	'{ticker}',
    	{yearDt},
    	{monthDt},
    	{dayDt},
    	{hourDt},
    	{minDt},
    	{hour_min},
    	{gap},
    	{close_lag},
    	{returnPrice}
    );
    """.format(dt= adjust_NA(dt), openPrice= adjust_NA(openPrice), highPrice= adjust_NA(highPrice), lowPrice= adjust_NA(lowPrice),
               closePrice= adjust_NA(closePrice), adjClosePrice= adjust_NA(adjClosePrice), volume= adjust_NA(volume), ticker= adjust_NA(ticker),
               yearDt= adjust_NA(yearDt), monthDt= adjust_NA(monthDt), dayDt= adjust_NA(dayDt),  hourDt= adjust_NA(hourDt), minDt= adjust_NA(minDt), hour_min= adjust_NA(hour_min),
               gap= adjust_NA(gap), close_lag= adjust_NA(close_lag), returnPrice= adjust_NA(returnPrice))
    
    return sql_stat



###################################################################################################################################
##########                                  1. Daily Equity Prices                                                  ###############
###################################################################################################################################

eq_listing= 'equity_market_price_extraction_index.xlsx'
api_key= 'API_KEY'
eq_ts_daily= pd.DataFrame()
eq_master_list= pd.read_excel(eq_listing,sheet_name='Iter_1')
eq_ticker_list= eq_master_list.ticker.tolist()
targetDt= '2020-10-20'

with open('price_extraction_1.txt','w') as f:
    for ticker in eq_ticker_list:
        tic= str(ticker)
        eq_api= 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=' + tic + '&outputsize=compact&datatype=csv&apikey=' + api_key
        eq_df_compact= pd.read_csv(eq_api)
        eq_df_compact.sort_values(by='timestamp',inplace=True,ascending=False)
        eq_df= eq_df_compact.iloc[0]
        timestamp= eq_df.timestamp
        if timestamp== targetDt:
            openPrice= eq_df.open 
            highPrice= eq_df.high 
            lowPrice= eq_df.low 
            closePrice= eq_df.close 
            adjClosePrice= eq_df.adjusted_close  
            volume= eq_df.volume 
            ticker= tic 
            yearDt= pd.to_datetime(timestamp).year
            monthDt= pd.to_datetime(timestamp).month 
            dayDt= pd.to_datetime(timestamp).day 
            hourDt= pd.to_datetime(timestamp).hour 
            minDt= pd.to_datetime(timestamp).minute 
            hour_min= hourDt*60 + minDt 
            gap= closePrice - openPrice 
            close_lag=  eq_df_compact.iloc[1].close 
            returnPrice= np.log(closePrice/close_lag)

            sql_stat= return_sql_stat(timestamp,openPrice, highPrice, lowPrice, closePrice, adjClosePrice,
                                      volume, ticker, yearDt, monthDt, dayDt, hourDt, minDt, hour_min, gap,
                                      close_lag, returnPrice)
                
            with pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_stat)
                    
           
            f.write(tic + ' processing... \n')
            f.write(tic + ' completed! \n')
            
            time.sleep(2)
            




