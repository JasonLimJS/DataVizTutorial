import numpy as np
import pandas as pd
import os
import json
import urllib.request
import time
import re
import datetime
import pyodbc


os.chdir(r'C:\Users\jason\Documents\FRMViz\Web App\backend')
connect_stat = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password

def adjust_NA(var):
    if pd.isna(var):
        return 'NULL'
    else:
        return var


def return_sql_stat(dt, openPrice, highPrice, lowPrice, closePrice, adjClosePrice, volume, ticker, yearDt, monthDt,
                    dayDt, hourDt, minDt, hour_min, gap, close_lag, returnPrice):
    sql_stat = """
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
    """.format(dt=adjust_NA(dt), openPrice=adjust_NA(openPrice), highPrice=adjust_NA(highPrice),
               lowPrice=adjust_NA(lowPrice),
               closePrice=adjust_NA(closePrice), adjClosePrice=adjust_NA(adjClosePrice), volume=adjust_NA(volume),
               ticker=adjust_NA(ticker),
               yearDt=adjust_NA(yearDt), monthDt=adjust_NA(monthDt), dayDt=adjust_NA(dayDt), hourDt=adjust_NA(hourDt),
               minDt=adjust_NA(minDt), hour_min=adjust_NA(hour_min),
               gap=adjust_NA(gap), close_lag=adjust_NA(close_lag), returnPrice=adjust_NA(returnPrice))

    return sql_stat


# Create a backup file
master_path = os.path.join(os.getcwd(), 'Data', 'Equity Prices')
# print(master_path)

if os.path.isdir(master_path):
    pass
else:
    os.mkdir(master_path)

eq_price_data = os.path.join(master_path, 'eq_master_price_all.csv')
eq_price_backup = os.path.join(master_path, 'eq_master_price_all_backup.csv')
eq_hist_price_data = os.path.join(master_path, 'eq_hist_master_price_all.csv')
eq_hist_price_backup = os.path.join(master_path, 'eq_hist_master_price_all_backup.csv')

if os.path.isfile(eq_price_data):
    current_master_intraday = pd.read_csv(eq_price_data)
    current_master_intraday.to_csv(eq_price_backup, index=False)
else:
    pass

if os.path.isfile(eq_hist_price_data):
    current_master_historical = pd.read_csv(eq_hist_price_data)
    current_master_historical.to_csv(eq_hist_price_backup, index=False)
else:
    pass

today = str(pd.to_datetime('today').strftime('%Y_%m_%d'))

###################################################################################################################################
##########                                  1. Daily Equity Prices                                                  ###############
###################################################################################################################################

eq_listing = 'extracted_NASDAQ_data.xlsx'

eq_ts_daily = pd.DataFrame()
eq_master_list = pd.read_excel(eq_listing)
eq_ticker_list = eq_master_list.Ticker.tolist()

# for tic_iter in range(len(eq_ticker_list)):
# index: 0 to 59
start_index = 2755  # 330
end_index = 2760
while start_index < 6510:
    for tic_iter in range(start_index, end_index):

        tic = eq_ticker_list[tic_iter]
        eq_api = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=' + tic + '&outputsize=full&datatype=csv&apikey=' + api_key
        eq_df = pd.read_csv(eq_api)
        eq_df['ticker'] = tic
        eq_df['timestamp'] = pd.to_datetime(eq_df.timestamp, format="%Y-%m-%d %H:%M:%S")
        eq_df['year'] = eq_df.timestamp.map(lambda x: x.year)
        eq_df['month'] = eq_df.timestamp.map(lambda x: x.month)
        eq_df['day'] = eq_df.timestamp.map(lambda x: x.day)
        eq_df['hour'] = eq_df.timestamp.map(lambda x: x.hour)
        eq_df['min'] = eq_df.timestamp.map(lambda x: x.minute)
        eq_df['hour_min_id'] = eq_df['hour'] * 60 + eq_df['min']
        eq_df.sort_values(by=['year', 'month', 'day', 'hour_min_id'], ascending=True, inplace=True)
        eq_df['gap'] = eq_df.close - eq_df.open

        eq_df['close_lag'] = eq_df.close.shift(1)
        eq_df['return'] = np.log(eq_df.close / eq_df.close_lag)

        eq_ts_daily = eq_ts_daily.append(eq_df)

        if os.path.isfile(eq_hist_price_data):
            in_folder_eq_csv = pd.read_csv(eq_hist_price_data)
            in_folder_eq_csv.to_csv(eq_hist_price_backup, index=False)

        eq_ts_daily.to_csv(eq_hist_price_data, index=False)

        eq_df.drop(['dividend_amount', 'split_coefficient'], inplace=True, axis=1)
        eq_df_columns = eq_df.columns.tolist()

        print(tic + ' processing...')

        for index, row in eq_df.iterrows():
            sql_stat = return_sql_stat(row[eq_df_columns[0]].strftime('%Y-%m-%d'), row[eq_df_columns[1]],
                                       row[eq_df_columns[2]], row[eq_df_columns[3]], row[eq_df_columns[4]],
                                       row[eq_df_columns[5]], row[eq_df_columns[6]], row[eq_df_columns[7]],
                                       row[eq_df_columns[8]], row[eq_df_columns[9]], row[eq_df_columns[10]],
                                       row[eq_df_columns[11]], row[eq_df_columns[12]], row[eq_df_columns[13]],
                                       row[eq_df_columns[14]], row[eq_df_columns[15]],
                                       row[eq_df_columns[16]])

            with pyodbc.connect(
                    'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql_stat)

            print('ticker: ' + tic + ', row indexed ' + str(index) + ' imported!')

        print(tic + ' completed!')

    if start_index == 2755:
        start_index = 2730 + 150
    else:
        start_index += 150

    end_index += 150
