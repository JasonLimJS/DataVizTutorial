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







