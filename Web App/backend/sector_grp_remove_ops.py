# -*- coding: utf-8 -*-
"""
Created on Tue Oct 27 00:06:48 2020

@author: jason
"""
#Sector Ticker_Sector total
import pandas as pd
import os
import pickle

os.chdir(r'C:\Users\jason\Documents\FRMViz\Web App\backend')
sector_grp= pd.read_csv('sector_grp.csv')
return_data= pd.read_csv('return_data.csv')
relevant_tic= return_data.columns.tolist()
del relevant_tic[relevant_tic.index('Date')]

def selRelevant(listStr):
    listStr_list= listStr.split(',')
    ticker_list= [x[:x.find('-')].strip() for x in listStr_list]
    listStr_intersected= set(ticker_list).intersection(set(relevant_tic))
    listStr_included= [x for x in listStr_list if x[:x.find('-')].strip() in listStr_intersected]
    listStr_included= sorted(list(listStr_included))
    listStr_out= ','.join(listStr_included)
    return listStr_out

sector_grp['Ticker']= sector_grp.Ticker.map(lambda x: selRelevant(x))
sector_grp['total']= sector_grp.Ticker.map(lambda x: len(x.split(',')))

master_list= []
for listing in sector_grp.Ticker.tolist():
    master_list.extend(listing.split(','))
master_list_unique= sorted(list(set(master_list))) 
with open('all_ticks.pkl','wb') as w:
    pickle.dump(master_list_unique,w)
    
sector_grp.to_csv('sector_grp.csv')
    
    
    

