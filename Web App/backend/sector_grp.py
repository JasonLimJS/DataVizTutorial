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
master_data_all= pd.read_csv('extracted_NASDAQ_data.csv')

finance_exclusion_list= ['Exchange Traded Fund','Shell Companies','Closed-End Fund - Debt',
                         'Closed-End Fund - Equity','Closed-End Fund - Foreign']

master_data= master_data_all[~master_data_all.Industry.isin(finance_exclusion_list)]
remover_list= pickle.load(open('remover_list.p','rb'))
master_data= master_data[~master_data.Ticker.isin(remover_list)]

print('There are %d sectors' % len(master_data.Sector.drop_duplicates()))
print(master_data.Sector.drop_duplicates().tolist())

print('There are %d industries' % len(master_data.Industry.drop_duplicates())) 

master_data['Volume']= master_data.Volume.map(lambda x: x.replace(',',''))
master_data['Volume']= master_data['Volume'].astype(float)
master_data_sorted= master_data.sort_values(by='Volume',ascending=False).groupby('Sector').head(len(master_data))


filtered_data_1= master_data_sorted[['Ticker','Company','Sector']].copy()
filtered_data_1['Company']= filtered_data_1['Company'].map(lambda x: x.replace(',',''))
filtered_data_1['Ticker_Company']= filtered_data_1.Ticker + ' - ' + filtered_data_1.Company
filtered_data_1['Ticker_Sector']= filtered_data_1.groupby('Sector')['Ticker_Company'].transform(lambda x: ','.join(x))


filtered_data_2= filtered_data_1[['Sector','Ticker_Sector']].drop_duplicates()
filtered_data_2['total']= filtered_data_2.Ticker_Sector.map(lambda x: len(x.split(',')))
filtered_data_2.to_excel('sector_grp.xlsx',index=False)
filtered_data_2.to_csv('sector_grp.csv',index=False)
