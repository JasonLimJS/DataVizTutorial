from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
import os
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import getpass
import tempfile

os.chdir(r'C:\Users\jason\Documents\FRMViz\Web App\backend')
options= webdriver.ChromeOptions()
options.add_argument('headless')

driver= webdriver.Chrome('chromedriver.exe')
master_pandas_df= pd.DataFrame()
k= 1
max_counter= 6619

while k<max_counter:
    url= r'https://finviz.com/screener.ashx?v=111&f=geo_usa&r=' + str(k)
    driver.get(url)
    time.sleep(5)
    
    header_col= driver.find_elements_by_xpath('/html/body/table[3]/tbody/tr[4]/td/div/table/tbody/tr[4]/td/table/tbody/tr[1]/td')
    web_body= driver.find_elements_by_xpath('/html/body/table[3]/tbody/tr[4]/td/div/table/tbody/tr[4]/td/table/tbody/tr')
    
    # for i in range(len(header_col)):
    #     print(header_col[i].text)
        
    # for i in range(len(web_body)):
    #     print(web_body[i].text)
    
    for j in range(len(header_col)):
        globals()['var_' + str(j)]= list()
        
    for j in range(len(header_col)):
        for i in range(len(web_body[1:])):
            html_path= '/html/body/table[3]/tbody/tr[4]/td/div/table/tbody/tr[4]/td/table/tbody/tr[' + str(i+2) + ']/td[' + str(j+1) + ']'
            data= driver.find_element_by_xpath(html_path)
            globals()['var_'+ str(j)].append(data.text)
            
    pandas_df_dict= dict()
    
    for j in range(len(header_col)):
        data= globals() ['var_'+str(j)]
        pandas_df_dict[header_col[j].text]= data
        
    pandas_df= pd.DataFrame(pandas_df_dict)
    master_pandas_df= master_pandas_df.append(pandas_df)
    k= k + len(pandas_df)
    
driver.quit()
master_pandas_df.to_csv('extracted_NASDAQ_data.csv')
    
    
    
