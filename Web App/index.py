from flask import Flask, jsonify, request, render_template
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import pickle
import os
import json
import math
from datetime import datetime
import urllib.parse
import numpy as np

SECTOR_DF= pd.read_csv(os.path.join(os.path.realpath('.'),'backend','sector_grp.csv'))
return_df= pd.read_csv(os.path.join(os.path.realpath('.'),'backend','return_data.csv'),index_col='Date')

ALL_TICKS_PATH= os.path.join(os.path.realpath('.'),'backend','all_ticks.pkl')
test_list= pickle.load(open(ALL_TICKS_PATH,'rb'))

params = urllib.parse.quote_plus(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')

app= Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"]= "mssql+pyodbc:///?odbc_connect=%s" % params
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"]=False

db= SQLAlchemy(app)

class userInteractivity(db.Model):
    id= db.Column(db.Integer,primary_key=True)
    dt= db.Column(db.DateTime,default=datetime.utcnow())
    ticker= db.Column(db.String(10),nullable=False)
    position= db.Column(db.String(10),nullable=False)
    unit= db.Column(db.Float)

    def __init__(self,dt,ticker,position,unit):
        self.dt= dt
        self.ticker= ticker
        self.position= position
        self.unit= unit

@app.route('/')
def index():
    return render_template('VaR.html',tickerList=test_list)

@app.route('/process',methods=['POST'])
def process():
    sector= request.form['filter']
    ticker_filtered= SECTOR_DF[SECTOR_DF.Sector==sector].Ticker.tolist()
    #print(ticker_filtered)
    return {'filteredList':ticker_filtered}

@app.route('/compute',methods=['POST'])
def compute():
    jsonObj= request.form['tickerDict']
    jsonObj_dict= json.loads(jsonObj)
    sel_ticker= jsonObj_dict['ticker']
    sel_ticker_filtered= [x[:x.find('-')].strip() for x in sel_ticker]
    #print(sel_ticker_filtered)
    sel_post= jsonObj_dict['position']
    sel_post_filtered= [1 if x=='Long' else -1 for x in sel_post]
    sel_units= jsonObj_dict['unit']
    sel_units= [float(x) for x in sel_units]
    sum_units= sum(sel_units)
    sel_units_filtered= [x/sum_units for x in sel_units]
    sel_df= pd.DataFrame({'ticker':sel_ticker_filtered,
                          'position':sel_post_filtered,
                          'units':sel_units_filtered})
    sum_sel_df= sel_df.groupby(['ticker','position']).sum()
    sum_sel_df.reset_index(inplace=True)
    sel_df= sum_sel_df
    sel_df['effective_position']= sel_df.position * sel_df.units
    #print(sel_df)

    _user= userInteractivity(datetime.utcnow(),sel_ticker_filtered[0],sel_post[0],sel_units[0])
    db.session.add(_user)
    db.session.commit()

    filtered_df= return_df[list(set(sel_ticker_filtered))][:]
    filtered_df.reset_index(inplace=True)
    pivot_df= filtered_df.melt(id_vars='Date',value_vars=list(set(sel_ticker_filtered)))
    pivot_df['Date']= pivot_df.Date.map(lambda x: datetime.strptime(x,'%d/%m/%Y'))
    pivot_df.sort_values(by=['Date','variable'],inplace=True)

    calc_df= sel_df.merge(pivot_df,left_on=['ticker'],right_on=['variable'])
    calc_df['return']= calc_df.value.map(lambda x: np.exp(x)-1)
    calc_df['effective_return']= calc_df.effective_position * calc_df['return']
    min_date_str= min(calc_df.Date.tolist()).strftime('%d %b %Y')
    max_date_str= max(calc_df.Date.tolist()).strftime('%d %b %Y')
    var_comp= pd.DataFrame(calc_df.groupby('Date')['effective_return'].sum())
    var_comp.reset_index(inplace=True)
    var_comp.sort_values(by='effective_return',ascending=True,inplace=True)
    var_comp_len= len(var_comp)
    return_List= var_comp.effective_return.tolist()
    var_pct= 0.05
    var_lower_index= math.floor(var_pct*var_comp_len)
    var_upper_index= var_lower_index + 1
    var_lower= var_comp['effective_return'].iloc[var_lower_index-1]
    var_upper = var_comp['effective_return'].iloc[var_upper_index - 1]
    var_final= var_lower + (var_pct*var_comp_len - var_lower_index)*(var_upper- var_lower)
    var_final_str= str(round(var_final*100,2)) + '%'
    #print(var_final_str)
    output_sel_df= sel_df[:]
    output_sel_df.drop(['position','units'],axis=1,inplace=True)
    output_sel_df.rename(columns={'ticker':'Ticker','effective_position':'Proportion'},inplace=True)
    output_sel_df['Unit']= sel_df.units.tolist()
    output_sel_df['Unit']= output_sel_df.Unit.map(lambda x: int(x*sum_units))
    output_sel_df['Position']= sel_df.position.tolist()
    output_sel_df['Position']= output_sel_df.Position.map(lambda x:'Long' if x==1 else 'Short')
    output_sel_df['Proportion_Desc']= output_sel_df['Proportion'].map(lambda x: str(round(x*100,2)) + '%')
    tickerList= output_sel_df.Ticker.tolist()
    #print(tickerList)
    propList= output_sel_df.Proportion_Desc.tolist()
    unitList= output_sel_df.Unit.tolist()
    positionList= output_sel_df.Position.tolist()

    var_comp_display= calc_df[['Date','variable','return']][:]
    #print(var_comp_display)
    var_comp_display_pivot= var_comp_display.pivot(index='Date',columns='variable',values=['return'])
    col_name= var_comp_display_pivot.columns.tolist()
    col_name_new= [x[1] + '_' + x[0] for x in col_name]
    var_comp_display_pivot.columns= col_name_new
    var_comp_display_pivot.reset_index(inplace=True)
    var_comp_display_pivot_2= var_comp_display_pivot.merge(var_comp,left_on='Date',right_on='Date',how='left')
    var_comp_display_pivot_2.rename(columns={'effective_return':'Portfolio Return'},inplace=True)
    var_comp_display_pivot_2.sort_values(by='Portfolio Return',inplace=True)
    var_comp_display_pivot_2.reset_index(inplace=True)
    var_comp_display_pivot_2.drop('index',inplace=True,axis=1)
    var_comp_display_pivot_2['Rank']= var_comp_display_pivot_2.index + 1
    output_list= list()
    for c in range(len(var_comp_display_pivot_2.columns)):
        output_list.append(var_comp_display_pivot_2.iloc[:,c].tolist())
    returnTitleList= var_comp_display_pivot_2.columns.tolist()
    returnTitleList= np.array(returnTitleList).reshape(-1,1).tolist()

    return {'tickerDictQuery':jsonObj,'returnQuery':return_List,'varTitle':var_final_str,
            'startDate':min_date_str, 'endDate':max_date_str, 'tickerList': tickerList,
            'propList': propList, 'unitList': unitList, 'positionList': positionList,
            'returnTitleList': returnTitleList, 'returnOutput': output_list}

if __name__=="__main__":
    db.create_all()
    app.run(debug=True)



