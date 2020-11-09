from flask import Flask, jsonify, request, render_template
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import pickle
import os
import json
import math
from datetime import datetime
import urllib.parse

SECTOR_DF= pd.read_csv(os.path.join(os.path.realpath('.'),'backend','sector_grp.csv'))
return_df= pd.read_csv(os.path.join(os.path.realpath('.'),'backend','return_data.csv'),index_col='Date')

server = 'frmviz.database.windows.net'
database = 'FRMViz'
username = 'frmviz_admin'
password = 'WelcomeFRM1234$'
driver= '{ODBC Driver 17 for SQL Server}'

params = urllib.parse.quote_plus(f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username}PWD={password}')

app= Flask(__name__)
app.config["SECRET_KEY"]="secret_cannot_tell"
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
    return render_template('VaR.html')

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
    sel_ticker_filtered= [x[:x.find('-')] for x in sel_ticker]
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


