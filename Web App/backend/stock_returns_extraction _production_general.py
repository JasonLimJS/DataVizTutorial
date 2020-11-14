from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pandas as pd
import os
import pickle

jars= r'C:\Users\jason\Downloads\sqljdbc_8.4\enu\mssql-jdbc-8.4.1.jre8.jar'

conf= SparkConf()\
      .setAppName("Stock Returns Data")\
      .setMaster("local[*]")\
      .set("spark.driver.extraClassPath",jars)
sc= SparkContext(conf=conf)
sql= SQLContext(sc)
spark= sql.sparkSession

########################################################### Iteration: 1 #########################################
dbtable= 'equity_market_price_test_4_processed'
url= f'jdbc:sqlserver://{hostname};databaseName={dbname};'


spark_df= spark.read.format("jdbc")\
         .option('url',url)\
         .option('dbtable',dbtable)\
         .option('user',username)\
         .option('password',password)\
         .load()

os.chdir(r'C:\Users\jason\Documents\FRMViz\Web App\backend')
master_data_all= pd.read_csv('extracted_NASDAQ_data.csv')
finance_exclusion_list= ['Exchange Traded Fund','Shell Companies','Closed-End Fund - Debt',
                         'Closed-End Fund - Equity','Closed-End Fund - Foreign']
master_data= master_data_all[~master_data_all.Industry.isin(finance_exclusion_list)]
remover_list= pickle.load(open('remover_list.p','rb'))
master_ticker= list(set(master_data.Ticker.tolist())-set(remover_list))

filtered_spark_df= spark_df.filter(spark_df.ticker.isin(master_ticker))
filtered_spark_df= filtered_spark_df.select('timestamp','ticker','adjReturnPrice')
window_function= Window.partitionBy('ticker').orderBy(F.col('timestamp').desc())


filtered_spark_df= filtered_spark_df.withColumn('rowSeq',
                        F.row_number().over(window_function))
filtered_spark_df= filtered_spark_df.repartition('ticker')
filtered_spark_df.cache().show(10)

filtered_spark_df= filtered_spark_df.filter(F.col('rowSeq')<102)
filtered_pandas_df= filtered_spark_df.toPandas()

filtered_pandas_pivot= filtered_pandas_df.pivot_table(index='timestamp',columns='ticker',values='adjReturnPrice')
filtered_pandas_pivot.dropna(inplace=True)
combined_01= filtered_pandas_pivot.copy()

########################################################### Iteration: 2 #########################################
dbtable= 'equity_market_price_test_6_processed'
url= f'jdbc:sqlserver://{hostname};databaseName={dbname};'


spark_df= spark.read.format("jdbc")\
         .option('url',url)\
         .option('dbtable',dbtable)\
         .option('user',username)\
         .option('password',password)\
         .load()

filtered_spark_df= spark_df.filter(spark_df.ticker.isin(master_ticker))
filtered_spark_df= filtered_spark_df.select('timestamp','ticker','adjReturnPrice')
window_function= Window.partitionBy('ticker').orderBy(F.col('timestamp').desc())


filtered_spark_df= filtered_spark_df.withColumn('rowSeq',
                        F.row_number().over(window_function))
filtered_spark_df= filtered_spark_df.repartition('ticker')
filtered_spark_df.cache().show(10)

filtered_spark_df= filtered_spark_df.filter(F.col('rowSeq')<102)
filtered_pandas_df= filtered_spark_df.toPandas()

filtered_pandas_pivot= filtered_pandas_df.pivot_table(index='timestamp',columns='ticker',values='adjReturnPrice')
filtered_pandas_pivot.dropna(inplace=True)
combined_02= filtered_pandas_pivot.copy()

########################################################### Iteration: 3 #########################################
dbtable= 'equity_market_price_test_7_processed'
url= f'jdbc:sqlserver://{hostname};databaseName={dbname};'


spark_df= spark.read.format("jdbc")\
         .option('url',url)\
         .option('dbtable',dbtable)\
         .option('user',username)\
         .option('password',password)\
         .load()


filtered_spark_df= spark_df.filter(spark_df.ticker.isin(master_ticker))
filtered_spark_df= filtered_spark_df.select('timestamp','ticker','adjReturnPrice')
window_function= Window.partitionBy('ticker').orderBy(F.col('timestamp').desc())


filtered_spark_df= filtered_spark_df.withColumn('rowSeq',
                        F.row_number().over(window_function))
filtered_spark_df= filtered_spark_df.repartition('ticker')
filtered_spark_df.cache().show(10)

filtered_spark_df= filtered_spark_df.filter(F.col('rowSeq')<102)
filtered_pandas_df= filtered_spark_df.toPandas()

filtered_pandas_pivot= filtered_pandas_df.pivot_table(index='timestamp',columns='ticker',values='adjReturnPrice')
filtered_pandas_pivot.dropna(inplace=True)
combined_03= filtered_pandas_pivot.copy()

########################################################### Iteration: 4 #########################################
dbtable= 'equity_market_price_test_8_processed'
url= f'jdbc:sqlserver://{hostname};databaseName={dbname};'


spark_df= spark.read.format("jdbc")\
         .option('url',url)\
         .option('dbtable',dbtable)\
         .option('user',username)\
         .option('password',password)\
         .load()

filtered_spark_df= spark_df.filter(spark_df.ticker.isin(master_ticker))
filtered_spark_df= filtered_spark_df.select('timestamp','ticker','adjReturnPrice')
window_function= Window.partitionBy('ticker').orderBy(F.col('timestamp').desc())


filtered_spark_df= filtered_spark_df.withColumn('rowSeq',
                        F.row_number().over(window_function))
filtered_spark_df= filtered_spark_df.repartition('ticker')
filtered_spark_df.cache().show(10)

filtered_spark_df= filtered_spark_df.filter(F.col('rowSeq')<102)
filtered_pandas_df= filtered_spark_df.toPandas()

filtered_pandas_pivot= filtered_pandas_df.pivot_table(index='timestamp',columns='ticker',values='adjReturnPrice')
filtered_pandas_pivot.dropna(inplace=True)
combined_04= filtered_pandas_pivot.copy()

########################################################### Iteration: 5 #########################################
dbtable= 'equity_market_price_test_9_processed'
url= f'jdbc:sqlserver://{hostname};databaseName={dbname};'


spark_df= spark.read.format("jdbc")\
         .option('url',url)\
         .option('dbtable',dbtable)\
         .option('user',username)\
         .option('password',password)\
         .load()

filtered_spark_df= spark_df.filter(spark_df.ticker.isin(master_ticker))
filtered_spark_df= filtered_spark_df.select('timestamp','ticker','adjReturnPrice')
window_function= Window.partitionBy('ticker').orderBy(F.col('timestamp').desc())

filtered_spark_df= filtered_spark_df.withColumn('rowSeq',
                        F.row_number().over(window_function))
filtered_spark_df= filtered_spark_df.repartition('ticker')
filtered_spark_df.cache().show(10)

filtered_spark_df= filtered_spark_df.filter(F.col('rowSeq')<102)
filtered_pandas_df= filtered_spark_df.toPandas()

filtered_pandas_pivot= filtered_pandas_df.pivot_table(index='timestamp',columns='ticker',values='adjReturnPrice')
filtered_pandas_pivot.dropna(inplace=True)
combined_05= filtered_pandas_pivot.copy()

output_data_return= combined_01.merge(combined_02,left_index=True,right_index=True)
output_data_return= output_data_return.merge(combined_03,left_index=True,right_index=True)
output_data_return= output_data_return.merge(combined_04,left_index=True,right_index=True)
output_data_return= output_data_return.merge(combined_05,left_index=True,right_index=True)
output_data_return.reset_index(inplace=True)
output_data_return.sort_values(by='timestamp',ascending=False,inplace=True)
output_data_return.rename(columns={'timestamp':'Date'},inplace=True)
output_data_return['Date']=output_data_return.Date.map(lambda x: x.strftime('%d/%m/%Y'))
output_data_return.to_csv('return_data.csv',index=False)
