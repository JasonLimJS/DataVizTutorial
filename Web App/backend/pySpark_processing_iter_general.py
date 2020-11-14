# -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 23:09:24 2020

@author: jason
"""

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import numpy as np

jars = r'C:\Users\jason\Downloads\sqljdbc_8.4\enu\mssql-jdbc-8.4.1.jre8.jar'

conf = SparkConf() \
    .setAppName('FRMViz DB 1') \
    .setMaster('local[*]') \
    .set("spark.driver.extraClassPath", jars)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
spark = sqlContext.sparkSession

jdbcDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://{hostname}:1433;databaseName={database}") \
    .option("dbtable", "equity_market_price_test_4") \
    .option("user", username) \
    .option("password", password) \
    .load()

jdbcDF.printSchema()
jdbcDF = jdbcDF.repartition('ticker')
jdbcDF.rdd.getNumPartitions()
jdbcDF = jdbcDF.orderBy(jdbcDF.ticker.asc(), jdbcDF.timestamp.desc())
jdbcDF.cache().show(10)

lagging_window = Window.partitionBy('ticker').orderBy(F.col('timestamp'))
jdbcDF = jdbcDF.withColumn('adjClose_lag', F.lag(jdbcDF.adjClosePrice, 1).over(lagging_window))
jdbcDF.select('ticker', 'timestamp', 'adjClosePrice', 'adjClose_lag').show(3)
jdbcDF.select('ticker').distinct().show(10)
jdbcDF.filter(jdbcDF.ticker == 'SABR').select('ticker', 'timestamp', 'adjClosePrice', 'adjClose_lag').show(10)

jdbcDF = jdbcDF.withColumn('adjReturnPrice', F.log(jdbcDF.adjClosePrice / jdbcDF.adjClose_lag))
jdbcDF.select('ticker', 'timestamp', 'adjClosePrice', 'adjClose_lag', 'adjReturnPrice').show(10)
jdbcDF.select('ticker', 'timestamp', 'adjClosePrice', 'adjClose_lag', 'adjReturnPrice').filter(
    F.col('ticker') == 'SABR').show(10)

jdbcDF.write.jdbc(f'jdbc:sqlserver://{hostname}:1433;databaseName={database}',
                  table='equity_market_price_test_4_processed',
                  properties={'user': username, 'password': password},
                  mode="overwrite")

spark.stop()




