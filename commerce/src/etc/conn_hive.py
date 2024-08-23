# -*- coding: utf-8 -*-
import os

os.chdir('../../../')
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window
import re

if __name__ == "__main__": 
    spark = SparkSession.builder \
        .appName('Compound word Job') \
        .master('local[*]') \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '16g') \
        .config('spark.driver.cores', '8') \
        .config('spark.executor.memory', '16g') \
        .config('spark.submit.deployMode', 'client') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", 10000) \
        .config("spark.hive.resultset.use.unique.column.names", False) \
        .config("spark.sql.extensions", "org.apache.spark.sql.dialect.KyuubiSparkJdbcDialectExtension") \
        .getOrCreate()

    (spark.read.format("jdbc").option("url", url)
     .option("driver", "org.apache.hive.jdbc.HiveDriver")
     .option("user", user)
     .option("password", password)
     .option(
        "dbtable", "lz_tvi.dw_purchase_hist").load().show())
    jdbcDF = spark.read \
        .format("jdbc") \
        .options(
                 url="jdbc:hive2://*/default",
                 user=user,
                 password=password",
                 query="select count(*) from table ",
                 inferSchema=True
                 ) \
        .load()
    jdbcDF.show()
 