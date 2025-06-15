# -*- coding: utf-8 -*-
"""
    Connect pySpark to MySQL

    [Desc]
    1. Download mysql jdbc .jar file
        - select "Platform Independent"
        - Download "Platform Independent (Architecture Independent), ZIP Archive"
    2. spark.driver.extraClassPath
        - Write .jar file location
"""
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import os


url= "jdbc:mysql://127.0.0.1:3306/hive"
driver = "com.mysql.cj.jdbc.Driver"
table_name = "PARTITIONS"
user = 'root'
passwd = 'root'

if __name__ == "__main__":
    
    spark = SparkSession.builder \
        .config("spark.driver.extraClassPath", "../../jar/mysql-connector-j-9.3.0.jar") \
        .appName('Conenction pypark and Mysql') \
        .getOrCreate()

    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", passwd) \
        .load()
    df.show()

    # properties = {
    #     "user": "hive",
    #     "passwd": "hive",
    #     "driver": "com.mysql.jdbc.Driver"
    # }