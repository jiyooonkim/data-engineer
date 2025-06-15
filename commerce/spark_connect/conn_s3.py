# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os

from main import upload_file
now_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
service_name = 's3'
endpoint_url = 'https://kr.object.*'
region_name = ''
access_key = ''
secret_key = ''

local_path= '/'
dt= ""

parquet_local_path = '/'


if __name__ == "__main__":
    spark = (SparkSession.builder
                .master("local[8]")
                .config('spark.driver.memory', "16g")
                .config("spark.driver.cores", 8)
                .config('spark.executor.memory', "16g")
                .config("spark.network.timeout", 100000)
                .config("spark.hadoop.fs.s3a.access.key", access_key)
                .config("spark.hadoop.fs.s3a.secret.key", secret_key)
                .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.sql.debug.maxToStringFields", "100000")
                .config("spark.hadoop.fs.s3a.endpoint", endpoint_url)
                .config("com.amazonaws.services.s3.enableV4", "true")
                .config("spark.shuffle.encryption.enabled", False)
                .config('spark.driver.extraJavaOptions', "-Duser.timezone=UTC")
                .config('spark.sql.shuffle.partitions', '800')
                .config("spark.sql.sources.partitionColumnTypeInference.enabled", False)
                .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
                .appName('transform_Job')
                .getOrCreate()
            )
    sc = spark.sparkContext

    tg_df = spark.read.option("header", True).option("delimiter", '\t').csv(input_tg)\
        .select(
            F.col('s3_path'),
            F.col('file_name'),
            F.col('old_table_name'),
            F.col('new_table_name'),
        ).alias('tg_df')
