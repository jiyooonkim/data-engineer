# -*- coding: utf-8 -*-
############################
# title : 상품명에 적합한 inner keyword 후보 추출 위해
# desc :
# pro : - 속성 및 불용어로 제거 할 수 있음
#
############################
import os
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('jy_kim') \
        .master('local[*]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '32G') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()
    # /usr/local/Cellar/hadoop/3.3.4/libexec/bin/hdfs
    df = spark.read.parquet('hdfs://localhost:9000/test/prod/*')
    df.show()

    attr = spark.read.parquet("hdfs://localhost:9000/dictionary/measures_attribution/")   # 속성 df





    exit(0)