# -*- coding: utf-8 -*-
########################################################################
# 목적 : cosine similarity
# 이유 :
########################################################################

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName('jy_kim') \
            .master('local[*]') \
            .config('spark.sql.execution.arrow.pyspark.enabled', True) \
            .config('spark.sql.session.timeZone', 'UTC') \
            .config('spark.driver.memory', '32g') \
            .config('spark.driver.cores', '8') \
            .config('spark.driver.maxResultSize', '0') \
            .config('spark.executor.memory', '16g') \
            .config('spark.submit.deployMode', 'client') \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.network.timeout",10000000) \
            .config('spark.ui.showConsoleProgress', True) \
            .config('spark.sql.repl.eagerEval.enabled', True) \
            .getOrCreate()
    tf_t = spark.read.parquet('hdfs://localhost:9000/test/tf_t/').alias('tf_t').repartition(650, F.col('prod_nm'))
    tf_d = spark.read.parquet('hdfs://localhost:9000/test/tf_d/').alias('tf_d')
    tf_t.show()
    tf_d.show()
    tf_t.printSchema()
    tf_d.printSchema()


    exit(0)
