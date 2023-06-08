# -*- coding: utf-8 -*-
"""
# title : compound_word
# doc : 합성어 추출
# desc :

"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_token(crr_wd, cndd_wd):
    tokens = []
    if str(crr_wd).__contains__(cndd_wd):
        tokens.append(cndd_wd)
        tokens.append(crr_wd.strip(cndd_wd))
    return tokens


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('jy_kim') \
        .master('local[*]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '32g') \
        .config('spark.driver.cores', '8') \
        .config('spark.executor.memory', '16g') \
        .config('spark.submit.deployMode', 'client') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", 1000000) \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()

    compound_word_candidate = spark.read.parquet("hdfs://localhost:9000/compound_word_candidate")
    compound_word_candidate = spark.read.parquet("hdfs://localhost:9000/compound_word_candidate")\
        .select(F.col('prod_nm'), F.col('count_w').alias('prod_nm_cnt'), F.col('cate'))\
        .where(F.col('jaccard_sim') > 0.3)\
        .withColumn('compound_word', get_token(F.col('prod_nm'), F.col('cate')))      # count : 6805

    compound_word_candidate.where(F.size(F.col('compound_word')) > 1).show(100, False)
    # compound_word_candidate.select(F.count(F.col('prod_nm'))).show(100, False)








    exit(0)