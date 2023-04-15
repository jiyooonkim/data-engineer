# -*- coding: utf-8 -*-
############################
# title : 상품명에 적합한 inner keyword 후보 추출 위해
# desc : - inner keyword : 검색에 사용하는 필터링 키워드
# pro : - 속성, 불용어(stopword) 제거
# - 상품명에서 inner keyword 제거 후 남은 것이 Stopword 가 될 가능성은 ??
# 연관키워드, 연관상품, 추천 상품 ,추천키워드 (해쉬태그....??)

'''
word2vec
- word to vector
- 자주 같이 등장할수록 두 단어는 비슷한 의미를 가진다는 것
- cbow : 맥락(주변) 중간단어 예측
- skip-gram : 중간단어로 맥락(주변) 예측
-
format
상품명      innerkwd

'''
############################
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2Vec
import pyspark.sql.functions as F


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('jy_kim') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '16G')\
        .config("spark.dynamicAllocation.enabled", True)\
        .config("spark.shuffle.service.enabled", True)\
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .config('spark.executor.extraJavaOptions', 'Ddev.ludovic.netlib.blas.nativeLib=libopenblas.so') \
        .getOrCreate()

    # skip-gram
    df = spark.read.parquet('hdfs://localhost:9000/test/prod2')\
        .select(F.regexp_replace(F.lower(F.col('prod_nm')), '  ', ' ').alias('prod_nm'))\
        .withColumn("prod_nm_tkns", F.split(F.regexp_replace(F.lower(F.col('prod_nm')), ' ', ','), ",")).repartition(500,) # .limit(500)
    # df.show(10, False)
    # df.select(F.count(F.col('prod_nm_tkns'))).show()

    word2Vec = Word2Vec(vectorSize=4, seed=3, inputCol="prod_nm_tkns", outputCol="model")
    word2Vec.setMaxIter(10)
    model = word2Vec.fit(df)
    model.getVectors().show(100, False)

    # todo: 네거티브 샘플링



    # # /usr/local/Cellar/hadoop/3.3.4/libexec/bin/hdfs

    #
    # attr = spark.read.parquet("hdfs://localhost:9000/dictionary/measures_attribution/")   # 속성 df
    # attr.show(10, False)





    exit(0)