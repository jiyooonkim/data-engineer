# -*- coding: utf-8 -*-
"""
# title :  TF-IDF
# desc : 문서 전체에 단어 중요도 구하고자
# doc : - https://yeong-jin-data-blog.tistory.com/entry/TF-IDF-Term-Frequency-Inverse-Document-Frequency
# pro : -
tf vs tf-idf
tf : 일반적인 텍스트 유사도 계산시
tf-idf : 검색쿼리 적절성 판단 시
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
os.chdir('../../../')

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('jy_kim') \
        .master('local[4]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '32G') \
        .config('spark.executor.memory', '32G') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.shuffle.service.enabled', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .config('spark.sql.adaptive.enabled', True) \
        .getOrCreate()

    df1 = spark.read. \
        option('header', True). \
        csv('commerce/data/nvr_prod.csv')\
        .select(F.col("상품명")).distinct()
    df2 = spark.read. \
        option('header', True). \
        csv("commerce/data/nvr_prod_2.csv")\
        .select(F.col("상품명")).distinct()
    shipping_df = spark.read.csv("commerce/data/송장명.csv")\
        .select(F.col("_c2").alias("상품명")).distinct()
    total_df = (
                    df1
                    .unionByName(df2, allowMissingColumns=True)
                    .unionByName(shipping_df, allowMissingColumns=True)
                ).distinct().alias("total_df")
    ori_df = total_df.\
        select(
            F.regexp_replace(F.col('상품명'), "[^a-zA-Zㄱ-힝0-9]", ' ').alias("prod_nm"),
        ).distinct()\
        .withColumn(
            "prod_nm_token",
            F.explode(F.split(F.regexp_replace(F.lower(F.col('prod_nm')), ' ', ','), ","))  # 영,한,숫 이외 제거 및 토큰화 확장  1:1
        ).where(
            F.col('prod_nm_token') != ""
        ).where(
            F.length(F.col('prod_nm_token')) > 1
        ).repartition(500, F.col('prod_nm_token'))\
        .alias('ori_df')

    # 토큰 리스트 생성 및 토큰 매핑
    get_collect_list = ori_df\
        .groupBy(F.col('prod_nm'))\
        .agg(F.collect_list("prod_nm_token").alias('tokens'))\
        .withColumn('token', F.explode(F.col('tokens')))\
        .alias('get_collect_list')

    ''' tf(d,t) : 특정 문서 d에서의 특정 단어 t의 등장 횟수 '''
    tf_df = get_collect_list \
        .groupBy(F.col('prod_nm'), F.col('token')) \
        .agg(F.count(F.col('token')).alias('tf')) \
        .alias('tf_t').repartition(500, F.col('token')).alias("tf_df")  # (문서 d 에서 단어 t 의 출현 빈도)
    # tf_df.where(F.col('prod_nm') == 'MAD DOG 매드독 차량용 공기청정기 MAD 350').show(100, False)

    ''' df(t) : 특정 단어 t가 등장한 문서의 수 '''
    df_df = ori_df.groupBy(F.col("prod_nm_token")).agg(F.count(F.col("prod_nm")).alias("df_cnt")).alias("df_df")
    # ori_df.where(F.col('prod_nm') == 'MAD DOG 매드독 차량용 공기청정기 MAD 350').show(100, False)
    # df_df.orderBy(F.col("df_cnt").desc()).show(100, False)

    D = ori_df.count()  # 총 문서의 개수
    idf = df_df.withColumn("idf", (F.log(D + 1 / F.col("df_cnt") + 1)) + 1).alias("idf")
    # idf.where(F.col("prod_nm_token") == "mad").orderBy(F.col("prod_nm_token").desc()).show(100, False)

    tf_idf = tf_df\
        .join(idf, F.col("tf_df.token") == F.col("idf.prod_nm_token"), "left")\
        .select(tf_df["*"], F.col("idf.idf"))\
        .withColumn("tf-idf", F.col("tf_df.tf") * F.col("idf.idf"))\
        .alias("tf_idf")
    # tf_idf.groupBy(F.col('token')).agg(F.count(F.col('prod_nm')).alias("cnt")).orderBy(F.col("cnt").desc()).show(1000, False)
    tf_idf.where(F.col("token").like("nike")).distinct().orderBy(F.col("tf-idf").desc()).show(1000, False)
    # tf_idf.orderBy(F.col("tf-idf").desc()).show(1000, False)
    tf_idf.write.format("parquet").mode("overwrite").save("data/parquet/tfidf/")
    #  코닥, 모닝, 화이트, 헬시



    exit(0)