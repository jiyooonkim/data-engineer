# -*- coding: utf-8 -*-
"""
    # title : Synoym Word(https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-synonym-graph-tokenfilter.html)
    # doc : 동의어 추출
    # desc :
        - 동등한 동의어
             캘린더-달력-켈린더, 손전등-랜턴, 리클라이너-소파, 모노포드-삼각대, 남자-남성, 여자-여성, 빨강-붉은-Red, TV-television-티비-텔레비젼,
        - 명시적 동의어
            personal computer => pc
            sea biscuit, sea biscit => seabiscuit


"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os

os.chdir('../../../')



if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('Compound word Job') \
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


    prod = (spark.read
          .option("header", True)
          .csv("commerce/data/nvr_prod.csv")
          )
    '''
    세분류 - 소분류 - 상품명 토크나이징 
    소분류 - 중분류 - 상품명 토크나이징 
    불필요한 것들 보고 제거 
    
    '''
    (prod.select(F.col("대분류"), F.col("중분류"), F.col("소분류"), F.col("세분류")).show(1000, False))


    set1 = prod.select(F.explode(F.split(F.col('상품명'), " ")).alias("prod_tkn"), F.col("소분류"), F.col("세분류"))
    set2 = prod.select(F.explode(F.split(F.col('상품명'), " ")), F.col("중분류"), F.col("소분류"))
    set = set1.union(set2)
    # set.show()

    (set.where((F.col('세분류')!= "N") & (F.length(F.col('prod_tkn'))>1) & (F.col("prod_tkn") != F.col("세분류")) & (F.col("prod_tkn") != F.col("소분류")))
     .groupby(F.col('prod_tkn'), F.col('소분류'), F.col('세분류'))
     .count().orderBy(F.col("prod_tkn"))
     .show(1000))


    # todo : stop word 제거 , 윗쪽 상품명에서 특수기오 제거 





    exit(0)
