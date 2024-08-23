# -*- coding: utf-8 -*-
"""
Linked prediction

# trial
1. 최대한 많은 Edge 생성
    [여성,나이키], [운동화,여성], [직방, 지문인식]
2. 엣지 기반 확률 구하기 => 유사한 상품일 가능성은 ?? , 태그(inner keyword)기반 검색에 사용 될 수 있을지 ...?

# available
1. 상품 내부 키워드 매핑 : 상품제목, 타이틀에 없을 경우 내부 키워드에 추가 -> 검색 개선
2. 태그, 카테고리성 질의 -> 연관상품 찾아줄 수 있어
3.

"""

import os

os.chdir('../../../')
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window
import re


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def get_edges(tkn_lst):
    set = []
    for nd in range(0, len(tkn_lst) - 1):
        for edg in range(nd + 1, len(tkn_lst) - 1):
            if tkn_lst[nd] != tkn_lst[edg]:
                set.append([tkn_lst[nd], tkn_lst[edg]])
    return set


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('Linked prediction Job') \
        .master('local[*]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '16g') \
        .config('spark.driver.cores', '8') \
        .config('spark.executor.memory', '16g') \
        .config('spark.submit.deployMode', 'client') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", 10000) \
        .config('spark.sql.shuffle.partitions', '300') \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()

    '''
        step1 . 토큰셋 생성 
    '''
    basic_tkn = (spark.read.parquet('data/parquet/tfidf/')
                 # .select(F.regexp_extract(F.col("prod_nm"), "\s+", " "))
                 .withColumn("tkn", F.regexp_replace(F.lower(F.col('prod_nm')), '\s+', ' '))
                 .withColumn("tkn_lst", F.split(F.col("tkn"), "\s+"))
                 .withColumn("edges", get_edges(F.col("tkn_lst")))
                 )
    # basic_tkn.select(F.col("prod_nm"), F.col("edges")).where(F.col("prod_nm").like("%겉싸개%")).show(60, False)
    # basic_tkn.where(F.col("token")=='나이키').where(F.col('tf-idf') > 15).show(30, False)

    '''
    Check list 
        1. 상품명 기준으로 edges 만듦  -> edges 기준으로 확률 구했을때 상품명의 연관섣은 얼마다 될까?
        2. 의미있는( 예시> 브랜드 -카테코리 쌍 ) edege 라면 의미 있을 수도 있지 않을까? -> edge filter는 어떻게 ....?
        3. 
        4.
        5. 
    
    
    
    '''

    # trial1. 공통 이웃수 보기
    # 예외 : 동일한 상품(hash 값) 다른 것
    #

    basic_tkn.select(F.col("prod_nm"),F.hash(F.col("prod_nm")), F.explode(F.col("edges"))).show(100,False)


    exit(0)
