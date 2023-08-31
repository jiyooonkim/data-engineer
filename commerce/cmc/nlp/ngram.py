# -*- coding: utf-8 -*-
"""
# title : Ngram
# doc : https://heytech.tistory.com/343
# desc :
    - 다음 단어를 예측할 때 문장 내 모든 단어를 고려하지 않고 특정 단어의 개수 N개만 고려
    - N 개의 연속적인 단어의 나열을 하나의 묶음(=token)으로 간주
    ex) "오늘 점심 추천 메뉴: 파스타, 피자" 경우,
        Unigram(N=1)	오늘, 점심, 추천, 메뉴, 파스타, 피자
        Bigram(N=2)	오늘 점심, 점심 추천, 추천 메뉴, 메뉴 파스타, 파스타 피자
        Trigram(N=3)	오늘 점심 추천, 점심 추천 메뉴, 추천 메뉴 파스타, 메뉴 파스타 피자
        4-gram(N=4)	오늘 점심 추천 메뉴, 점심 추천 메뉴 파스타, 추천 메뉴 파스타 피자

    - 한계점
        - 정확도 : N개 연속된 단어만 고려하기 떄문에 문장의 맥락이 안맞을 수 있음
        - 희소(sparsity) : N개의 단어를 연속적으로 갖는 문장자체가 드물다
        - 상충(trade-off) : N값의 크기(너무 크거나작거나), N=5를 권장, 희소문제 연관성
        -

"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def get_gram(token, n):
    lst = []
    for i in range(0, len(token)):
        if len(token[i:i+n]) == n:
            lst.append(token[i:i+n])
    return lst


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('Compound word Job') \
        .master('local[*]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '16g') \
        .config('spark.driver.cores', '8') \
        .config('spark.executor.memory', '16g') \
        .config('spark.submit.deployMode', 'client') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", 10000) \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()

    prod_1 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod.csv") \
        .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))

    prod_2 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod_2.csv") \
        .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))

    prod = (prod_1.union(prod_2))\
        .select(F.col('상품명').alias('prod_nm'), F.split(F.lower(F.col('prod_nm')), " ").alias("prod_nm_tkns"))\
        .withColumn("bi_gram", get_gram(F.col("prod_nm_tkns"), F.lit("2").cast(T.IntegerType())))\
        .withColumn("tri_gram", get_gram(F.col("prod_nm_tkns"), F.lit("3").cast(T.IntegerType())))\
        .withColumn("4_gram", get_gram(F.col("prod_nm_tkns"), F.lit("4").cast(T.IntegerType())))

    '''
        <Linked prediction단어 예측>
         - Cosideration 
            - N Size 
            - 불용어 제거
            - 구둣점, 특수문자 제거 
         - scalability
            - 오타교정 : 철자 단위 앞뒤로 어떤 문자들이 많이 왔는가 -> 정타 사전 구축 가능 할듯 , 가지고 있는 오타가 많이 없으니
    '''
    '''
        step1. 상품 유사도 구한걸로 linked precition 해보기 
        step2. 결과 별로라면 카테고리 기반으로 해보기 
        ex)
            [여성,나이키], [운동화,여성]      예측결과물:  [나이키,운동화] 
    '''
    a = prod.select(F.explode(F.col("bi_gram")))
    a.show(50, False)

    exit(0)
