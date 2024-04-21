# -*- coding: utf-8 -*-
"""
    # title : cosine similarity
    # doc : https://needjarvis.tistory.com/665
    # desc :
        - 단어의 빈도수 중요할 경우 사용 (자카드유사도와 반대)
        - text similarity 측정
        - ver1. 상품명 vs 상품명

    # insight :
        - 연관 상품 가능성
            ex) '2p 눈썹칼 눈썹정리 칼 접이식' 의 추천 상품명 '눈썹칼 눈썹정리 메이크업소품 눈썹정리기 접이식', '왁싱 접이식 눈썹칼 3종 set 왁싱디자인 눈썹정리 잔털정리', '화장소품 눈썹정리 눈썹칼 가위 아이브로우', '메이크업소품 2단 접이식 눈썹칼 2개입'
    # dev :
        - 현) 카테고리 미적용, 상품개수 4만개라서..  -> 카테고리 기준으로 묶는다면 유사한(연관성) 상승 할 것으로 예측
    # pro :
         - 단어의 가중치에 따른 분류 필요  -> 'cc 28 희귀한 판화' <-> '랑콤 cc 크림 w spf 50 5'  'cc' 로 묶임
         - 단어(word)자체 비교이기 때문에 동일한 단어가 아닐 경우 예측이 안 됨으로 자세한 분석이 될 수 없음 ->  유의어,동의어.. 같은 것은 비교가 안됨

"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window
import os
os.chdir('../../../')


@F.udf(returnType=T.ArrayType(T.IntegerType()))
def get_token_count(candidate, tkn):
    # 전체 토큰 후보(candidate)들 중 빈도 수 카운팅
    # candidate : tkns      tkn : tkns1
    tkns_cnt = []
    for i in range(0, len(candidate)):
        cnt = 0
        for j in range(0, len(tkn)):
            if candidate[i] == tkn[j]:
                cnt = cnt + 1
        tkns_cnt.append(cnt)
    return tkns_cnt


@F.udf(returnType=T.IntegerType())
def get_square_root_candidate(candidate):
    # 리스트 제곱근 구하기
    return sum([k ** 2 for k in candidate])


@F.udf(returnType=T.DoubleType())
def get_demominator(cs_val1, cs_val2):
    # 분모(demominator) : 두 벡터의 곱
    return (cs_val1 * cs_val2) ** (1 / 2)


@F.udf(returnType=T.IntegerType())
def get_numerator(tkns_cnt1, tkns_cnt2):
    # 분자(numerator) : 두 벡터의 내적
    numerator = 0
    for i in range(0, len(tkns_cnt1)):  # 토큰 카운트 리스트 받아옴
        numerator = numerator + (tkns_cnt1[i] * tkns_cnt2[i])
    return numerator


@F.udf(returnType=T.DoubleType())
def get_cosine_similarity(numerator, demom):
    return numerator / demom

    
if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName('cosine similarity') \
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
    print(os.getcwd())

    prod = spark.read.parquet('./data/parquet/prod2/') \
        .select(
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.lower(F.col('prod_nm')), "[^A-Za-z0-9가-힣]", ' '),    # 다중 공백 제거
                    r"\s+", ' '
                )
            ).alias('prod_nm')
        )\
        .withColumn("prod_nm_tkns", F.split(F.regexp_replace(F.lower(F.col('prod_nm')), ' ', ','), ",")).distinct()\
        .alias('prod')\
        .repartition(600)    # cnt : 46271


    # Cosine similarity
    # ver1. 상품명 vs 상품명
    # todo : 공백 제거
    get_token = prod.withColumn('tkns', F.explode(F.col('prod_nm_tkns')))
    get_token_cnt = get_token\
        .groupby(F.col('prod_nm'), F.col('tkns'))\
        .agg(F.count(F.col('tkns')).alias('cnt'))\
        .where(F.length(F.col('tkns')) >= 2)        # 토큰길이 1 인것 제거

    get_token_cnt = get_token_cnt \
        .groupby(F.col('prod_nm')) \
        .agg(
            F.collect_list(F.col('tkns')).alias('tkns_list')
        )
    get_token_cnt.show()

    self_prod = get_token_cnt\
        .where(F.length(F.col('prod_nm')) < 28).alias('df1')\
        .join(
            get_token_cnt.alias('df2'),
            F.col('df1.prod_nm') != F.col('df2.prod_nm'),
            # 'full'
        ).select(
            F.col('df1.prod_nm').alias('prod_nm_1'),
            F.col('df1.tkns_list').alias('tkns_list_1'),
            F.col('df2.prod_nm').alias('prod_nm_2'),
            F.col('df2.tkns_list').alias('tkns_list_2'),
        ).where(
            F.size(F.array_intersect("tkns_list_1","tkns_list_2")) >= 1
            # 중복 토큰 일정 이상만 대상, 중복 토큰 없을 경우 cosine similariy '0' 일 수도..
        ).withColumn(
            'tkns',
            F.array_distinct(F.concat(F.col('tkns_list_1'), F.col('tkns_list_2')))
        )   # 토큰 리스트 합치기

    get_vector = self_prod\
        .withColumn('tkns_list_cnt1', get_token_count(F.col('tkns'), F.col('tkns_list_1')))\
        .withColumn('tkns_list_cnt2', get_token_count(F.col('tkns'), F.col('tkns_list_2')))\
        .withColumn('square_root1', get_square_root_candidate(F.col('tkns_list_cnt1')))\
        .withColumn('square_root2', get_square_root_candidate(F.col('tkns_list_cnt2')))\
        .withColumn('demominator', get_demominator(F.col('square_root1'), F.col('square_root2')))\
        .withColumn('numerator', get_numerator(F.col('tkns_list_cnt1'), F.col('tkns_list_cnt2')))\
        .withColumn('cosine_similarity', get_cosine_similarity(F.col('demominator'), F.col('numerator')))\
        .withColumn(
            'rnk',
            F.rank().over(window.Window.partitionBy(F.col('prod_nm_1')).orderBy(F.col('cosine_similarity')))
        ).where(F.col('rnk') < 5)   # 가장 유사한 상위 5개만
    # get_vector.orderBy(F.col('prod_nm_1').desc(), F.col('cosine_similarity').desc()).show(100, False)
    get_vector.show(100, False)
    # get_vector.write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/cosine_similarity")

    exit(0)