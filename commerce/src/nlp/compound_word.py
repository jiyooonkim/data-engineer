# -*- coding: utf-8 -*-
"""
    # title : compound_word
    # doc : 합성어 추출
    # desc : - 합성어 예시 : 클렌징 + 폼 = 클렌징폼, 메주 + 가루 = 메주가루, 휴대용 + 빨래판 = 휴대용빨래판, 수영 + 안경 = 수영안경
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window
import os
os.chdir('../../../')


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_token_ver1(crr_wd, cndd_wd):
    """
        선글라스케이스 |케이스 |[케이스, 선글라] |
    """
    tokens = []
    if cndd_wd in crr_wd:
        if len(crr_wd.strip(cndd_wd)) > 1:
            tokens.append(cndd_wd)
            tokens.append(crr_wd.replace(cndd_wd, ''))
    if crr_wd in cndd_wd:
        if len(cndd_wd.strip(crr_wd)) > 1:
            tokens.append(crr_wd)
            tokens.append(cndd_wd.replace(crr_wd, ''))
    return tokens


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_token_ver2(crr_wd, cndd_wd):
    """
    문제 : 선글라스케이스 |케이스 |[케이스, 선글라] |
    """
    tokens = []
    if crr_wd.__contains__(cndd_wd):
        if len(crr_wd.strip(cndd_wd)) > 1:
            tokens.append(cndd_wd)
            if len(crr_wd.split(cndd_wd)[0]) > 1 or len(crr_wd.split(cndd_wd)[1]) > 1:
                tokens.extend(crr_wd.split(cndd_wd))
            # else:
            #     tokens.append(crr_wd)

    if cndd_wd.__contains__(crr_wd):
        if len(cndd_wd.strip(crr_wd)) > 1:
            tokens.append(crr_wd)
            if len(cndd_wd.split(crr_wd)[0]) > 1 or len(cndd_wd.split(crr_wd)[1]) > 1:
                tokens.extend(cndd_wd.split(crr_wd))
            # else:
            #     tokens.append(crr_wd)
    return list(filter(None, tokens))


def remove_txt(word, tkns):
    for j in tkns:
        word = word.replace(j, ' ')
    return len(word.replace(" ", ""))


@F.udf(returnType=T.StringType())
def get_log_txt(word1, word2):
    if len(word1) < len(word2):
        word = word2
    else:
        word = word1
    return word


@F.udf(returnType=T.BooleanType())
def check_token_correction(tkns1, tkns2, word):
    if (remove_txt(word, tkns1) == 0) & (remove_txt(word, tkns2) == 0):
        return True
    else:
        return False


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

    # setp1 . 자카드 유사도 이용해 토근 후보들 추출 - 단어의 유사성 이용하여 seed 생성
    compound_word_candidate = spark.read.parquet("data/parquet/compound_word_candidate") \
        .select(
            F.regexp_replace(F.lower(F.col('prod_nm')), ' ', '').alias('prod_nm'),
            F.col('count_w').alias('prod_nm_cnt'),
            F.regexp_replace(F.lower(F.col('cate')), ' ', '').alias('cate'),
        ).where(F.col('jaccard_sim') > 0.4) \
        .withColumn('compound_word_v2', get_token_ver2(F.col('prod_nm'), F.col('cate'))) \
        .withColumn('compound_word_v1', get_token_ver1(F.col('prod_nm'), F.col('cate'))) \
        .withColumn('target_word', get_log_txt(F.col('prod_nm'), F.col('cate'))) \
        .withColumn('check_correction', check_token_correction(F.col('compound_word_v2'), F.col('compound_word_v1'), F.col('target_word')))

    # step2. 자카드 유사도 확률, 2가지 방식 토크나이징 사용하여 조건
    condition = compound_word_candidate \
        .where(F.size(F.col('compound_word_v1')) > 1) \
        .where(F.length(F.col('cate')) > 1) \
        .select(F.col('target_word'), F.col('compound_word_v2'), F.col('compound_word_v1'), F.col('check_correction')) \
        .where(F.col('check_correction') == True)
    # condition.where(F.col('target_word')=='아이스스케이트').orderBy(F.col('target_word')).show(1000, False)

    # 토큰들의 빈도수로 필터링 : "아이스스케이트 = 아이스스 +케이트" 가 True -> 아이스스 토큰은 없을 것이라 가정
    a = condition.select(
                            F.col('target_word'),
                            F.col('compound_word_v2'),
                            F.explode(F.col('compound_word_v2')).alias('tkns')
                        )
    b = spark.read.parquet("data/parquet/compound_word_candidate") \
        .select(
            F.col('prod_nm'),
            F.col('count_w')
        ).distinct()
    c = a.join(b, F.col('tkns') == F.col('prod_nm'), 'left') \
        .select(
            F.col('target_word'),
            F.col('compound_word_v2'),
            F.col('tkns'),
            F.coalesce(F.col('count_w'), F.lit(0)).alias('freq')
        )
    d = c.groupby(F.col('target_word'), F.col('compound_word_v2')) \
        .agg(F.sum(F.col('freq')).alias('freq')) \
        .distinct() \
        .withColumn(
            "rnk",
            F.rank().over(window.Window.partitionBy(F.col('target_word')).orderBy(F.col('freq').desc()))
        ).where(
            F.col('rnk') == 1
        ).dropDuplicates(['target_word'])
    d.write.format("parquet").mode("overwrite").save("data/parquet/compound/")
    d.show(1000, False)

    '''
    todo : 
        - python 에 hash ??? 값 ? 장/단점은 ?
        - 3개이상 토큰 추출해보기
            ex> 현) 건강용품도매 = 건강용품 + 도매      후) 건강+용품+도매  로 작업 해볼 것 
        - 서브워드 토크나이저 기법 사용 
    '''
    exit(0)
