# -*- coding: utf-8 -*-
'''
title : 모델명 추출
desc : - 영+숫 타입 추출
모델번호 != 상품번호 != 모델명
only number : 모델번호
'''

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_text_tpye_count(col):
    type_rlt = ""
    num_cnt = 0
    eng_cnt = 0
    kor_cnt = 0
    etc_cnt = 0
    # type_rlt = []
    for i in list(col):
        if i == 'num':  # 숫자
            type_rlt += 'num '
            num_cnt += 1
            # type_rlt.append('num')
        elif i == 'eng':  # 영어
            type_rlt += 'eng '
            eng_cnt += 1
            # type_rlt.append('eng')
        elif i == 'kor':  # 한글
            type_rlt += 'kor '
            kor_cnt += 1
            # type_rlt.append('kor')
        else:  # 그 외 기타
            type_rlt += 'etc '
            etc_cnt += 1
            # type_rlt.append('etc')
    return [type_rlt[:-1], int(num_cnt), int(eng_cnt), int(kor_cnt), int(etc_cnt)]


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def get_spelling_type(col):
    type_rlt = []
    for i in col:
        if i.isdigit():  # 숫자
            type_rlt.append('num')
        elif i.encode().isalpha():  # 영어
            type_rlt.append('eng')
        elif i.isalpha():  # 한글
            type_rlt.append('kor')
        else:  # 그 외 기타
            type_rlt.append('etc')

    return list(set(type_rlt)), list(type_rlt)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('mapping_shipping_prodnm_job') \
        .master('local[8]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '32G') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()

    # Step1. 상품명에서 영+숫 타입 토큰 확보
    prod_token = spark.read.csv('/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod.csv')\
        .select(
            F.regexp_replace(
                F.regexp_replace(F.lower(F.col('_c1')), "[^A-Za-z0-9]", ' '),  # 숫자, 영어(소문자)만 남김
                r"\s+", ' '  # 다중 공백 제거
            ).alias('prod_nm_cndd'),
        F.explode(
            F.split(
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(F.lower(F.col('_c1')), "[^A-Za-z0-9]", ' '),  # 숫자, 영어(소문자ㅊ)만 남김
                        r"\s+", ' '     # 다중 공백 제거
                    )
                ), " "
            )
        ).alias('token')
    ).where(
        (F.length(F.col('token')) > 4)        # over 4 length
    ).distinct().alias('prod_token')

    # prod_token.where(F.col('token').cast('string').isNotNull()).show(1000, False)

    attr = spark.read.parquet('/Users/jy_kim/Documents/private/nlp-engineer/data/parquet/measures_attribution').alias('attr')
    a = prod_token\
        .join(
            attr,
            F.col('prod_token.token') == F.col('attr.shp_nm_token'),
            'leftanti'
        ).withColumn(
            'tps',
            get_spelling_type(F.col('token'))[0]
        ).withColumn('num_cnt', get_text_tpye_count(get_spelling_type(F.col('token'))[1])[1])\
        .withColumn('eng_cnt', get_text_tpye_count(get_spelling_type(F.col('token'))[1])[2])\
        .withColumn('kor_cnt', get_text_tpye_count(get_spelling_type(F.col('token'))[1])[3])\
        .withColumn('etc_cnt', get_text_tpye_count(get_spelling_type(F.col('token'))[1])[4])


    '''
        todo : 숫자로만 이루어진 토큰으로 ngram
        조건 : 공백 전 후로만 
    '''
    # a.where(F.size(F.col('tps')) == 1).where(F.col('tps')[0] == 'num').orderBy(F.col('token')).show(10000, False)
    a.where(F.col('tps')[0] == 'num').orderBy(F.col('token')).show(10000, False)

    # # 도량형 속성 제거 : eng , num 개수로 판별 하기
    # vv = a.where(F.col('tps')[0] == 'num').where((F.col('num_cnt')>1)).where((F.col('eng_cnt')>1)).sample(0.5)
    # vv.show(100, False)
    # # attr.where(F.col('shp_nm_token').like('%p')).show()


    exit(0)
