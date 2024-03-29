# -*- coding: utf-8 -*-
"""
    title : 모델명 추출
    desc : - 영+숫 타입 추출
    모델번호 != 상품번호 != 모델명
    only number : 모델번호
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os
os.chdir('../../../')


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
        .master('local[*]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '32G') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()

    # Step1. 상품명에서 영+숫 타입 토큰 확보
    prod_token = spark.read.csv('commerce/data/nvr_prod.csv')\
        .select(F.col('_c1'), 
            F.regexp_replace(
                F.regexp_replace(F.lower(F.col('_c1')), "[^A-Za-z0-9-]", ' '),  # 숫자, 영어(소문자)만 남김
                r"\s+", ' '  # 다중 공백 제거
            ).alias('prod_nm_cndd'),
            F.explode(
                F.split(
                    F.trim(
                        F.regexp_replace(
                            F.regexp_replace(F.lower(F.col('_c1')), "[^A-Za-z0-9-]", ' '),  # 숫자, 영어(소문자ㅊ)만 남김
                            r"\s+", ' '     # 다중 공백 제거
                        )
                    ), " "
                )
            ).alias('token')
    ).where(
        (F.length(F.col('token')) > 4)        # over 4 length
    ).distinct()\
    .alias('prod_token')

    attr = spark.read.parquet('data/parquet/measures_attribution').alias('attr')
    get_token_info = prod_token.join(
                                        attr,
                                        F.col('prod_token.token') == F.col('attr.shp_nm_token'),
                                        'leftanti'
                                    ).withColumn(
                                        'tps',
                                        get_spelling_type(F.col('token'))[0]
                                    ).withColumn(
                                        'word_tp',
                                        get_text_tpye_count(get_spelling_type(F.col('token'))[1])[0]
                                    ).withColumn(
                                        'num_cnt',
                                        get_text_tpye_count(get_spelling_type(F.col('token'))[1])[1]
                                    ).withColumn(
                                        'eng_cnt',
                                        get_text_tpye_count(get_spelling_type(F.col('token'))[1])[2]
                                    ).withColumn(
                                        'kor_cnt',
                                        get_text_tpye_count(get_spelling_type(F.col('token'))[1])[3]
                                    ).withColumn(
                                        'etc_cnt',
                                        get_text_tpye_count(get_spelling_type(F.col('token'))[1])[4]
                                    ).alias('get_token_info')

    model_nm_cndd_1 = get_token_info.where(
                                                (F.col('word_tp').like('eng %')) &
                                                (F.col('num_cnt') > 2) &
                                                (F.col('eng_cnt') > 2)
                                            ).select(
                                                F.col('_c1').alias('prod_nm'),
                                                F.col('token').alias('model_nm')
                                            ).alias('model_nm_cndd_1')
    model_nm_cndd_2 = get_token_info.where(
                                                ~(F.col('word_tp').like('% eng')) &
                                                ~(F.col('word_tp').like('num %')) &
                                                (F.col('num_cnt') > 2) &
                                                (F.col('eng_cnt') > 1) &
                                                (F.col('eng_cnt') > 1)
                                            ).select(
                                                F.col('_c1').alias('prod_nm'),
                                                F.col('token').alias('model_nm')
                                            ).alias('model_nm_cndd_2')    # todo : 첫글자, 마지막글자 -(dash) 인것 삭제
    model_nm = (model_nm_cndd_1.union(model_nm_cndd_2)).distinct()
    model_nm.write.format("parquet").mode("overwrite")\
        .save("data/output/model_name/")

    # 번외 : 속성 추출 : eng , num 개수로 판별 하기
    get_attr = get_token_info\
        .where(
            (F.col('word_tp').like('% eng')) &
            (F.col('word_tp').like('num %')) &
            (F.col('eng_cnt') < 3) &
            ~(F.col('word_tp').like('%etc%'))
        ).select(
            F.col('token')
        ).alias('get_attr')

    msr_cndd = get_attr\
        .groupby(F.regexp_replace((F.col('token')), "[^a-z]", '').alias('msr'))\
        .agg(F.count(F.col('token')).alias('cnt'))\
        .where(
            (F.col('cnt') > 4) &
            (F.length(F.col('msr')) == 2) &
            (F.col('msr') != 'vv') &
            (F.col('msr') != 'ti') &
            (F.col('msr') != 'va') &
            (F.col('msr') != 'ea') &
            (F.col('msr') != 'xm')
        ).alias('msr_cndd')
    # msr_cndd.distinct().orderBy(F.col('cnt').desc()).show(10000, False)
    msr_attr = msr_cndd.join(get_attr, F.col('token').contains(F.col('msr'))).select(F.col('get_attr.token')).distinct()
    msr_attr.coalesce(1).write.format("parquet").mode("overwrite").option("compression", "gzip")\
        .save("data/output/measures_attribution_ver2/")

    # get_attr = get_attr.sample(0.5)
    # get_attr.show(10, False)

    '''
        todo : 숫자로만 이루어진 토큰으로 ngram
        조건 : 공백 전 후로만 
        영어로만 된것 -> 브랜드 가능성
        숫자로만 된것 -> 모델 번호
        숫자+ 영어로 끝나는 -> 속성 
    '''
    exit(0)
