# -*- coding: utf-8 -*-
"""
# title : 외래어(Loan Word) 추출
# desc :
    - 외래어란? 고유어가 아닌 외국에서 들여와 자국어처럼 사용하는 말
    - 예시 : 아디다스(adidas), 나이키(nike), 맥도널드(macdonald) ...
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('jy_kim') \
        .master('local[4]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '32g') \
        .config('spark.driver.cores', '8') \
        .config('spark.executor.memory', '16g') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", 1000000) \
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

    prod = (prod_1.union(prod_2)).distinct()
    prod_nm = prod.select(
                            F.lower(F.col('상품명')).alias('prod_nm'),
                            F.explode(
                                F.split(
                                    F.trim(
                                        F.regexp_replace(
                                            F.regexp_replace(F.lower(F.col('상품명')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' '
                                        )
                                    ), ' '
                                )
                            ).alias('prod_nm_token')
                        ).withColumn(
                            "only_kor",
                            ~F.col("prod_nm_token").rlike("[^ㄱ-힣]")
                        ).withColumn(
                            "only_eng",
                            ~F.col("prod_nm_token").rlike("[^a-z]")
                        ).alias('prod_nm')

    '''
        step1. 상품명 토크나이징 후 하위 dataframe 생성
        대상1     |   후보들
        kor      |   eng
    '''
    get_kor_tkn = prod_nm\
        .where(F.col('only_kor') == True)\
        .groupby(
            F.col('prod_nm_token'),
            F.col('only_kor')
        ).agg(
            F.collect_list(F.col('prod_nm')).alias('prod_nms')
        ).where(
            (F.size(F.col('prod_nms')) > 1)
        ).alias('get_kor_tkn')

    get_kor_tkn = get_kor_tkn.select(F.col('prod_nm_token'), F.col('only_kor'), F.explode(F.col('prod_nms')).alias('prod_nm')).alias('get_kor_tkn')
    c = get_kor_tkn.select(
                                F.col('prod_nm'),
                                F.col('prod_nm_token'),
                                F.explode(
                                            F.split(
                                                F.trim(
                                                    F.regexp_replace(
                                                        F.regexp_replace(F.lower(F.col('prod_nm')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' '
                                                    )
                                                ), ' '
                                            )
                                        ).alias('prod_nm_token_cndd'),
                                (~F.col("prod_nm_token_cndd").rlike("[^a-z]")).alias("only_eng_cndd"),
                                (~F.col("prod_nm_token_cndd").rlike("[^가-힣]")).alias("only_kor_cndd"),
                            ).where(F.length(F.col('prod_nm_token_cndd')) > 3)#.sample(0.5)

    # 후보군들이 영어인것만 매칭
    get_cndd_eng = c\
        .where(F.col('only_eng_cndd') == True)\
        .groupby(F.col('prod_nm_token'), F.col('prod_nm_token_cndd'))\
        .agg(F.count(F.col('prod_nm_token_cndd')).alias('cnt'))\
        .where(F.length(F.col('prod_nm_token')) < F.length(F.col('prod_nm_token_cndd')))\
        .where(F.length('prod_nm_token') > 1)\
        .alias('get_cndd_eng')

    # 카테고리성 키워드 제거
    get_cate = (
                    (prod.select(F.explode(F.split(F.col('대분류'), "/")))).\
                    union(prod.select(F.explode(F.split(F.col('중분류'), "/")))).\
                    union(prod.select(F.explode(F.split(F.col('소분류'), "/"))))
                ).distinct().alias('get_cate')

    except_cate = get_cndd_eng.join(get_cate, F.col('prod_nm_token') == F.col('col'), 'leftanti').where(F.col('prod_nm_token_cndd') == 'canon')
    # except_cate.show(1000, False)

    '''
        step2. 상품명 토크나이징 후 하위 dataframe 생성 (step1 과 반대 )
        대상1     |   후보들
        eng      |   kor
    '''
    get_eng_tkn = prod_nm \
        .where(F.col('only_eng') == True) \
        .groupby(
            F.col('prod_nm_token'),
            F.col('only_eng')
        ).agg(
            F.collect_list(F.col('prod_nm')).alias('prod_nms')
        ).where(
            (F.size(F.col('prod_nms')) > 1)
        ).select(
            F.col('prod_nm_token'),
            F.col('only_eng'),
            F.explode(F.col('prod_nms')).alias('prod_nm')
        ).alias('get_eng_tkn')

    ca = get_eng_tkn.select(
        F.col('prod_nm'),
        F.col('prod_nm_token'),
        F.explode(
            F.split(
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(F.lower(F.col('prod_nm')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' '
                    )
                ), ' '
            )
        ).alias('prod_nm_token_cndd'),
        (~F.col("prod_nm_token_cndd").rlike("[^a-z]")).alias("only_eng_cndd"),
        (~F.col("prod_nm_token_cndd").rlike("[^가-힣]")).alias("only_kor_cndd"),
    ).where(F.length(F.col('prod_nm_token_cndd')) > 3)  # .sample(0.5)

    # 후보군들이 한글인것만 매칭
    get_cndd_kor = ca \
        .where(F.col('only_kor_cndd') == True) \
        .groupby(F.col('prod_nm_token'), F.col('prod_nm_token_cndd')) \
        .agg(F.count(F.col('prod_nm_token_cndd')).alias('cnt')) \
        .where(F.length(F.col('prod_nm_token')) >= F.length(F.col('prod_nm_token_cndd'))) \
        .where(F.length('prod_nm_token') > 2) \
        .alias('get_cndd_kor')
    get_cndd_kor.orderBy(F.col('cnt').desc()).show(1000, False)
    # get_cndd_kor.where(F.col('prod_nm_token') == 'nike').orderBy(F.col('cnt').desc()).show(100, False)
    # get_cndd_kor.where(F.col('prod_nm_token_cndd') == '나이키').orderBy(F.col('cnt').desc()).show(100, False)

    # todo : JOIN step1 and step2



    '''
    step0. 브랜드 부터 찾기!!
        todo : 숫자로만 이루어진 토큰으로 ngram
        조건 : 공백 전 후로만 
        영어로만 된것 -> 브랜드 가능성
        숫자로만 된것 -> 모델 번호
        숫자+ 영어로 끝나는 -> 속성 
    '''

