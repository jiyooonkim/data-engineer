# -*- coding: utf-8 -*-
########################################################################
# 목적 : 상품명에 대한 카테고리 매칭 정제
# 이유 : 같은 상품명에 여러 카테고리, 카테고리 오매칭, 카테고리 범주를 좁히고자(카테고리는 수동으로 생성가능한 영역이라 신뢰성 부족 -> 해결방안 없음..)
# 생산성 : 브랜드, 시리즈, 모델명, (도량형)속성 사전 구축
########################################################################

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window
# from konlpy.tag import Okt, Kkma
import os
os.chdir('../../../')


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_morpheme(txt):
    #  품사 구하기
    if len(txt) > 1:
        okt = Okt()  # 단어 개별 분석
        kkma = Kkma()  # 단어 중복 분석
        pos = okt.pos(str(txt), join=True)      # 품사 판별
        # tweet_okt = okt.nouns(str(txt))
        # tweet_kkma = kkma.nouns(txt)
        return pos


if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName('naver_product matching Job') \
            .master('local[*]') \
            .config('spark.sql.execution.arrow.pyspark.enabled', True) \
            .config('spark.sql.session.timeZone', 'UTC') \
            .config('spark.driver.memory', '32G') \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config('spark.ui.showConsoleProgress', True) \
            .config('spark.sql.repl.eagerEval.enabled', True) \
            .getOrCreate()
    print(os.getcwd())
    ori_df = spark.read.\
            option('header', True).\
            csv('commerce/data/nvr_prod.csv').\
            select(
                F.regexp_replace(F.col('상품명'), "[^a-zA-Zㄱ-힝0-9]", ' ').alias("prod_nm"),
                # F.col('cate_code'),
                F.col('대분류').alias('l_cate'),
                # F.col('중분류').alias('m_cate'),
                # F.col('소분류').alias('s_cate'),
                # F.col('세분류').alias('d_cate'),
            ).withColumn(
                "prod_nm_token",
                F.explode(F.split(F.regexp_replace(F.lower(F.col('prod_nm')), ' ', ','), ","))      # 영,한,숫 이외 제거 및 토큰화 작업
            ).where(
                F.col('prod_nm_token') != ""
            ).repartition(500, F.col('prod_nm_token'))\
        .alias('ori_df')

    # ori_df = ori_df.withColumn(
    #                                 "morpheme",
    #                                  get_morpheme(('prod_nm_token'))  # 품사구하기
    #                             )

    # ori_df.where(F.col('prod_nm').like('%DIY 만들기%목걸이%')).show(100, False)
    # ori_df.orderBy(F.col('prod_nm')).show(400, False)

    get_tkn_prct = ori_df.groupBy(
                        F.regexp_replace(F.col('prod_nm_token'), "[^ㄱ-힝]", '').alias("token"),
                        # F.col('prod_nm_token').alias('token'),
                        F.col('l_cate').alias('tkn_l_cate')
                    ).agg(
                        F.count(F.col('prod_nm_token')).alias('cnt')
                    ).where(
                        F.length(F.col('token')) > 1
                    ).alias('get_tkn_prct')     # 1글자 의미 없음
    # get_tkn_prct.where(F.col('token') == '만들기').show()

    get_prod_tkn = ori_df.join(
        get_tkn_prct,
        [
            F.col('get_tkn_prct.token') == F.col('ori_df.prod_nm_token'),
            F.col('get_tkn_prct.tkn_l_cate') == F.col('ori_df.l_cate')
        ]
        # 'right'
    ).select(
            ori_df['*'],
            F.col('get_tkn_prct.cnt')
    ).distinct().alias('get_prod_tkn')

    # # # ori_df.where(F.col('prod_nm_token') == '캐주얼').show(100, False)
    # get_prod_tkn.orderBy(F.col('prod_nm')).show(300, False)
    # # get_prod_tkn.select(F.count(F.col('prod_nm'))).show()

    # get_prod_tkn.coalesce(20).write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/test/prod2/")      # save hdfs
    get_prod_tkn.show()
    get_prod_tkn.write.format("parquet").mode("overwrite").save("data/parquet/prod2/")

    # get_prod_tkn.write.mode('overwrite').saveAsTable("stag_os.hive_test_3")

    # 송장명 토크나이징
    shipping_nm = spark.read.csv("/commerce/data/송장명.csv")\
                    .select(
                        F.explode(
                            F.split(F.regexp_replace(F.lower(F.col('_c2')), ' ', ','), ",")
                        ).alias('shipping_nm')
                    ).withColumn(
                        "shp_tkn",
                        F.regexp_replace(F.col('shipping_nm'), "[^ㄱ-힝]", '')
                    ).where(
                        F.length(F.col('shp_tkn')) > 1
                    ).alias("shipping_nm")

    shp_tkn_cnt = shipping_nm.groupBy(F.col('shp_tkn')).agg(F.log10(F.count(F.col('shp_tkn'))).alias('shp_cnt')).alias('shp_tkn_cnt')
    # shp_tkn_cnt.orderBy((F.col('shp_cnt')).desc()).show(300, False)
    # 후보들 : cnt 낮은것들이 후보에 가까움
    # noun(명사)만 된것, 1글자 유력
    #

    match_shp_prod = get_prod_tkn.join(
                                            shp_tkn_cnt,
                                            F.col('get_prod_tkn.prod_nm_token') == F.col('shp_tkn_cnt.shp_tkn'),
                                            'left'
                                        ) 
    exit(0)
