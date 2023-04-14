# -*- coding: utf-8 -*-
############################
# title : 유사한 상품명 찾기 by TF-IDF &
# desc : 연관있는 상품명을 찾아 브랜드, 시리즈, 속성 ,카테고리 등 보기 위해 (참고문서:https://yeong-jin-data-blog.tistory.com/entry/TF-IDF-Term-Frequency-Inverse-Document-Frequency)
# pro : - 각 상품명(문서)을 토크나이징 후 비교
#
############################
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


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
        csv("/Users/jy_kim/Documents/private_project/commerce/data/nvr_prod.csv")

    df2 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private_project/commerce/data/nvr_prod_2.csv")

    df = df1.unionByName(df2, allowMissingColumns=True)

    ori_df = df.\
        select(
            F.regexp_replace(F.col('상품명'), "[^a-zA-Zㄱ-힝0-9]", ' ').alias("prod_nm"),
            # F.col('cate_code'),
            F.col('대분류').alias('l_cate'),
            # F.col('중분류').alias('m_cate'),
            # F.col('소분류').alias('s_cate'),
            # F.col('세분류').alias('d_cate'),
        ).withColumn(
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
        .groupBy(F.col('prod_nm'), F.col('l_cate'))\
        .agg(F.collect_list("prod_nm_token").alias('tokens'))\
        .withColumn('token', F.explode(F.col('tokens')))\
        .alias('get_collect_list')

    # TF(t, d) = (문서 d 에서 단어 t 의 출현 빈도) / (문서 d 에서 총 단어의 수)
    tf_t = get_collect_list \
        .groupBy(F.col('prod_nm'), F.col('l_cate'), F.col('token')) \
        .agg(F.count(F.col('token')).alias('tf_t_cnt')) \
        .alias('tf_t').repartition(500, F.col('token'))  # (문서 d 에서 단어 t 의 출현 빈도)
    # tf_t.where(F.col('prod_nm') == 'MAD DOG 매드독 차량용 공기청정기 MAD 350').show(100, False)
    # tt.select(F.count(F.col('prod_nm'))).show()

    tf_d = get_collect_list\
        .groupBy(F.col('prod_nm'), F.col('l_cate'))\
        .agg(F.count(F.col('token')).alias('tf_d_cnt'))\
        .alias('tf_d')    # (문서 d 에서 총 단어의 수)
    # tf_d.where(F.col('prod_nm') == 'MAD DOG 매드독 차량용 공기청정기 MAD 350').show(100, False)
    # tf_d.show(10, False)
    # dd.select(F.count(F.col('prod_nm'))).show()

    # tf.where(F.col('token') == '안전').where(F.col('l_cate') == '출산/육아').show(1000, False)
    # tf_t.select(F.count(F.col('prod_nm'))).show()     # 321850
    # tf_d.select(F.count(F.col('prod_nm'))).show()     # 46672

    # tf_t.coalesce(10).write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/test/tf_t/")  # save hdfs
    # tf_d.coalesce(10).write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/test/tf_d/")  # save hdfs

    tf = tf_t.join(
                        tf_d,
                        [
                            F.col('tf_t.prod_nm') == F.col('tf_d.prod_nm'),
                            F.col('tf_t.l_cate') == F.col('tf_d.l_cate')
                        ]
                    ).select(
                        tf_t['*'],
                        F.col('tf_d.tf_d_cnt')
                    ).withColumn(
                        'tf',
                        F.round(F.col('tf_t_cnt')/F.col('tf_d_cnt'), 3)
                    ).alias('tf')

    # IDF(t, D) = log(총 문서의 개수 / 단어 t를 포함하는 문서의 개수)
    # IDF는 전체 문서수를 해당 단어의 DF로 나눈 뒤 로그를 취해준 값
    tt = df.count()  # 총 문서의 개수
    DD = ori_df.groupBy(F.col('prod_nm_token'))\
        .agg(F.count(F.col('prod_nm')).alias('D'))\
        .withColumn("t", F.lit(tt))\
        .withColumn("idf", F.round(F.log(F.col('t')/F.col('D')), 3))\
        .alias('DD')    # 단어 t를 포함하는 문서의 개수

    # tf_idf = tf * idf
    tf_idf = tf.join(
                        DD,
                        [
                            F.col('tf.token') == F.col('DD.prod_nm_token'),
                        ]
                    ).select(
                        F.col('tf.prod_nm'),
                        F.col('tf.l_cate'),
                        F.col('tf.token'),
                        F.col('tf.tf'),
                        F.col('DD.idf')
                    ).withColumn(
                        'tf_idf',
                        F.col('tf') * F.col('idf')
                    )

    # tf_idf.select(F.col('l_cate'), F.col('token'), F.col('tf_idf')).where(F.col('l_cate') == '디지털/가전').distinct().orderBy(F.col('tf_idf').asc()).show(1000, False)
    # tf_idf.select(F.col('prod_nm'), F.col('l_cate'), F.col('token'), F.col('tf_idf')).where(F.col('token') == '안전').distinct().orderBy(F.col('prod_nm').asc()).show(1000, False)
    tf_idf.coalesce(15).write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/prod/tf_idf/")  # save hdfs

    ####################################################################################
    # 카테고리별로 tf-idf 구해보기 -> 카테고리별로 많이 등장하는 키워드 보기 위해
    # TF(t, d) = (문서 d 에서 단어 t 의 출현 빈도) / (문서 d 에서 총 단어의 수)
    tf_t_1 = get_collect_list \
        .groupBy(F.col('l_cate'), F.col('token')) \
        .agg(F.count(F.col('token')).alias('tf_t_cnt')) \
        .alias('tf_t_1').repartition(500, F.col('token'))  # (문서 d 에서 단어 t 의 출현 빈도)
    # tf_t.where(F.col('prod_nm') == 'MAD DOG 매드독 차량용 공기청정기 MAD 350').show(100, False)
    # tt.select(F.count(F.col('prod_nm'))).show()

    tf_d_1 = get_collect_list \
        .groupBy(F.col('l_cate')) \
        .agg(F.count(F.col('token')).alias('tf_d_cnt')) \
        .alias('tf_d_1')  # (문서 d 에서 총 단어의 수)

    tf_1 = tf_t_1.join(
                            tf_d_1,
                            F.col('tf_t_1.l_cate') == F.col('tf_d_1.l_cate'),
                            'left'
                        ).select(
                            tf_t_1['*'],
                            F.col('tf_d_1.tf_d_cnt')
                        ).withColumn(
                            'tf',
                            (F.col('tf_t_cnt')/F.col('tf_d_cnt'))
                        ).alias('tf_1')

    # IDF(t, D) = log(총 문서의 개수 / 단어 t를 포함하는 문서의 개수)
    tt_1 = df.count()  # 총 문서(상품명)의 개수
    DD_1 = ori_df\
        .groupBy(F.col('prod_nm_token')) \
        .agg(F.count(F.col('prod_nm')).alias('D')) \
        .withColumn("t", F.lit(tt)) \
        .withColumn("idf", F.log(F.col('t') / F.col('D'))) \
        .alias('DD_1')  # 단어 t를 포함하는 문서의 개수

    # tf_idf = tf * idf
    tf_idf_1 = tf_1.join(
                            DD_1,
                            F.col('tf_1.token') == F.col('DD_1.prod_nm_token'),
                            'left'
                        ).select(
                            # tf_1['*'],
                            F.col('tf_1.l_cate'),
                            F.regexp_replace(F.col('tf_1.token'), r"[^ㄱ-ㅣ가-힣\s]", '').alias('token'), #  한글, 영어와 숫자 끊어 보기
                            F.col('tf_1.tf'),
                            F.col('DD_1.idf')
                        ).withColumn(
                            'tf_idf',
                            F.col('tf') * F.col('idf')
                        ).where(F.length(F.col('token')) > 1)

    # 안전, 국산, 남자, 대형, 투명, 기타



    # tf_idf_1\
    #     .orderBy(F.col('tf_idf').desc()) \
    #     .where(F.col('token') == '개입') \
    #     .show(1000, False)

    get_tkn_lcate_cnt = tf_idf_1.groupBy(F.col('token')).agg(F.count(F.col('l_cate')).alias('l_cate_cnt')).orderBy(F.col('l_cate_cnt').desc())
    # get_tkn_lcate_cnt.where(F.col('token') == '효과').show(1000, False)

    aaa = tf_idf_1.groupBy(F.col('l_cate'), F.col('token')).agg(F.count(F.col('token')).alias('token_cnt')).orderBy(F.col('token').desc())
    aaa.where(F.col('token_cnt') > 10).show(1000, False)
    exit(0)