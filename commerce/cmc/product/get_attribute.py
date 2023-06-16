# -*- coding: utf-8 -*-
########################################################################
# 목적 : 송장명에 적합한 상품명, 키워드 후보  (외전, 카테고리 매핑)
# 이유 :

########################################################################

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_text_tpye(col):
    type_rlt = ""
    num_cnt = 0
    eng_cnt = 0
    kor_cnt = 0
    etc_cnt = 0
    # type_rlt = []
    for i in col:
        if i.isdigit():  # 숫자
            type_rlt += 'num '
            num_cnt += 1
            # type_rlt.append('num')
        elif i.encode().isalpha():  # 영어
            type_rlt += 'eng '
            eng_cnt += 1
            # type_rlt.append('eng')
        elif i.isalpha():  # 한글
            type_rlt += 'kor '
            kor_cnt += 1
            # type_rlt.append('kor')
        else:  # 그 외 기타
            type_rlt += 'etc '
            etc_cnt += 1
            # type_rlt.append('etc')
    return [type_rlt[:-1], int(num_cnt), int(eng_cnt), int(kor_cnt), int(etc_cnt)]


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

    ori_shp = spark.read \
        .option('header', True) \
        .csv('/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/송장명.csv')

    shp = ori_shp\
        .select(F.regexp_replace(F.col('_c2'), "[^a-zA-Zㄱ-힝0-9]", ' ').alias("shipping_nm")) \
        .withColumn(
            "shp_nm_token",
            F.explode(F.split(F.regexp_replace(F.lower(F.col('shipping_nm')), ' ', ','), ","))  # 영,한,숫 이외 제거 및 토큰화 작업
        ).where(
            F.col('shp_nm_token') != ""
        ).repartition(500, F.col('shp_nm_token')) \
        .alias('shp')
    shp_agg = shp\
        .groupBy(F.col('shp_nm_token'))\
        .agg(F.count(F.col('shp_nm_token'))
        .alias('cnt'))

    # shp_agg.orderBy(F.col('cnt').desc()).show(1000, False)

    # 상품명 df에서 카테고리 추출
    prod_df1 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod.csv")

    prod_df2 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod_2.csv")

    prod = prod_df1.unionByName(prod_df2, allowMissingColumns=True)  # .select(F.col('대분류'), F.col('중분류'), F.col('소분류'), F.col('세분류'))
    l_cate = prod.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('대분류')), '/', ','), ",")).alias('cate'))
    m_cate = prod.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('중분류')), '/', ','), ",")).alias('cate'))
    s_cate = prod.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('소분류')), '/', ','), ",")).alias('cate'))
    d_cate = prod.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('세분류')), '/', ','), ",")).alias('cate'))
    cate = (l_cate.unionAll(m_cate).unionAll(s_cate).unionAll(d_cate))\
        .select(F.regexp_replace(F.col('cate'), r"[^ㄱ-ㅣ가-힣\s]", '').alias('cate'))
    cate = cate \
        .select(F.explode(F.split(F.regexp_replace(F.col('cate'), ' ', ','), ",")).alias('cate')) \
        .distinct()
    # F.collect_list("prod_nm_token").alias('tokens')
    # cate.orderBy(F.col('cate')).show(10000)

    ''' (도량형)속성 추출 '''
    # 추출 대상 : 도량형(숫+영, 숫+한) 숫자/한글/영어 체크후 판별 로직,
    # 목표 결과물: 불용어(Stop word)에 사용 => 상품에서 무의미 토큰 추출
    # 제거 대상 : 브랜드, 상품명
    get_txt_tp = shp_agg \
        .join(cate, F.col('shp_nm_token') == F.col('cate'), 'leftanti') \
        .orderBy(F.col('cnt').desc()) \
        .withColumn('txt_tps', get_text_tpye(F.col('shp_nm_token'))[0])\
        .withColumn('num_cnt', get_text_tpye(F.col('shp_nm_token'))[1])\
        .withColumn('eng_cnt', get_text_tpye(F.col('shp_nm_token'))[2])\
        .withColumn('kor_cnt', get_text_tpye(F.col('shp_nm_token'))[3])\
        .withColumn('etc_cnt', get_text_tpye(F.col('shp_nm_token'))[4])\
        # .withColumn('txt_tp', F.explode(F.split(F.regexp_replace(F.lower(get_text_tpye(F.col('shp_nm_token'))), ' ', ','), ",")))

    # (도량형) 속성 (measures attribution)
    msr_attr_1 = get_txt_tp\
        .where((F.col('txt_tps').like("%num kor")) | (F.col('txt_tps').like("%num eng")))\
        .where(F.length(F.col('shp_nm_token')) < 4)
    # msr_attr_1.select(F.count(F.col('shp_nm_token'))).show()

    msr_attr_2 = get_txt_tp\
        .where((F.col('num_cnt') >= F.col('eng_cnt')) | (F.col('num_cnt') >= F.col('kor_cnt')))\
        .where((F.col('num_cnt') >= 1) & (F.col('eng_cnt') < 3))\
        .where((F.col('num_cnt') >= 1) & (F.col('kor_cnt') < 3))\
        .where(((F.col('num_cnt') > 0) &(F.col('eng_cnt') > 0)) | ((F.col('kor_cnt') > 0) & (F.col('num_cnt') > 0)))\
        .where(~(F.col('shp_nm_token').like("%원")))\
        .where((F.col('txt_tps').like("%num kor kor")) | (F.col('txt_tps').like("%num eng eng")))
    # msr_attr_2.show(10000, False)
    # msr_attr_2.select(F.count(F.col('shp_nm_token'))).show()

    msr_attr = (msr_attr_1.unionAll(msr_attr_2)).where(F.length(F.col('shp_nm_token')) < 7).distinct()
    msr_attr.select(F.count(F.col('shp_nm_token'))).show()
    # msr_attr.coalesce(15).write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/dictionary/measures_attribution/")  # save hdfs
    msr_attr.write.format("parquet").mode("overwrite").save("/Users/jy_kim/Documents/private/nlp-engineer/data/parquet/measures_attribution/")  # save hdfs

    exit(0)