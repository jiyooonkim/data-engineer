"""
    # title :  불용어 추출
    # desc :
        - 불용어 대상 :  (도량형, 단순) 속성, 형용사, 부사, 수치(단순숫자?)
                    1+1, 초특가, 국내산, 중국산, 좋은아침, 스타, 싱싱아삭, 요리용, unknown,
        - 불용어 미대상 : 상품번호, 모델명, 브랜드, 시리즈, 카테고리성 키워드
    - 해결 방안
        - 4개이상의 카테고리에서 등장한 키워드
        - 카테고리별 많이 등장한 키워드의 공통찾기
"""

import os

os.chdir('../../../')
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window

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

    prod_1 = spark.read.option('header', True). \
        csv("commerce/data/nvr_prod.csv") \
        .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))

    nike_dt = spark.read. \
        csv("commerce/data/nike_data.csv")
    get_nike_tkn_2 = nike_dt.select(
        F.explode(F.split(F.regexp_replace(F.lower(F.col('_c0')), "[^A-Za-z0-9가-힣]", ' '), ' ')))

    # get_nike_tkn_2.groupby(F.col('col')).agg(F.count(F.col('col'))).show(1000, False)

    l_cate = prod_1.select(F.explode(F.split(F.col('대분류'), "/")).alias("cate"))
    m_cate = prod_1.select(F.explode(F.split(F.col('중분류'), "/")).alias("cate"))
    s_cate = prod_1.select(
        F.explode(F.split(F.regexp_replace(F.lower(F.col('소분류')), "[^A-Za-z0-9가-힣]", ' '), "/")).alias("cate"))
    color_attr = spark.read.parquet('data/parquet/color_attribution/').select(F.col('color')).distinct()
    ctg = (l_cate.union(m_cate).union(s_cate).union(get_nike_tkn_2).union(color_attr)
           .groupby(F.col('cate')).agg(F.count(F.col('cate')).alias('cnt')).alias('ctg'))

    prod_nm = prod_1.select(
        F.explode(
            F.split(
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(F.lower(F.col('상품명')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' '
                    )
                ), ' '
            )
        ).alias('prod_tkn'), F.col('대분류'), F.col('대분류'), F.col('중분류')).alias('prod_nm')
    stop_word_1 = (prod_nm.join(ctg, F.col('ctg.cate') == F.col('prod_nm.prod_tkn'), 'leftanti')
                   .distinct()
                   .groupby(F.col('prod_tkn'))
                   .agg(F.count(F.col('대분류')).alias('cnt'))
                   .alias('stop_word_1'))
    non_stop_wd = spark.read.option('header', False).csv("data/parquet/non_stopword").alias('non_stop_wd')  # 불용어 아닌 것 제거

    st_wd = stop_word_1.join(
                                non_stop_wd,
                                F.col('non_stop_wd._c0') == F.col('stop_word_1.prod_tkn'),
                                'leftanti'
                            )
    st_wd.where(F.col('cnt') >= '14').select(F.count(F.col('prod_tkn'))).show(1000, False)
    st_wd.where(F.col('cnt') >= '14').show(1000, False)
    # stop_word_1.coalesce(1).where(F.col('cnt') >= '14').write.format('parquet').mode('overwrite').save('data/parquet/stop_word_1')
    st_wd.where(F.col('cnt') >= '14').select(F.col('prod_tkn')).coalesce(1).write.format('text').mode('overwrite').save('data/parquet/stopword')
    # df1.write.text("output_compressed", compression="gzip")

    exit(0)
