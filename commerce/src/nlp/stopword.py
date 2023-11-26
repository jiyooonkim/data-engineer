'''

    불용어사전 대상 :  (도량형, 단순) 속성, 형용사, 부사, 수치(단순숫자?)
    1+1, 초특가, 국내산, 중국산, 좋은아침, 스타, 싱싱아삭, 요리용, unknown,
    불용어사전 미대상 : 상품번호, 모델명, 브랜드, 시리즈, 카테고리성 키워드

    todo
    - 상품명에서 미대상 제거후 남은 삼품명 보기
'''

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
    get_nike_tkn_2 = nike_dt.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('_c0')), "[^A-Za-z0-9가-힣]", ' '), ' ')))

    # get_nike_tkn_2.groupby(F.col('col')).agg(F.count(F.col('col'))).show(1000, False)

    l_cate = prod_1.select(F.explode(F.split(F.col('대분류'), "/")).alias("cate"))
    m_cate = prod_1.select(F.explode(F.split(F.col('중분류'), "/")).alias("cate"))
    s_cate = prod_1.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('소분류')), "[^A-Za-z0-9가-힣]", ' '), "/")).alias("cate"))
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
    stop_word_1 = prod_nm.join(ctg, F.col('ctg.cate') == F.col('prod_nm.prod_tkn'), 'leftanti')\
                   .distinct() \
                   .groupby(F.col('prod_tkn'))\
                   .agg(F.count(F.col('대분류')).alias('cnt'))\

    stop_word_1.where(F.col('cnt') >= '14').write.format('parquet').mode('overwrite').save('data/output/stop_word_1')
    # stop_word_1.orderBy(F.col('cnt').desc()).show(1000, False)
    df = spark.createDataFrame(["고양이",
"스테인레스",
"코스트코",
"임산부",
"컴퓨터",
"논슬립",
"삼성",
"케이블",
"스마트폰",
"디스플레이",
"블루투스",
"데스크탑",
"샤오미",
"나이키",
"무릎",
"이케아",
"다이아몬드",
"스테인리스",
"바보사랑",
"아디다스",
"안경",
"삼성전자",
"오뚜기",
"아트박스",
"디즈니",
"수건",
"딸기"], T.StringType())
    df.show()
    df.coalesce(1).write.mode('overwrite').csv("data/output/non_stopword")
    # df.write.format('csv').mode('overwrite').save('data/output/non_stopword')
    # prod_1.select(F.col('소분류'), F.col('중분류'), F.col('대분류')).distinct().show(1000, False)
    # prod_nm.select(F.col('prod_tkn'), F.col('대분류'), F.col('중분류')).where(F.col('prod_tkn') =='스테인레스').distinct().show(1000, False)
     # .withColumn(
     #        'rnk',
     #        F.rank().over(window.Window.partitionBy(F.col('대분류')).orderBy(F.col('cnt').desc()))
     #    ).orderBy(F.col('rnk').desc()))

    '''
    가설
    - 4개이상의 카테고리에서 등장한 키워드
    - 카테고리별 많이 등장한 키워드의 공통찾기
    
    '''