# -*- coding: utf-8 -*-
"""
# title : cosine similarity, jaccard similarity
# desc :
- 단어의 빈도수 중요할 경우 사용 (자카드유사도와 반대)
- text similarity 측정
- Jaccard_Similarity 높을수록 유사도 큰 것
- ver1. 상품명 vs 상품명 : 유사한 상품명
- ver2. 상품명 vs 송장명  : 상품명으로 송장명 유추

# insight :
 - 연관 상품, 추천 상품에 사용 가능성
  ex) 프라다 뿌르 옴므 오 드 뚜왈렛 & 랄프로렌 폴로 블루 맨 베어 에디션 오 드 뚜왈렛, 쿠첸 cjh pa0651ic & 쿠첸 cjh bt1061sk
# dev :
 - 현) 카테고리 미적용, 상품개수 4만개라서..  -> 카테고리 기준으로 묶는다면 유사한(연관성) 상승 할 것으로 예측
# pro :
 - 단어(word)자체 비교이기 때문에 동일한 단어가 아닐 경우 예측이 안 됨으로 자세한 분석이 될 수 없음 ->  유의어, 동의어, 외래어.. 같은 것은 비교가 안됨
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window


@F.udf(returnType=T.DoubleType())
def get_jaccard_sim(str1, str2):
    # set 이유 : 중복성 무시
    a = set(str1)   # set(str1.split())
    b = set(str2)   # set(str2.split())
    a_set = []
    for wd in a:
        if len(wd) > 1:
            a_set.append(wd)

    b_set = []
    for wd in b:
        if len(wd) > 1:
            b_set.append(wd)
    itc = set(a_set).intersection(set(b_set))
    return float(len(itc)) / (len(set(a_set)) + len(set(b_set)) - len(set(itc)))


if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName('jy_kim') \
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
    prod = spark.read.parquet('hdfs://localhost:9000/test/prod2/') \
        .select(
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.lower(F.col('prod_nm')), "[^A-Za-z0-9가-힣]", ' '),
                    r"\s+", ' '
                )
            ).alias('prod_nm'))\
        .withColumn("prod_nm_tkns", F.split(F.regexp_replace(F.lower(F.col('prod_nm')), ' ', ','), ",")).distinct()\
        .alias('prod')\
        .repartition(600)    # cnt : 46271

    # Jaccard similarity
    # ver1. 상품명 vs 상품명
    self_prod = prod.alias('prod_1').where(F.length(F.col('prod_1.prod_nm')) >= 20)\
        .join(
            prod.alias('prod_2'),
            F.col('prod_1.prod_nm') != F.col('prod_2.prod_nm'), 'full'
        ).select(
            F.col('prod_1.prod_nm').alias('prod_nm_1'),
            F.col('prod_1.prod_nm_tkns').alias('prod_nm_tkns_1'),
            F.col('prod_2.prod_nm').alias('prod_nm_2'),
            F.col('prod_2.prod_nm_tkns').alias('prod_nm_tkns_2')
        )

    self_join_prod = self_prod\
        .withColumn('Jaccard_Similarity', get_jaccard_sim(F.col('prod_nm_tkns_1'), F.col('prod_nm_tkns_2')))\
        .where(F.col('Jaccard_Similarity') > 0.22)\
        # .withColumn(
        #     '_rnk',
        #     F.rank().over(window.Window.partitionBy(F.col('prod_nm_1')).orderBy(F.col('Jaccard_Similarity')))
        # ).where(F.col('_rnk') < 4)
        # .withColumn("word1", F.explode(F.col('prod_nm_tkns_1')))\
        # .withColumn("word2", F.explode(F.col('prod_nm_tkns_2')))
    # self_prod.select(F.col('prod_nm_1'), F.col('prod_nm_2'), F.col('Jaccard_Similarity')).show(1000, False)
    self_join_prod.write.format("parquet").mode("append").save("hdfs://localhost:9000/jaccard_similarity")

    # ver2. 상품명 vs 송장명 : 상품명으로 유사한 송장명 유추
    # 송장명 토크 나이징
    shipping_df = spark.read.csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/송장명.csv")\
        .select(
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.lower(F.col('_c2')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' ')
            ).alias('shipping_nm'),
        ).where(
            (F.col('shipping_nm').isNotNull())
            & (F.length(F.col('shipping_nm')) > 1)
        ).withColumn(
            "shipping_nm_tkns",
            F.split(F.regexp_replace(F.lower(F.col('shipping_nm')), ' ', ','), ",")
        ).where(F.length(F.col('shipping_nm')) < 20)\
        .repartition(600)\
        .alias('shipping_df')

    # shipping_df.orderBy(F.col('shipping_nm')).show(10, False)
    # prod.show(10, False)
    # shipping_df.select(F.count(F.col('shipping_nm'))).show(100, False)

    ship_prod = shipping_df.join(
        prod,
        F.col('shipping_df.shipping_nm') != F.col('prod.prod_nm'), 'full'
    ).alias('ship_prod')
    # ship_prod.show(100, False)

    ship_prod = ship_prod.withColumn(
        'Jaccard_Similarity',
        # F.array_intersect(F.col('prod_nm_tkns'), F.col('shipping_nm_tkns'))
        get_jaccard_sim(F.col('prod_nm_tkns'), F.col('shipping_nm_tkns'))
    ).select(F.col('prod_nm'), F.col('shipping_nm'), F.col('Jaccard_Similarity'))
    # ship_prod.select(F.count(F.col('prod_nm'))).show()
    ship_prod.repartition(600, F.col('prod_nm_tkns')).where(F.col('Jaccard_Similarity') > 0.4).show(100, False)
    # ship_prod.orderBy(F.col('Jaccard_Similarity')).write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/jaccard_similarity_ship_prod")

    exit(0)