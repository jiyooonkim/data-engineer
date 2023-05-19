# -*- coding: utf-8 -*-
"""
# title : cosine similarity
# doc : https://needjarvis.tistory.com/665
# desc :
- 단어의 빈도수 중요할 경우 사용 (자카드유사도와 반대)
- text similarity 측정
- ver1. 상품명 vs 상품명 : 유사한 상품명
- ver2. 상품명 vs 송장명  : 상품명으로 송장명 유추

# insight :
 - 연관 상품, 추천 상품에 사용 가능성
  ex) 프라다 뿌르 옴므 오 드 뚜왈렛 & 랄프로렌 폴로 블루 맨 베어 에디션 오 드 뚜왈렛, 쿠첸 cjh pa0651ic & 쿠첸 cjh bt1061sk
# dev :
 - 현) 카테고리 미적용, 상품개수 4만개라서..  -> 카테고리 기준으로 묶는다면 유사한(연관성) 상승 할 것으로 예측
# pro :
 - 단어(word)자체 비교이기 때문에 동일한 단어가 아닐 경우 예측이 안 됨으로 자세한 분석이 될 수 없음 ->  유의어,동의어.. 같은 것은 비교가 안됨
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window


@F.udf(T.MapType(T.StringType(), T.StringType()))
def create_struct(key, val):
    print("key : " , key)
    print("val : " , val)
    # return {key: val}


@F.udf(returnType=T.ArrayType(T.IntegerType()))
def get_cosine_similarity(val1, val2):
    # val1 type : [2, 5, 3, 7]
    data1 = sum([i ** 2 for i in range(1, val1 + 1)]) ** (1 / 2)
    data2 = sum([i ** 2 for i in range(1, val2 + 1)]) ** (1 / 2)
    val = data1 * data2
    return val
    
    
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
                    F.regexp_replace(F.lower(F.col('prod_nm')), "[^A-Za-z0-9가-힣]", ' '),    # 다중 공백 제거
                    r"\s+", ' '
                )
            ).alias('prod_nm')
        )\
        .withColumn("prod_nm_tkns", F.split(F.regexp_replace(F.lower(F.col('prod_nm')), ' ', ','), ",")).distinct()\
        .alias('prod')\
        .repartition(600)    # cnt : 46271

    # Cosine similarity
    # ver1. 상품명 vs 상품명
    # todo : 공백 제거
    get_token = prod.withColumn('tkns', F.explode(F.col('prod_nm_tkns')))
    get_token_cnt = get_token\
        .groupby(F.col('prod_nm'), F.col('tkns'))\
        .agg(F.count(F.col('tkns')).alias('cnt'))\


    get_token_cnt = get_token_cnt\
        .groupby(F.col('prod_nm'))\
        .agg((F.create_map(F.collect_list(F.col('tkns')), F.collect_list(F.col('cnt')))).alias('map_list'))

    aa = get_token_cnt.where(F.length(F.col('prod_nm')) < 19).alias('df1')\
        .join(
            get_token_cnt.alias('df2'),
            F.col('df1.prod_nm') != F.col('df2.prod_nm'),
            # 'full'
        ).select(
            F.col('df1.prod_nm').alias('prod_nm_1'),
            F.col('df1.map_list').alias('map_list_1'),
            F.col('df2.prod_nm').alias('prod_nm_2'),
            F.col('df2.map_list').alias('map_list_2'),
        )

    # aa.select(F.count(F.col('prod_nm_1'))).show()   # All cnt : 2139848822
    aa.show(100, False)








    exit(0)