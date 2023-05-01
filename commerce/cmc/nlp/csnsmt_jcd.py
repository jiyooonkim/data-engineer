# -*- coding: utf-8 -*-
########################################################################
'''
# title : cosine similarity, jaccard similarity
# desc :
- 단어의 빈도수 중요할 경우 사용 (자카드유사도와 반대)
- text similarity 측정
-
'''
########################################################################

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window

@F.udf(returnType=T.DoubleType())
def get_jaccard_sim(str1, str2):
    # set : 중복성 무시
    a = set(str1) #set(str1.split())
    b = set(str2) #set(str2.split())
    itc = a.intersection(b)
    return float(len(itc)) / (len(a) + len(b) - len(itc))


if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName('jy_kim') \
            .master('local[*]') \
            .config('spark.sql.execution.arrow.pyspark.enabled', True) \
            .config('spark.sql.session.timeZone', 'UTC') \
            .config('spark.driver.memory', '32g') \
            .config('spark.driver.cores', '8') \
            .config('spark.driver.maxResultSize', '0') \
            .config('spark.executor.memory', '16g') \
            .config('spark.submit.deployMode', 'client') \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.network.timeout", 1000000) \
            .config('spark.ui.showConsoleProgress', True) \
            .config('spark.sql.repl.eagerEval.enabled', True) \
            .getOrCreate()
    prod = spark.read.parquet('hdfs://localhost:9000/test/prod2') \
        .select(F.regexp_replace(F.lower(F.col('prod_nm')), '  ', ' ').alias('prod_nm')) \
        .withColumn("prod_nm_tkns", F.split(F.regexp_replace(F.lower(F.col('prod_nm')), ' ', ','), ",")).distinct()\
        .alias('prod')    # cnt : 46271


    # Jaccard similarity
    self_prod = prod.alias('prod_1')\
        .join(
            prod.alias('prod_2'),
            F.col('prod_1.prod_nm') != F.col('prod_2.prod_nm'), 'full'
        ).select(
            F.col('prod_1.prod_nm').alias('prod_nm_1'),
            F.col('prod_1.prod_nm_tkns').alias('prod_nm_tkns_1'),
            F.col('prod_2.prod_nm').alias('prod_nm_2'),
            F.col('prod_2.prod_nm_tkns').alias('prod_nm_tkns_2')
        )
    self_prod = self_prod\
        .where(F.length(F.col('prod_1.prod_nm')) < 20)\
        .withColumn('Jaccard_Similarity', get_jaccard_sim(F.col('prod_nm_tkns_1'), F.col('prod_nm_tkns_2'))).where(F.col('Jaccard_Similarity') > 0.1)\
        .withColumn(
            '_rnk',
            F.rank().over(window.Window.partitionBy(F.col('prod_nm_1')).orderBy(F.col('Jaccard_Similarity')))
        ).where(F.col('_rnk')<4)
        # .withColumn("word1", F.explode(F.col('prod_nm_tkns_1')))\
        # .withColumn("word2", F.explode(F.col('prod_nm_tkns_2')))

    # self_prod.show(100, False)
    self_prod.write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/jaccard_similarity")

    # self_prod.select(F.count(F.col('prod_nm_1'))).show()



    exit(0)
