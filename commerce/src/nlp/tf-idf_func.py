# -*- coding: utf-8 -*-
"""

"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.feature import HashingTF, IDF, Tokenizer


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
    a = spark.read. \
        option('header', True). \
        csv('/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod.csv').limit(100)\
        .select(F.explode(F.split(F.regexp_replace(F.lower(F.col('상품명')), ' ', ','), ",")).alias('상품명')).where((F.col('상품명') =="고양이") | (F.col('상품명') =="크런치"))#.rdd
    # a.groupBy(F.col('상품명')).agg(F.count(F.col('상품명')).alias('cnt')).orderBy(F.col('cnt').desc()).show()

    sentenceData = spark.createDataFrame([
        (0, "Hi I heard about Spark"),
        (0, "I wish Java could use case classes"),
        (1, "Logistic regression models are neat")
    ], ["label", "sentence"])
    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    tokenizer.transform(sentenceData).head()

    # wordsData = tokenizer.transform(sentenceData)
    # hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
    # featurizedData = hashingTF.transform(wordsData)
    # idf = IDF(inputCol="rawFeatures", outputCol="features")
    # idfModel = idf.fit(featurizedData)
    # rescaledData = idfModel.transform(featurizedData)
    # for features_label in rescaledData.select("features", "label").take(3):
    #     print(features_label)


    exit(0)