# -*- coding: utf-8 -*-
"""
    # title : 합성어 인덱싱
    # doc : 합성어를 es에 색인
"""
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import os

os.chdir('../../../')

es_options = {
    "es.nodes": "http://localhost:9200",
    "es.nodes.wan.only": "true",
    "es.batch.size.bytes": "6m",
    "es.batch.size.entries": "6000",
    "es.batch.write.refresh": "false",
    "es.net.http.auth.user": "elastic",
    "es.net.http.auth.pass": "elastic",

}

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('Export compound word at Elasticsearch') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config('spark.driver.memory', '8g') \
        .config('spark.driver.cores', '4') \
        .config('spark.executor.memory', '8g') \
        .config("spark.network.timeout", 10000) \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.shuffle.partitions', '200') \
        .getOrCreate()
    # .config("spark.jars", "../jar/elasticsearch-spark-20_2.12-8.8.2.jar") \

    df = spark.read.parquet('data/parquet/compound/')
    df.show(10, False)

    (df.write \
     .format("org.elasticsearch.spark.sql") \
     .options(**es_options)
     .option("es.resource", 'compound2') \
     .mode("overwrite") \
     .save()
     )

    es_reader = (spark.read
                 .format("org.elasticsearch.spark.sql")
                 .option("inferSchema", "true")
                 .option("es.read.field.as.array.include", "tags")
                 .option("es.nodes", "localhost:9200")
                 )

    query = {'match': {'target_word': '0t장판'}}
    get_compound = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.read.field.as.array.include", "NerArray") \
        .option("es.read.metadata", "true") \
        .option("es.query", query) \
        .option("inferSchema", "true") \
        .option("es.nodes", "localhost:9200") \
        .option("es.nodes.discovery", "true") \
        .load("compound")
    spark.sql("show tables").show()
    get_compound.select('target_word').show(10, False)
    exit(0)
