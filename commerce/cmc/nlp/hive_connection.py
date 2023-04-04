# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName('jy_kim') \
            .master('local[*]') \
            .config('spark.sql.execution.arrow.pyspark.enabled', True) \
            .config('spark.sql.session.timeZone', 'UTC') \
            .config('spark.driver.memory', '32G') \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.sql.warehouse.dir", 'hdfs://localhost:9000/user/hive/warehouse') \
        .enableHiveSupport() \
            .getOrCreate()
    # .config("hive.metastore.uris", "jdbc:mysql://localhost:3306/metastore") \

    df = spark.read.parquet("hdfs://localhost:9000/test/prod/*.parquet")
    df.show(10)
    df.write.mode('overwrite').saveAsTable("hive.test")