
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os



if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('typo dictionary job') \
        .master('local[5]') \
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
    df = spark.read.parquet("/Users/jy_kim/Downloads/73vfq6ua.parquet")
    df.show()
