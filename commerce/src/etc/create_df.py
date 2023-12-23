# stop_word_1.orderBy(F.col('cnt').desc()).show(1000, False)
from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2Vec
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window
import os
os.chdir('../../../')
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
# df.show()
df.coalesce(1).write.mode('overwrite').csv("data/parquet/non_stopword")