
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('Ngram Job') \
        .master('local[*]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '16g') \
        .config('spark.driver.cores', '8') \
        .config('spark.executor.memory', '16g') \
        .config('spark.submit.deployMode', 'client') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", 10000) \
        .config('spark.sql.shuffle.partitions', '300') \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()

    import pyspark.sql.functions as F
    input_extract_councils_path = '/Users/jy_kim/Documents/private/jiyooonkim/data-engineer/commerce/src/etc/csv_files/data/england_councils/district_councils.csv'
    df = (spark.read.option("header", True).csv(input_extract_councils_path)
          .withColumn(
        'council_type',
        F.when(F.col(""), F.lit('District Council'))
    )
          )

    df.show(100, False)