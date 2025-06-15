
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os



if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('test job') \
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


    data = [("name1", 1), ("name2", 2), ("name3", 3), ("name3", 4), ("name3", 4)]
    columns = ["Name", "dgree"]
    df = spark.createDataFrame(data, columns)


    # df.show()

    # 외부에서 정의된 dict (직렬화할 수 없음)
    my_dict = {"key1": "value1", "key2": "value2"}


    # UDF 정의 (클로저에서 dict를 참조)
    def process_data_with_dict(x):
        my_dict["key3"] = x  # 딕셔너리를 수정하는 작업
        return  my_dict, "!23"


    # 데이터프레임 생성
    data = [("John", 28), ("Alice", 35), ("Bob", 42)]
    df = spark.createDataFrame(data, ["name", "age"])

    # UDF 등록
    process_data_udf = F.udf(process_data_with_dict, T.StringType())

    # UDF를 DataFrame에 적용
    df_with_processed_data = df.withColumn("processed", process_data_udf(df["age"]))

    # 실행 시 `TaskNotSerializable` 예외 발생
    # df_with_processed_data.show()





    import requests

    json_url = "http://health.data.ny.gov/api/views/jxy9-yhdk/rows.json"

    # Step 1: Read json
    _json = requests.get(json_url).json()
    # print(_json['data'][0])

    # Step 2: create dataframe
    data = _json['data']
    cols = []
    for col in _json['meta']['view']['columns']:
        cols.append(col['name'])
        # print(col)
    print((cols))
    schema = T.StructType(
        [
            T.StructField("sid", T.StringType(), True),
            T.StructField("id", T.StringType(), True),
            T.StructField("position", T.IntegerType(), True),
            T.StructField("created_at", T.StringType(), True),
            T.StructField("created_meta", T.DoubleType(), True),
            T.StructField("updated_at", T.StringType(), True),
            T.StructField("updated_meta", T.StringType(), True),
            T.StructField("meta", T.StringType(), True),
            T.StructField("Year", T.StringType(), True),
            T.StructField("First", T.StringType(), True),
            T.StructField("County", T.StringType(), True),
            T.StructField("Sex", T.StringType(), True),
            T.StructField("Count", T.StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema=schema)
    df.show()
    # Step 3 : register temp table
    df.createOrReplaceTempView("baby_names_tmp")

    df.show(1, False)

    # Step 4 :
    result = spark.sql("SELECT * FROM baby_names_tmp LIMIT 5")
    result.show()

