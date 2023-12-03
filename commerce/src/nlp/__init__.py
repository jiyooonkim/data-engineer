# from pyspark.ml.base import (
#     Estimator,
#     Model,
#     Predictor,
#     PredictionModel,
#     Transformer,
#     UnaryTransformer,
# )
# from pyspark.ml.pipeline import Pipeline, PipelineModel
# from pyspark.ml import (
#     classification,
#     clustering,
#     evaluation,
#     feature,
#     fpm,
#     image,
#     recommendation,
#     regression,
#     stat,
#     tuning,
#     util,
#     linalg,
#     param,
# )
from pyspark.ml.torch.distributor import TorchDistributor
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.sql.window as window
import re
DEFAULT_PATH = '../'


class CreateData:
    """
        Managing table 
    """

    def __init__(self):
        self.column = column
        self.dirctory = dirctory
        self.file_type = file_type
        self.header = header
        self.db = db
        self.mode = mode
        self.table = table

    def read_data(self, dirctory, file_type, header=None):
        return None

    def save_data(self, dirctory, file_type, mode=None):
        return None


def get_tonkenizing():
    # todo
    get_nike_tkn_2 = nike_dt.select(
        F.explode(F.split(F.regexp_replace(F.lower(F.col('_c0')), "[^A-Za-z0-9가-힣]", ' '), ' ')))


@F.udf(returnType=T.StringType())
def get_txt_type(wd):
    if wd.encode().isalpha():  # only eng
        str_tp = 'eng'
    elif wd.isalpha():  # eng+kor, kor
        res = re.compile(u'[^a-z]+').sub(u'', wd)
        if res:  # kor + eng
            str_tp= 'engkor'
        else:  # only kor
            str_tp = 'kor'
    elif wd.isdigit():  # 숫자만
        str_tp = 'num'
    elif wd.isalnum():  # 영어/한글 + 숫자
        str_tp = 'txtnum'
    else:
        str_tp = 'etc'
    return str_tp


def init_spark_session(app_name="spark_job", arrow=True, dm="16g", dc=8, em="16g", deployMode="client"):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName(app_name) \
        .master('local[*]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', arrow) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', dm) \
        .config('spark.driver.cores', dc) \
        .config('spark.executor.memory', em) \
        .config('spark.submit.deployMode', deployMode) \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", 10000) \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()
    return spark

# __all__ = [
#     "init_spark_session",
# ]
