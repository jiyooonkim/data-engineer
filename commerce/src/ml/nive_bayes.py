'''
    # title : Naive Bayes Classifier
    # doc : https://tensorflow.blog/%ED%8C%8C%EC%9D%B4%EC%8D%AC-%EB%A8%B8%EC%8B%A0%EB%9F%AC%EB%8B%9D/2-3-4-%EB%82%98%EC%9D%B4%EB%B8%8C-%EB%B2%A0%EC%9D%B4%EC%A6%88-%EB%B6%84%EB%A5%98%EA%B8%B0/
    # desc :
        - 조건부 확률 기반의 분류 모델
        - 지도 학습
        - bayes : 두 확률 변수의 사전 확률과 사후 확률 사이의 관계를 나타내는 정리
        - GaussianNB : 정규분포
        - BernoulliNB : 베르누이분포
        - MultinomialNB : 다항분포

    # Refer :
        - https://bkshin.tistory.com/entry/%EB%A8%B8%EC%8B%A0%EB%9F%AC%EB%8B%9D-1%EB%82%98%EC%9D%B4%EB%B8%8C-%EB%B2%A0%EC%9D%B4%EC%A6%88-%EB%B6%84%EB%A5%98-Naive-Bayes-Classification
    # 주제
        - 나이브베이즈 분류 활용하여 송장명 카테고리 예측
            - traiging set : nvr_prod
            - test set : 송장명
'''

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window


if __name__ == "__main__":
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

    # prod_1 = spark.read. \
    #     option('header', True). \
    #     csv("/Users/jy_kim/Documents/private/data-engineer/commerce/data/nvr_prod.csv") \
    #     .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))
    #
    # prod_1.show()
    # prod_1.select(F.count(F.col('상품명'))).show()

    df = spark.read.parquet('/Users/jy_kim/Documents/private/data-engineer/data/parquet/linked_predict')
    df.show(1000, False)
    '''
        - 사건 B가 일어난 후 사건 A가 일어날 확률
        1. 전제: 두 사건 A, B가 있고, 사건 B가 발생한 이후에 사건 A가 발생한다고 가정한다.
        2. 정의: 사건 B가 일어난 후 사건 A가 일어날 확률이다.
        3. P(A): 사건 A가 일어날 확률
        4. P(B): 사건 B가 일어날 확률 = 사건 A가 발생하기 전 사건 B가 일어날 확률 = 사전확률
        5. P(A¦B): 사건 B가 일어난 후 사건 A가 일어날 확률 = 조건부 확률
        6. P(B¦A): 사건 A가 일어났을 때 사건 B가 앞서 일어났을 확률 = 사후확률
    '''
    #

