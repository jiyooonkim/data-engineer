# -*- coding: utf-8 -*-

"""
# title : 상품명에 적합한 inner keyword 후보 추출 위해
# desc :
 - inner keyword : 검색에 사용하는 필터링 키워드
# document:
 - skip-gram : https://wikidocs.net/69141
# pro :
 - 속성, 불용어(stopword) 제거
 - 상품명에서 inner keyword 제거 후 남은 것이 Stopword 가 될 가능성은 ??
 # insight :
 - skip-gram을 이용해서 연관키워드 도출 가능 함    ex) 히프커버->힙커버, 낚시복, 낚시옷, 낚시용품 ..., 힐링쉴드-> 폰트리, 액정보호필름, 시계보호필름
# etc :
 - 연관키워드, 연관상품, 추천 상품 ,추천키워드 (해쉬태그....??)
"""

'''
word2vec(word embedding)
- word to vector
- 이론 : 자주 같이 등장할수록 두 단어는 비슷한 의미를 가진다는 것
- skip-gram : 중간단어로 맥락(주변) 예측
- cbow : 맥락(주변) 중간단어 예측

-
format
상품명      innerkwd

'''
############################
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import Word2Vec
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def get_embadding_layer(col_lst):
    # i : 입력1(embedding layer1)   j : 입력2(embedding layer2)
    lst = []
    for i in col_lst:
        for j in col_lst:
            if i != j:
                lst.append([i, j])
    return lst


@F.udf(returnType=T.StringType())
def get_txt_type(col):
    val = ''
    if col.isalpha():  # eng+kor, kor
        val = 'kor'
    if col.encode().isalpha():  # only eng
        val = 'eng'
    if col.isdigit():   # 숫자만
        val = 'num'
    # isalnum() # 영어/한글 + 숫자
    return val


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('jy_kim') \
        .master('local[4]') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config('spark.executor.extraJavaOptions', 'Ddev.ludovic.netlib.blas.nativeLib=libopenblas.so') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '16G')\
        .config("spark.dynamicAllocation.enabled", True)\
        .config("spark.shuffle.service.enabled", True)\
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()

    # skip-gram
    df = spark.read.parquet('hdfs://localhost:9000/test/prod2')\
        .select(F.regexp_replace(F.lower(F.col('prod_nm')), '  ', ' ').alias('prod_nm'))\
        .withColumn("prod_nm_tkns", F.split(F.regexp_replace(F.lower(F.col('prod_nm')), ' ', ','), ","))\
        .withColumn("couple_word", get_embadding_layer(F.col('prod_nm_tkns')))\
        .repartition(500)   # .limit(500)
    embd_lble = df.select(
                            F.col('prod_nm'),
                            F.explode(F.col('couple_word')).alias('embad_layer'),
                            F.col('embad_layer')[0].alias('layer1'),
                            F.col('embad_layer')[1].alias('layer2')
                        ).where(
                            (F.length(F.col('layer1')) > 1) &
                            (F.length(F.col('layer2')) > 1)
                        ).alias('embd_lble')

    # 예측값 구하기, 레이블이 1인것만 가능, 0은 불가
    kor_eng_lble_frq = embd_lble\
        .withColumn('txt_type', get_txt_type(F.col('layer1')))\
        .where(F.col('txt_type') == 'kor')\
        .groupBy(F.col('layer1'), F.col('layer2'))\
        .agg(F.count(F.col('layer2')).alias('cnt')).where(F.col('cnt') > 10)

    # layer2 상위 4개까지 리스트형식으로
    get_candidate = kor_eng_lble_frq\
        .withColumn(
            'prdt_val',
            F.round(F.log((F.col('cnt')/kor_eng_lble_frq.count())), 4)
        ).withColumn(
            'lyr2_rnk',
            F.rank().over(window.Window.partitionBy(F.col('layer1')).orderBy(F.col('prdt_val')))
        ).where(F.col('lyr2_rnk') < 6)
    # get_candidate.show(100, False)

    get_candidate.coalesce(20).write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/word2vec/skip_gram/cnadidate")  # save hdfs
    # todo : 뒷좌석, 뒷자석 -> typo correction
    get_candidate_list = get_candidate.groupBy(F.col('layer1')).agg(F.collect_list(F.col('layer2')))
    get_candidate_list.show(1000, False)
    get_candidate_list.coalesce(20).write.format("parquet").mode("overwrite").save("hdfs://localhost:9000/word2vec/skip_gram/cnadidate_list")  # save hdfs

    # cbow

    word2Vec = Word2Vec(vectorSize=4, seed=3, inputCol="prod_nm_tkns", outputCol="model")
    word2Vec.setMaxIter(10)
    model = word2Vec.fit(df)
    # model.getVectors().show(100, False)

    # ## step.1 ##
    # todo: 네거티브 샘플링 skip-gram(SGNS)
    # 중심단어 & 주변단어 매핑 해서 전체 확률 구해보기
    # 세트로 등장하는 단어 빈도수 잘라보면...?




    # # /usr/local/Cellar/hadoop/3.3.4/libexec/bin/hdfs

    #
    # attr = spark.read.parquet("hdfs://localhost:9000/dictionary/measures_attribution/")   # 속성 df
    # attr.show(10, False)





    exit(0)