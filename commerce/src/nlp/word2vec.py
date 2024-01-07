# -*- coding: utf-8 -*-
"""
# title : 상품명에 적합한 inner keyword 후보 추출 위해
# desc :
    - inner keyword : 검색에 사용하는 필터링 키워드
# document:
    - skip-gram :
        - https://wikidocs.net/69141
        - https://everyday-deeplearning.tistory.com/entry/Python%EC%9C%BC%EB%A1%9C-%EB%94%A5%EB%9F%AC%EB%8B%9D%ED%95%98%EA%B8%B0%EC%9E%90%EC%97%B0%EC%96%B4-2%EC%9E%84%EB%B2%A0%EB%94%A9-Word2Vec
# pro :
     - 속성, 불용어(stopword) 제거
     - 상품명에서 inner keyword 제거 후 남은 것이 Stopword 가 될 가능성은 ??
 # insight :
    - 연관키워드 : window 내에 가장 많이 등장한 키워드 세트
        ex) 히프커버->힙커버, 낚시복, 낚시옷, 낚시용품 ..., 힐링쉴드-> 폰트리, 액정보호필름, 시계보호필름, 골프 -> 골프체, 웨어, 남성, 가방, 여성
    - 오타교정 : 초,중,종성의 앞,뒤로 올 확률 구할 수 있을듯 : 정타사전 만들어 보기!!
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

os.chdir('../../../')


# @F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
# def get_embadding_layer(col_lst=[]):
#     '''
#         :param  i : 입력1(embedding layer1)   j : 입력2(embedding layer2)
#         window size = 1
#     '''
#     lst = []
#     for i in col_lst:
#         for j in col_lst:
#             if i != j:
#                 lst.append([i, j])
#     return lst

@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def sliding_window(tokens=[], window_size=3):
    """
        :param tokens: 토큰리스트 : ['홀더', '스탠드', '삼각대', '플렉시블', '헤드', '핫슈', '조인트', '듀얼']
        :param window_size: 슬라이스 사이즈
        :return: ['홀더', '스탠드', '삼각대']
    """
    skip_gram = list()
    for idx in range(len(tokens)):
        if idx - window_size < 0:
            skip_gram.append(tokens[0: idx + window_size + 1])
        else:
            skip_gram.append(tokens[idx - window_size: idx + window_size + 1])
    return skip_gram


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def get_skip_gram(tokens=[], center="", window_size=3):
    """
        :param tokens: ['플렉시블', '헤드', '핫슈', '조인트', '듀얼']
        :param window_size: 2
        :return: [['핫슈', '플렉시블'], ['핫슈', '헤드'], ['핫슈', '조인트'], ['핫슈', '듀얼']]
    """

    def skip_grams(center, tkns):
        grams = list()
        for i in tkns:
            if center != i:
                grams.append([center, i])
        return grams

    res = list()
    for idx in range(len(tokens)):
        if center == tokens[idx]:
            if idx - window_size < 0:
                res.extend(skip_grams(tokens[idx], tokens[0: idx + window_size + 1]))
            else:
                res.extend(skip_grams(tokens[idx], tokens[idx - window_size: idx + window_size + 1]))
    return res


@F.udf(returnType=T.StringType())
def get_txt_type(col):
    val = ''
    if col.isalpha():  # eng+kor, kor
        val = 'kor'
    if col.encode().isalpha():  # only eng
        val = 'eng'
    if col.isdigit():  # 숫자만
        val = 'num'
    # isalnum() # 영어/한글 + 숫자
    return val


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('Word 2 vector Job') \
        .master('local[*]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '32g') \
        .config('spark.driver.cores', '8') \
        .config('spark.executor.memory', '16g') \
        .config('spark.submit.deployMode', 'client') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.network.timeout", 100000) \
        .config('spark.ui.showConsoleProgress', True) \
        .config('spark.sql.repl.eagerEval.enabled', True) \
        .getOrCreate()

    ''' skip-gram '''
    prod_df = spark.read.parquet('data/parquet/prod2/').select(
        F.regexp_replace(
            F.lower(F.trim(F.col('prod_nm'))), '\s+', ' '
        ).alias('prod_nm')
    ).withColumn(
        "prod_nm_tkns",
        F.split(F.regexp_replace(F.lower(F.col('prod_nm')), ' ', ','), ",")
    ).withColumn(
        "target_word",
        F.explode(F.col('prod_nm_tkns'))
    ).distinct().alias("prod_df")

    skipgram = (prod_df
                .withColumn("skip_gram", get_skip_gram(F.col('prod_nm_tkns'), F.col('target_word'), F.lit(2)))
                .repartition(350)
                .alias("skipgram"))

    embd_lble = skipgram.select(
        F.col('prod_nm'),
        F.explode(F.col('skip_gram')).alias('embad_layer'),
        F.col('embad_layer')[0].alias('layer1'),
        F.col('embad_layer')[1].alias('layer2')
    ).where(
        (F.length(F.col('layer1')) > 1) &
        (F.length(F.col('layer2')) > 1)
    ).alias('embd_lble')
    # embd_lble.orderBy(F.col('prod_nm')).show(100, False)

    # negative sampling : 예측값 구하기, 레이블이 1인것만 가능, 0은 불가(일부만 샘플링하는것인데 일부의 기준 애매해서..)
    kor_eng_lble_frq = embd_lble \
        .withColumn('txt_type', get_txt_type(F.col('layer1'))) \
        .where(F.col('txt_type') == 'kor') \
        .groupBy(F.col('layer1'), F.col('layer2')) \
        .agg(F.count(F.col('layer2')).alias('cnt')) \
        .where(F.col('cnt') > 10) \
        .alias("kor_eng_lble_frq")

    # layer2 -> 상위 4개까지 리스트 형식 으로
    get_candidate = kor_eng_lble_frq \
        .withColumn(
            'prdt_val',
            F.round(F.log((F.col('cnt') / kor_eng_lble_frq.count())), 4)
        ).withColumn(
            'lyr2_rnk',
            F.rank().over(window.Window.partitionBy(F.col('layer1')).orderBy(F.col('prdt_val')))
        ).where(F.col('lyr2_rnk') < 6).alias('get_candidate')

    stop_word = spark.read.parquet("data/parquet/stop_word_1").alias('stop_word')
    except_stopword = get_candidate.join(
        stop_word,
        F.col('get_candidate.layer1') == F.col('stop_word.prod_tkn'),
        'leftanti'
    )

    (except_stopword.coalesce(20).write.format("parquet").mode("overwrite")
     .save("data/parquet/word2vec/skip_gram/cnadidate"))

    ''' Word2Vec library test (Only 벡터화 시켜주는 기능) '''
    # word2Vec = Word2Vec(vectorSize=20, seed=3, inputCol="layer1", outputCol="model")
    # word2Vec.setMaxIter(10)
    # model = word2Vec.fit(except_stopword)
    # model.getVectors().show(100, False)

    '''
        네거티브 샘플링 skip-gram(SGNS)
        중심단어 & 주변단어 매핑 해서 전체 확률 구해보기
        세트로 등장하는 단어 빈도수 잘라보면...?
        네거티브 샘플링의 목적은 타겟 단어와 연관성이 없을 것이라고 추정되는 단어를 뽑는 것
        negative sampling을 할 때는 더 자주 등장하는 단어일수록 연관성이 낮을 것
        
       분자 : 𝑓(𝑤𝑖) 는 해당 단어가 말뭉치에 등장한 비율(해당 단어 빈도/전체 단어수)
       분모 : 사실 중복을 허용한 전체 단어의 수
    '''
    # todo : 네거티브 샘플링 진행 하기
    
    skipgram.show(100, False)
    # # /usr/local/Cellar/hadoop/3.3.4/libexec/bin/hdfs

    # attr = spark.read.parquet("data/parquet/measures_attribution/")   # 속성 df
    # attr.show(100, False)

    exit(0)
