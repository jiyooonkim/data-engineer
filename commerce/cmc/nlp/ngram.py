# -*- coding: utf-8 -*-
"""
# title : Ngram
# doc : https://heytech.tistory.com/343
# desc :
    - 다음 단어를 예측할 때 문장 내 모든 단어를 고려하지 않고 특정 단어의 개수 N개만 고려
    - N 개의 연속적인 단어의 나열을 하나의 묶음(=token)으로 간주
    ex) "오늘 점심 추천 메뉴: 파스타, 피자" 경우,
        Unigram(N=1)	오늘, 점심, 추천, 메뉴, 파스타, 피자
        Bigram(N=2)	오늘 점심, 점심 추천, 추천 메뉴, 메뉴 파스타, 파스타 피자
        Trigram(N=3)	오늘 점심 추천, 점심 추천 메뉴, 추천 메뉴 파스타, 메뉴 파스타 피자
        4-gram(N=4)	오늘 점심 추천 메뉴, 점심 추천 메뉴 파스타, 추천 메뉴 파스타 피자

    - 한계점
        - 정확도 : N개 연속된 단어만 고려하기 떄문에 문장의 맥락이 안맞을 수 있음
        - 희소(sparsity) : N개의 단어를 연속적으로 갖는 문장자체가 드물다
        - 상충(trade-off) : N값의 크기(너무 크거나작거나), N=5를 권장, 희소문제 연관성
# available :
    - Inner keyword 후보로 사용(상품명에 없는 키워드라도 관련 있는 키워드는 사용가능)
        ex) "삼성 도어락" 질의시, Ngram&Linked prediction 결과로 매핑된 후보 키워드들로 노출 가능
            ['삼성', ' 도어락']['삼성', 'SHP-DR700']['도어락', 'SHP-DR700']
# todo : 외래어 후보 (2개 타입) 구한것 다시 해보기
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def get_gram(token, n):
    lst = []
    for i in range(0, len(token)):
        if len(token[i:i+n]) == n:
            lst.append(token[i:i+n])
    return lst


@F.udf(returnType=T.StringType())
def get_token_type(col):
    val = ''
    if col.encode().isalpha():  # only eng
        val = 'eng'
    if col.isalpha():  # eng+kor, kor
        val = 'kor'
    if col.isdigit():   # 숫자만
        val = 'num'
    # isalnum() # 영어/한글 + 숫자
    return val


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def get_txt_type(col):  # txt type in list
    lst = []
    for sub_lst in col:
        s_lst = []
        sub_lst = str(sub_lst).replace('[', '').replace(']', '').replace("'", "").split(', ')
        for wd in sub_lst:
            if wd.encode().isalpha():  # only eng
                s_lst.append('eng')
            elif wd.isalpha():  # eng+kor, kor
                import re
                # reg = re.compile(r'[가-힣a-zA-Z]')
                res = re.compile(u'[^a-z]+').sub(u'', wd)
                # res = re.compile(r'[가-힣a-zA-Z]').sub(u'', res)
                # res = reg.sub(u'', res)
                if res:     # kor + eng
                    s_lst.append('engkor')
                else:       # only kor
                    s_lst.append('kor')

            elif wd.isdigit():   # 숫자만
                s_lst.append('num')
            elif wd.isalnum():   # 영어/한글 + 숫자
                s_lst.append('txtnum')
            else:
                s_lst.append('etc')
        lst.append(s_lst)
    return lst


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def find_kwd_set(token, lst):
    # 토큰이 포함된 ngram set 찾기
    # input ex=> token::나이키    lst::[[런닝화, 1개], [1개, 나이키], [나이키, 다운시프터12], [다운시프터12, 런닝화], [런닝화, dd9293]]
    rtn_lst = []
    for i in lst:
        for kwd in i:
            if kwd == token:
                rtn_lst.append(i)
    return rtn_lst


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

    prod_1 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod.csv") \
        .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))

    prod_2 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod_2.csv") \
        .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))

    prod = (prod_1.union(prod_2))\
        .select(F.col('상품명').alias('prod_nm'), F.split(F.lower(F.col('prod_nm')), " ").alias("prod_nm_tkns"))\
        .withColumn("bi_gram", get_gram(F.col("prod_nm_tkns"), F.lit("2").cast(T.IntegerType())))\
        .withColumn("tri_gram", get_gram(F.col("prod_nm_tkns"), F.lit("3").cast(T.IntegerType())))

    '''
        <Linked prediction단어 예측>
         - Cosideration 
            - N Size 
            - 불용어 제거
            - 구둣점, 특수문자 제거 
         - scalability
            - 오타교정 : 철자 단위 ngram한다면, 앞뒤로 어떤 문자들이 많이 왔는가 -> 정타 사전 구축 가능 할듯 , 가지고 있는 오타가 많이 없으니
            - Loan word : [English, Korean, English ..] 형 
    '''
    '''
        step1. 상품 유사도 구한걸로 linked precition 해보기 
        step2. 결과 별로라면 카테고리 기반으로 해보기 
        ex)
            [여성,나이키], [운동화,여성]      예측결과물:  [나이키,운동화] 
    '''

    b = spark.read.parquet('/Users/jy_kim/Documents/private/nlp-engineer/data/parquet/tfidf/')\
        .withColumn("prod_nm_tkns", F.split(F.lower(F.trim(F.regexp_replace(F.col('prod_nm'), r" +", ' '))), " "))\
        .where(F.length(F.col('token')) > 1)\
        .withColumn("bi_gram", get_gram(F.col("prod_nm_tkns"), F.lit("2").cast(T.IntegerType())))\
        .withColumn("find_tkn", find_kwd_set(F.col('token'), F.col('bi_gram')))

    # .withColumn("txt_tp", get_txt_type(F.col("bi_gram"))) \
    # b.where(F.col('token').like('나이키')).select(F.col('token'), F.col('tf-idf'), F.col('bi_gram'),F.col('find_tkn'))\
    #     .orderBy(F.col('tf-idf')).show(100, False)

    # b.where(F.col('token') == '프리미엄').select(F.col('prod_nm'), F.col('token'), F.col('tf-idf'), F.explode(F.col('find_tkn')).alias('ngram_set')).orderBy(F.col('tf-idf')).show(100, False)
    # b.groupby(F.col('token')).agg(F.count(F.col('token')).alias('cnt')).orderBy(F.col('cnt').desc()).show(1000, False)
    '''
        가설 : 한글,영어 or 영어,한글 형식이면 
        todo : loan word 확률 ?? 얼마나??  
    '''
    # b.withColumn("aaaaaa", get_txt_type(F.col("find_tkn"))).show(100, False)
    get_loan_wd_target = b.select(F.explode(F.col('find_tkn')).alias('toks')).withColumn('tp', get_txt_type(F.col("toks")))

    loan_wd_cndd1 = get_loan_wd_target.where(F.col('tp')[1][0] == 'kor').where(F.col('tp')[0][0] == 'eng')\
        .where(F.length(F.col('toks')[1]) < F.length(F.col("toks")[0]))
    loan_wd_cndd1.write.format("parquet").mode("overwrite").save("/Users/jy_kim/Documents/private/nlp-engineer/data/parquet/loan_wd_cndd1_by_ngram/")

    loan_wd_cndd2 = get_loan_wd_target.where(F.col('tp')[1][0] == 'eng').where(F.col('tp')[0][0] == 'kor') \
        .where(F.length(F.col('toks')[1]) > F.length(F.col("toks")[0]))
    loan_wd_cndd2.write.format("parquet").mode("overwrite").save("/Users/jy_kim/Documents/private/nlp-engineer/data/parquet/loan_wd_cndd2_by_ngram/")

    c = b.where((F.col('token') == '골프화') | (F.col('token') == '나이키'))\
        .select(F.col("token"), F.col("find_tkn"), F.col('tf-idf'), get_txt_type(F.col("find_tkn")).alias('tkn_tp'))\
        .where(F.size(F.col('find_tkn')) > 1).where(F.col('tkn_tp')[0][1] == 'kor').where(F.col('tkn_tp')[1][1] != 'num')\
        .show(1000, False)



    exit(0)