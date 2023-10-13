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
        ex) "삼성 도어락" 질의시, Ngram & Linked prediction 결과로 매핑된 후보 키워드들로 노출 가능
            ['삼성', '도어락']['삼성', 'SHP-DR700']['도어락', 'SHP-DR700']
    - 검색 필터 반영
            ex) "올세인츠" 질의시 검색필터 : 여성용, 남성용, 민소매, 긴소매, 가죽자켓, 원피스, 니트, S,M,L(사이즈)

    - 오타교정 : 철자 단위 ngram한다면, 앞뒤로 어떤 문자들이 많이 왔는가 -> 정타 사전 구축 가능 할듯 , 가지고 있는 오타가 많이 없으니
"""
import os
os.chdir('../../../')
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window
import re


@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def get_gram(token, n):
    lst = []
    for i in range(0, len(token)):
        if len(token[i:i+n]) == n:
            lst.append(token[i:i+n])
    return lst


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
                res = re.compile(u'[^a-z]+').sub(u'', wd)
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

@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def get_triple_token(tks):
    output_total = []
    for i in range(0, len(tks)):
        for j in range(i, len(tks)):
            for k in range(j, len(tks)):
                if (tks[i].__ne__(tks[j])) & (tks[j].__ne__(tks[k])) & (tks[k].__ne__(tks[i])):
                    output_total.append(([tks[i], tks[j], tks[k]]))
    return output_total


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
    '''
        step1. 상품 유사도 구한걸로 linked precition 해보기 
        step2. 결과 별로라면 카테고리 기반으로 해보기 
        ex)
            [여성,나이키], [운동화,여성]      예측결과물:  [나이키,운동화] 
    '''
    b = spark.read.parquet('data/parquet/tfidf/')\
        .withColumn("prod_nm_tkns", F.split(F.lower(F.trim(F.regexp_replace(F.col('prod_nm'), r" +", ' '))), " "))\
        .where(F.length(F.col('token')) > 1)\
        .withColumn("bi_gram", get_gram(F.col("prod_nm_tkns"), F.lit("2").cast(T.IntegerType())))\
        .withColumn("find_tkn", find_kwd_set(F.col('token'), F.col('bi_gram')))
    # b.where(F.col("prod_nm").like("%도어락%")).show(100, False)
    # .withColumn("txt_tp", get_txt_type(F.col("bi_gram"))) \
    # b.where(F.col('token').like('나이키')).select(F.col('token'), F.col('tf-idf'), F.col('bi_gram'),F.col('find_tkn'))\
    #     .orderBy(F.col('tf-idf')).show(100, False)

    # b.where(F.col('token') == '프리미엄').select(F.col('prod_nm'), F.col('token'), F.col('tf-idf'), F.explode(F.col('find_tkn')).alias('ngram_set')).orderBy(F.col('tf-idf')).show(100, False)
    # b.groupby(F.col('token')).agg(F.count(F.col('token')).alias('cnt')).orderBy(F.col('cnt').desc()).show(1000, False)
    '''
        가설 : 한글,영어 or 영어,한글 형식이면 
        trial: loan word 확률 ?? 얼마나??    
        result : 확률 없음 <<< loan_word.py 우세!!
            ex) 닌텐도, 나이키 등에 후보군 영어 후보군 없음!!
            +----------------+--------------+
            |toks            |tp            |
            +----------------+--------------+
            |[닌텐도, switch]|[[kor], [eng]]|
            |[닌텐도, wii]   |[[kor], [eng]]|
            |[닌텐도, epex]  |[[kor], [eng]]|
            |[나이키, jr]   |[[kor], [eng]]|
            |[나이키, sb]   |[[kor], [eng]]|
            |[나이키, nsw]  |[[kor], [eng]]|
            |[나이키, triax]|[[kor], [eng]]|
            |[나이키, w]    |[[kor], [eng]]
            +----------------+--------------+
    '''
    # # b.withColumn("aaaaaa", get_txt_type(F.col("find_tkn"))).show(100, False)
    # get_loan_wd_target = b.select(F.explode(F.col('find_tkn')).alias('toks')).withColumn('tp', get_txt_type(F.col("toks")))
    #
    # loan_wd_cndd1 = get_loan_wd_target.where(F.col('tp')[1][0] == 'kor').where(F.col('tp')[0][0] == 'eng')\
    #     .where(F.length(F.col('toks')[1]) < F.length(F.col("toks")[0]))
    # loan_wd_cndd1.write.format("parquet").mode("overwrite").save("/Users/jy_kim/Documents/private/nlp-engineer/data/parquet/loan_wd_cndd1_by_ngram/")
    #
    # loan_wd_cndd2 = get_loan_wd_target.where(F.col('tp')[1][0] == 'eng').where(F.col('tp')[0][0] == 'kor') \
    #     .where(F.length(F.col('toks')[1]) > F.length(F.col("toks")[0]))
    # loan_wd_cndd2.write.format("parquet").mode("overwrite").save("/Users/jy_kim/Documents/private/nlp-engineer/data/parquet/loan_wd_cndd2_by_ngram/")

    c = b\
        .select(F.col("token"), F.col("find_tkn"), F.col('tf-idf'), get_txt_type(F.col("find_tkn")).alias('tkn_tp'))\
        # .where(F.size(F.col('find_tkn')) > 1).where(F.col('tkn_tp')[1][1] != 'num')#.sample(0.5)
    # c.where(F.col("token") == '혜강씨큐리티').show(1000, False)
    '''
        todo : ['삼성', ' 도어락'] + ['삼성', 'SHP-DR700'] = ['도어락', 'SHP-DR700'] 형태 만들기!!
    '''
    get_couple_tokn = c.where(F.size(F.col('find_tkn')) == 2)\
        .withColumn("fst_end_1", F.array(F.col('find_tkn')[0][0], F.col('find_tkn')[1][1])).distinct().alias("get_couple_tokn")
    get_fst_end = get_couple_tokn.select(F.explode(F.col('find_tkn')).alias('fst_end_2')).distinct().alias("get_fst_end")

    # c = get_couple_tokn.join(
    #                             get_fst_end,
    #                             (
    #                                     (F.col("get_couple_tokn.fst_end_1") == F.col("get_fst_end.fst_end_2")) |
    #                                     (
    #                                         (F.col("get_couple_tokn.fst_end_1")[0] == F.col("get_fst_end.fst_end_2")[1])
    #                                         & (F.col("get_couple_tokn.fst_end_1")[1] == F.col("get_fst_end.fst_end_2")[0])
    #                                     )
    #                             )
    #                     ).select(F.col('token'), F.col('find_tkn'), F.col('fst_end_1')).distinct()
    # c.write.format("parquet").mode("overwrite").save("data/parquet/test/")

    '''
        <결과>
            - 연속된 단어들로만 보기때문에 정확도, 희소 문제 발생 
            - 중첩되는 토큰 중심으로 매핑되는 단어 찾기 힘듦 
            todo : 토크나이징 후 3개 토큰 묶음 생성      
                ex) 혜강씨큐리티 싱크 디지털도어락 SB500 => [혜강씨큐리티, 싱크, 디지털도어락], [혜강씨큐리티, 디지털도어락, SB500] .....
        <Linked prediction 기반 단어 예측>
         todo : 
            - N Size 
            - 불용어 제거
            - 구둣점, 특수문자 제거
    '''
    b = b.select(F.col('prod_nm'), F.col('prod_nm_tkns'), F.col('token'))\
        .where(F.length(F.col("token")) > 2).where(F.col("token") == '나이키')\
        .withColumn("linked", F.explode(get_triple_token(F.col("prod_nm_tkns"))))
    b.groupby(F.col('linked')).agg(F.count(F.col('linked')).alias('linked_cnt'))\
        .orderBy(F.col('linked_cnt').desc()).show(1000, False)

    exit(0)
    """
         모델명에 대한 상품 정보/특징 매핑 해보기 (색상, 상품명, 상품번호, 브랜드, 성별, 카테고리 등...)
    """
