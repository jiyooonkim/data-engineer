"""
    # title : 어간 추출(stemming)
    # desc : - 단어에서 개념적(최소) 의미를 갖는 어간만 추출하는 방법
                ex) analysis과 analytic ->analy
                    예시와 같이 어간만 추출하다보니 사전에 없는 단어 발생하기도 함
            - Stemming의 목적은 어근과 차이가 있더라도 관련이 있는 단어들이 일정하게 동일한 어간으로 매핑되게 하는 것 목적
            - 어간 VS 표제어
                - 어간 : 사전에 미존재하는 단어 나올 수 있음, 문맥정보 없음
                - 표제어 : 기본 사전형 의미, 단어의 형태가 보존되어야 함, 문맥정보 있음
            -
    # output
        -

    # solution
        1. 최소한의 의미 갖는 단위로 나눈뒤 어간을 보자

"""

import sys
from os import path
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))


# @F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
@F.udf(returnType=T.ArrayType((T.StringType())))
def get_tokns(tkn_list, word):
    total = []
    for i in range(0, len(tkn_list)):
        tkn_cndd = []
        max_len_txt = ''
        for j in range(0, len(tkn_list)):
            if tkn_list[j][1] != (tkn_list[i][1]):
                if tkn_list[j][1].__contains__(tkn_list[i][1]):
                    if len(max_len_txt) <= len(tkn_list[j][1]):
                        # if tkn_list[i][0] < tkn_list[j][0]:
                        max_len_txt = tkn_list[j][1]
                    tkn_cndd.append(tkn_list[j][1])
        if max_len_txt != "":
            word = word.strip(max_len_txt)
            total.append(max_len_txt)
    if word != "":
        total.append(word)

    return list(set(total))


@F.udf(returnType=T.ArrayType((T.StringType())))
def sub_tokenize(cndds, tkn_list):
    sub_tokens = []
    for i in cndds:     # ['사각', '연필꽂이']
        for j in tkn_list:      #
            if i.__ne__(j):
                if (len(i) > len(j)) & (len(j[1]) > 1):
                    if i[0:len(j[1])] == j[1]:
                        sub_tokens.append(i[0:len(j[1])])
                    elif i[len(i) - len(j[1]):] == j[1]:
                        sub_tokens.append(i[len(i) - len(j[1]):])
    return list(set(cndds + sub_tokens))


if __name__ == "__main__":
    from nlp import init_spark_session, F, get_txt_type

    spark = init_spark_session()

    # Step1. get product name
    df1 = (spark.read.option('header', True)
           .csv('../../../commerce/data/송장명.csv')
           .select(F.col('_c2').alias('prod_nm')))
    df2 = (spark.read
           .csv('../../../commerce/data/nike_data.csv')
           .select(F.col('_c2').alias('prod_nm')))
    df3 = (spark.read.option('header', True)
           .csv('../../../commerce/data/nvr_prod.csv')
           .select(F.col('상품명').alias('prod_nm')))
    origin_df = df1.union(df2).union(df3).select(
        F.explode(
            F.split(
                F.regexp_replace(F.regexp_replace(
                    F.lower(F.col('prod_nm')), "[^가-힣0-9-*&]", ' '
                ), r"\s+", ' '), ' '
            )
        ).alias('token')
    ).where(F.length(F.col('token')) != 0).alias('origin_df')
    #  = df1.union(df2).union(df3).select(
    #         F.col('prod_nm'),
    #         F.split(
    #             F.regexp_replace(F.regexp_replace(
    #                 F.lower(F.col('prod_nm')), "[^가-힣0-9-*&]", ' '
    #             ), r"\s+", ' '), ' '
    # 
    #     ).alias('token')
    # )
    # eeee.where(F.col('prod_nm').contains("브레이브")).show(10000, False)

    # todo : 영어 +  숫자 ver A-Za-z0-9
    '''
        # Todo
        - 전처리 : 토크나이징, 특수문자, 소문자 변환
        - Stemming 방식
            - 단어 기반 포함된 알고리즘 찾는것
            - 예상 output : 축이되는 단어 + 잘린단어
        - 문제점
            - 서브워드 토크나이징    ex) 사운드바 -> 사운드 + 바
            - 등장빈도수 우선순위 적용시 ex)  브레이브복싱글러브 ->  ['브레이브', '복싱글', '글러브'] , 복싱 과 글러브 의 추출 방안은?
            - 빈도수 우선순위 대체제는?

        - 완벽한 토크나이징은 없는 듯, 다양한 토크나이저들이 있지만 완벽히 해내는 것은 없느듯
        서브워드 토크나이징 처리 불가, 부정확한 토크나이징 등... 현상들이 많다.
        최적의 해결 방안은 ??
    '''
    df_3_size = (origin_df.where((F.length(F.col('token')) <= 4))
                 .groupby(F.col('token')).agg(F.count(F.col('token')).alias('cnt'))
                 .repartition(400, F.col('cnt'))
                 .alias('df_3_size'))
    # df_3_size.select(F.count(F.col('token'))).show() & (F.length(F.col('token')) > 1)
    # origin_df = origin_df.distinct().alias('origin_df')
    kor_ver = ((df_3_size
               .join(origin_df.distinct(), F.col('origin_df.token').contains(F.col('df_3_size.token')))
               .where(F.col('origin_df.token') != F.col('df_3_size.token'))
               .select(
                    F.col('origin_df.token').alias('word'),
                    F.col('df_3_size.token').alias('stem'),
                    F.col('df_3_size.cnt').alias('cnt'))
               .where((get_txt_type(F.col('word')) != 'num') & (get_txt_type(F.col('word')) != 'etc')))
               .where(F.col('cnt') > 7))



    # (kor_ver.withColumn("rnk", F.rank().over(Window.partitionBy(F.col('word')).orderBy(F.col('stem').desc())))
    #  .show(1000, False))
    a = (kor_ver
        .groupby(F.col('word'))
        .agg(
            F.array_sort(
                F.collect_list(
                    F.array(
                        F.col('cnt'), F.col('stem')
                    )
                )
            ).alias('lst')
        ).withColumn("cnd", get_tokns(F.col('lst'), F.col('word')))
         .withColumn("sub_cnd", sub_tokenize(get_tokns(F.col('lst'), F.col('word')), F.col('lst')))
         )
    a.where(F.size(F.col('cnd')) > 0).sample(0.6).show(10000, False)
    a.printSchema() 
  
    exit(0)
