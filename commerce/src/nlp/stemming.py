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
"""

import sys
from os import path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

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
    # .where(F.length(F.col('token')) > 1)
    # todo : 영어 +  숫자 ver A-Za-z0-9
    #
    '''
        # Todo 
        - 전처리 : 토크나이징, 특수문자, 소문자 변환
        - Stemming 방식
            - 단어 기반 포함된 알고리즘 찾는것 
            - 예상 output : 축이되는 단어 + 잘린단어 
 
    '''
    df_3_size = (origin_df.where((F.length(F.col('token')) <= 4))
                 .groupby(F.col('token')).agg(F.count(F.col('token')).alias('cnt'))
                 .repartition(400, F.col('cnt'))
                 .alias('df_3_size'))
    # df_3_size.select(F.count(F.col('token'))).show() & (F.length(F.col('token')) > 1)
    # origin_df = origin_df.distinct().alias('origin_df')
    kor_ver = (df_3_size
               .join(origin_df.distinct(), F.col('origin_df.token').contains(F.col('df_3_size.token')))
               .where(F.col('origin_df.token') != F.col('df_3_size.token'))
               .select(
                    F.col('origin_df.token').alias('word'),
                    F.col('df_3_size.token').alias('stem'),
                    F.col('df_3_size.cnt').alias('cnt'))
               .where(get_txt_type(F.col('word')) != 'num'))
    origin_df.select(F.count(F.col('token'))).show()
    df_3_size.select(F.count(F.col('token'))).show()
    kor_ver.show()

    kor_ver.write.format("parquet").mode("overwrite").save(
        "../../../data/output/stemming_kor/")
    # todo : 토큰 매핑 알고리즘 구현 , 안나올 경우 대비해 토큰 빈도수도 구해둠!!

    # todo : eng ver

    # .groupBy(F.col('stem')).agg(F.collect_list(F.col('word')))
    # df_3_size.join(origin_df, F.col('df_3_size.token').contains(F.col('origin_df.token'))).where(F.col('origin_df.token') != F.col('df_3_size.token')).show(10000, False)

    exit(0)
