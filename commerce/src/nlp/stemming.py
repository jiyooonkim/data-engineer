"""
    # title : 어간 추출(stemming)
    # desc : 단어에서 개념적(최소) 의미를 갖는 어간만 추출하는 방법
        ex) analysis과 analytic ->analy
        예시와 같이 어간만 추출하다보니 사전에 없는 단어가 생기게 됩니다.
        - Stemming의 목적은 어근과 차이가 있더라도 관련이 있는 단어들이 일정하게 동일한 어간으로 매핑되게 하는 것 목적
        어간 VS 표제어
            - 어간 : 사전에 미존재하는 단어 나올 수 있음, 문맥정보 없음
            - 표제어 : 기본 사전형 의미, 단어의 형태가 보존되어야 함, 문맥정보 있음
    # output
        -
"""

# import pyspark.sql.functions as F
# import pyspark.sql.types as T
# import pyspark.sql.window as window

import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

if __name__ == "__main__":
    from nlp import init_spark_session, F

    spark = init_spark_session()
    # print(os.getcwd())

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
                    F.lower(F.col('prod_nm')), "[^A-Za-z0-9가-힣-]", ' '
                ), r"\s+", ' '), ' '
            )
        ).alias('token')
    ).where(F.length(F.col('token')) > 1).distinct().alias('origin_df')
    '''
        # Todo 
        - 전처리 : 토크나이징, 특수문자, 소문자 변환
        - Stemming 방식
            - 단어 기반 포함된 알고리즘 찾는것 
            - 예상 output : 축이되는 단어 +  잘린단어 
    '''
    df_3_size = origin_df.where(F.length(F.col('token')) <= 3).alias('df_3_size')

    (df_3_size.join(origin_df, F.col('df_3_size.token').contains(F.col('origin_df.token')))
     .where(F.col('origin_df.token') != F.col('df_3_size.token')))
     # .show(10000, False))
    df_3_size.show(10000, False)
    # F.regexp_replace(F.lower(F.col('prod_nm')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' '))

    cp = spark.read.parquet('/Users/jy_kim/Documents/private/data-engineer/data/parquet/compound')
    # cp.orderBy(F.col('freq').desc()).show(1000, False)
    '''
    
    |2호세트                 |[세트, 2호]                 |1460|1  |
|3000세트                |[세트, 3000]                |1446|1  |
|300w스피커              |[스피커, 300w]              |144 |1  |
|32세트                  |[세트, 32]                  |1444|1  |
|34rc                    |[rc, 34]                    |71  |1  |
|35rc                    |[rc, 35]                    |90  |1  |
|3d선글라스              |[선글라스, 3d]              |211 |1  |
|3d스캐너                |[스캐너, 3d]                |193 |1  |
|3d영화                  |[영화, 3d]                  |127 |1  |
|3d퍼즐                  |[퍼즐, 3d]                  |183 |1  |
|3d프린터                |[프린터, 3d]                |217 |1  |
|3d피규어                |[피규어, 3d]                |'''

    exit(0)