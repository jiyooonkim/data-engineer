# -*- coding: utf-8 -*-
"""
# title : 불용어 사전(전처리, 정제작업)
# desc :
- 노이즈 데이터 : 의미 없는 글자(특수 문자 등)을 의미하기도 하지만, 분석 목적에 맞지 않는 불필요 단어
- candidate : 적은 빈도, 짧은 단어, 형용사
# object :
 - 텍스트 유사도 비교(상품명 vs 송장명 , 상품명 vs 상품명 ... 등) 비교시 불필요한 단어때문에 유사도 상승 -> 불필요한 단어 제거 목적
 ex) 기타 재래도시락김5g72봉  - 기타 쌀,  기타 고정플레이트 - 기타 쌀  ->'기타' 제거 필요
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_spelling(word):
    # 초성 리스트. 00 ~ 18
    chosung_list = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']
    # 중성 리스트. 00 ~ 20
    jungsung_list = ['ㅏ', 'ㅐ', 'ㅑ', 'ㅒ', 'ㅓ', 'ㅔ', 'ㅕ', 'ㅖ', 'ㅗ', 'ㅘ', 'ㅙ', 'ㅚ', 'ㅛ', 'ㅜ', 'ㅝ', 'ㅞ', 'ㅟ', 'ㅠ', 'ㅡ', 'ㅢ',
                     'ㅣ']
    # 종성 리스트. 00 ~ 27 + 1(1개 없음)
    jongsung_list = [' ', 'ㄱ', 'ㄲ', 'ㄳ', 'ㄴ', 'ㄵ', 'ㄶ', 'ㄷ', 'ㄹ', 'ㄺ', 'ㄻ', 'ㄼ', 'ㄽ', 'ㄾ', 'ㄿ', 'ㅀ', 'ㅁ', 'ㅂ', 'ㅄ', 'ㅅ',
                     'ㅆ', 'ㅇ', 'ㅈ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']
    r_lst = []
    for w in list(word.strip()): 
        if '가' <= w <= '힣':
            # 588개 마다 초성이 바뀜.
            ch1 = (ord(w) - ord('가')) // 588
            # 중성은 총 28가지 종류
            ch2 = ((ord(w) - ord('가')) - (588 * ch1)) // 28
            ch3 = (ord(w) - ord('가')) - (588 * ch1) - 28 * ch2
            r_lst.extend([chosung_list[ch1], jungsung_list[ch2], jongsung_list[ch3]])
        else:
            r_lst.extend(w)
    return r_lst


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('jy_kim') \
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
    # 송장명 개수 : 21135
    shipping_df = spark.read.csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/송장명.csv") \
        .select(
        F.split(
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.lower(F.col('_c2')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' '
                )
            ), ' '
        ).alias('shipping_nm')
    ).withColumn('tkns', F.explode(F.col('shipping_nm'))) \
        .where(F.length(F.col('tkns')) > 1)

    ship_tkn_agg = shipping_df \
        .groupby(F.col('tkns')) \
        .agg(F.count(F.col('tkns')).alias('cnt')) \
        .withColumn('txt_type', F.col('tkns').cast("int").isNotNull()) \
        .where(F.col('txt_type') == False).alias('ship_tkn_agg')  # remove only number value,   cnt : 43987

    attr = spark.read.parquet('hdfs://localhost:9000/dictionary/measures_attribution/') \
        .select(F.col('shp_nm_token'), F.col('cnt').alias('attr_cnt')).alias('attr')
    # ship_tkn_agg.select(F.count(F.col('tkns'))).show()
    # res = attr.unionAll(ship_tkn_agg.select(F.col('tkns'), F.col('cnt')))

    # ship_tkn_agg.join(attr, F.col('tkns') == F.col('shp_nm_token'), 'leftanti').orderBy(F.col('cnt').desc()).show(1000, False)
    # ship_tkn_agg.where(F.col('tkns') == '1개').show(10, False)

    prod_1 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod.csv") \
        .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))

    prod_2 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod_2.csv") \
        .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))

    prod = (prod_1.union(prod_2)).distinct()
    l_cate = prod.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('대분류')), '/', ','), ",")).alias('cate'))
    m_cate = prod.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('중분류')), '/', ','), ",")).alias('cate'))
    s_cate = prod.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('소분류')), '/', ','), ",")).alias('cate'))
    cate = (l_cate.unionAll(m_cate).unionAll(s_cate)).distinct()
    '''
    step1 : 카테고리들 과 유사한 단어 모아보기 (단어 유사도 (초,중,종성 단위비교할것) by Noisy Channel Model)
    step2 : 카테고리 중심(=candidate correction)이되어 노이즈 키워드 카테고리에서 뽑아내길 바란다.
    step3 :
    '''

    candidate_correction = cate.withColumn('chojungjong', get_spelling(F.col('cate')))

    prod.show(1000, False)
    # prod.select(F.col('상품명')).where(F.col('상품명').like('%블루 %')).show(100, False)
    ''' 
    숫자 + 영어/ 숫자 + 한글 : 도량형 속성
    영어 + 숫자/ 숫자+영어 : 상품번호
    '''

    # todo : 2개 토큰씩 묶어서 카운트 해보기
