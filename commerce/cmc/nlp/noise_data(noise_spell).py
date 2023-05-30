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


def get_match_length(crr_wd, cndd_wd):
    # step1. 길이 맞추기
    if len(crr_wd) > len(cndd_wd):
        for i in range(0, len(crr_wd) - len(cndd_wd)):
            cndd_wd.append("nn")
    else:
        for i in range(0, len(cndd_wd) - len(crr_wd)):
            crr_wd.append("nn")
    return crr_wd , cndd_wd


@F.udf(returnType=T.ArrayType(T.StringType()))
# @F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType()), T.ArrayType(T.StringType()), T.ArrayType(T.StringType())))
def get_err_type(crr_wd, cndd_wd):
    crr_txt = []
    err_txt = []
    posision = []
    tp = ''
    
    if len(crr_wd) > len(cndd_wd):
        tp = 'deletion'
        crr_wd, cndd_wd = get_match_length(crr_wd, cndd_wd)
        for i in range(0, len(crr_wd)):
            if cndd_wd[i] != crr_wd[i]:
                crr_txt.append(crr_wd[i])
                cndd_wd.insert(i, crr_wd[i])
                posision.append(i)
        
    if len(crr_wd) < len(cndd_wd):
        tp = 'insertion'
        crr_wd, cndd_wd = get_match_length(crr_wd, cndd_wd)
        for i in range(0, len(crr_wd)):
            if cndd_wd[i] != crr_wd[i]:
                err_txt.append(cndd_wd[i])
                crr_wd.insert(i, cndd_wd[i])
                posision.append(i)

    # step2-3. substitution, transposition(제외)
    else:
        tp = 'substitution'
        for i in range(0, len(crr_wd)): 
            if cndd_wd[i] != crr_wd[i]:
                crr_txt.append(crr_wd[i])
                err_txt.append(cndd_wd[i])
                posision.append(i)
    return [crr_txt, err_txt, posision, tp]
 

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
    # ship_tkn_agg.show(10000, False)

    prod_1 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod.csv") \
        .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))

    prod_2 = spark.read. \
        option('header', True). \
        csv("/Users/jy_kim/Documents/private/nlp-engineer/commerce/data/nvr_prod_2.csv") \
        .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))

    prod = (prod_1.union(prod_2)).distinct()
    prod_nm = prod.select(
        F.explode(
            F.split(
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(F.lower(F.col('상품명')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' '
                    )
                ), ' '
            )
        ).alias('prod_nm')
    ).where(
        F.length(F.col('prod_nm')) > 1
    ).withColumn('prod_tokens', get_spelling(F.col('prod_nm'))).alias('prod_nm')
    # prod_nm.show(1000)
    l_cate = prod.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('대분류')), '/', ','), ",")).alias('cate'))
    m_cate = prod.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('중분류')), '/', ','), ",")).alias('cate'))
    s_cate = prod.select(F.explode(F.split(F.regexp_replace(F.lower(F.col('소분류')), '/', ','), ",")).alias('cate'))
    cate = (l_cate.unionAll(m_cate).unionAll(s_cate)).distinct().alias('cate')
    '''
        step1 : 카테고리들 과 유사한 단어 모아보기 (단어 유사도 (초,중,종성 단위비교할것) by Noisy Channel Model)
        step2 : 카테고리 중심(=candidate correction)이되어 노이즈 키워드 카테고리에서 뽑아내길 바란다.
        부작용 : 카테고리는 한글만 존재 -> 영어, 숫자 섞인 단어는 매칭 불가 할 수도 ...?
        
        데이터프레임구할땐,
        prod 가 맞다는 가정하에 -> category 엔트리가 작기 때문에 join 하는데 비용이 적게 들것 이라 생각 
        마지막에 category groupby , collect list 써서 후보 리스트 만들것  
    '''

    cate = cate.withColumn('cate_tokens', get_spelling(F.col('cate')))
    get_word_cnt = prod_nm\
        .groupby(F.col('prod_nm'), F.col('prod_tokens'))\
        .agg(F.count(F.col('prod_nm')).alias('count_w'))\
        .withColumn('p_w', F.round(F.col('count_w') / prod_nm.count(), 9))\
        .alias('get_word_cnt')
    # get_word_cnt.join(cate, F.col('') != F.col(''))

    # get_word_cnt.select(F.count(F.col('prod_nm'))).show()   # 95873
    # cate.select(F.count(F.col('cate'))).show()  # cnt : 2043
    
    # todo : 적당한 후보 매핑이 필요함 , 워딩 별로 유사도 매겨서 get_err_type 호출할것 
    aa = get_word_cnt\
        .join(F.broadcast(cate))\
        .where(F.col('prod_nm') != F.col('cate'))\
        # .withColumn('err_tp', get_err_type(F.col('prod_tokens'), F.col('cate_tokens')))
    aa.show(100, False)

    # prod_nm.show(1000, False)
    # prod.select(F.col('상품명')).where(F.col('상품명').like('%블루 %')).show(100, False)
    ''' 
        숫자 + 영어/ 숫자 + 한글 : 도량형 속성
        영어 + 숫자/ 숫자+영어 : 상품번호
    '''

    # todo : 2개 토큰씩 묶어서 카운트 해보기
