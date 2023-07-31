# -*- coding: utf-8 -*-
"""
# title : 외래어(Loan Word) 추출
# desc :
    - 외래어란? 고유어가 아닌 외국에서 들여와 자국어처럼 사용하는 말
    - 예시 : 아디다스(adidas), 나이키(nike), 맥도널드(macdonald) ...
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_konglish(kor_txt):
    ja = {'ㄱ': ['K','G'], 'ㄲ': ['KK','GG'], 'ㄴ': 'N', 'ㄷ': 'D', 'ㄸ': 'D', 'ㄹ': 'R', 'ㅁ': 'M', 'ㅂ': 'B',
          'ㅃ': 'B', 'ㅅ': ['S', 'TH'], 'ㅆ': 'SS', 'ㅈ': 'J', 'ㅉ': 'J', 'ㅊ': 'C', 'ㅌ': 'T', 'ㅍ': 'P', 'ㅎ': ['H', 'WH'],
          'ㅋ': ['C','K', 'CH']}
    mo = {'ㅑ': 'Y', 'ㅕ': ['Y', 'TI'], 'ㅛ': 'Y', 'ㅠ': ["Y", "U"], 'ㅖ': 'Y',
          'ㅝ': 'W', 'ㅘ': 'W', 'ㅙ': 'W', 'ㅚ': 'W', 'ㅜ': 'W', 'ㅞ': 'W', 'ㅟ': 'W',
          'ㅔ': 'E', 'ㅡ': 'E', 'ㅢ': 'E',
          'ㅏ': 'A', 'ㅐ': 'A', 'ㅓ': 'U', 'ㅗ': 'O', 'ㅣ': 'I'}
    r_lst = []
    for i, w in enumerate(kor_txt):
        lst = []
        # print(" i : ", i)
        # print(" w : ", w)
        if w[0] in ja.keys():   # 초성
            if list(ja.keys()):
                lst.extend(ja[w[0]])
            else:
                lst.append(ja[w[0]])
        else:
            lst.append(' ')

        if w[1] in mo.keys():     # 중성
            if list(mo.keys()):
                lst.extend(mo[w[1]])
            else:
                lst.append(mo[w[1]])
        else:
            if list(ja.keys()):
                lst.extend(ja[w[0]])
            else:
                lst.append(' ')
        if w[2] in ja.keys():     # 종성
            # lst.append(ja.keys())
            lst.append(ja[w[2]])
        else:
            lst.append(' ')
        r_lst.append(lst)
    # for i, w in enumerate(kor_txt):
    #     if w[0] in ja.keys():  # 초성
    #         r_lst.append(ja[w[0]])
    #     if w[1] in mo.keys():  # 중성
    #         r_lst.append(mo[w[1]])
    #     if w[2] in ja.keys():  # 종성
    #         r_lst.append(ja[w[2]])
    #     else:
    #         r_lst.append(' ')
    return r_lst


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_jaso(txt):
    # 초성 리스트. 00 ~ 18
    CHOSUNG_LIST = ['ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']
    # 중성 리스트. 00 ~ 20
    JUNGSUNG_LIST = ['ㅏ', 'ㅐ', 'ㅑ', 'ㅒ', 'ㅓ', 'ㅔ', 'ㅕ', 'ㅖ', 'ㅗ', 'ㅘ', 'ㅙ', 'ㅚ', 'ㅛ', 'ㅜ', 'ㅝ', 'ㅞ', 'ㅟ', 'ㅠ', 'ㅡ', 'ㅢ',
                     'ㅣ']
    # 종성 리스트. 00 ~ 27 + 1(1개 없음)
    JONGSUNG_LIST = [' ', 'ㄱ', 'ㄲ', 'ㄳ', 'ㄴ', 'ㄵ', 'ㄶ', 'ㄷ', 'ㄹ', 'ㄺ', 'ㄻ', 'ㄼ', 'ㄽ', 'ㄾ', 'ㄿ', 'ㅀ', 'ㅁ', 'ㅂ', 'ㅄ', 'ㅅ',
                     'ㅆ', 'ㅇ', 'ㅈ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']

    r_lst = []
    for w in list(txt.strip()):
        if '가' <= w <= '힣':
            ch1 = (ord(w) - ord('가')) // 588
            ch2 = ((ord(w) - ord('가')) - (588 * ch1)) // 28
            ch3 = (ord(w) - ord('가')) - (588 * ch1) - 28 * ch2
            r_lst.append([CHOSUNG_LIST[ch1], JUNGSUNG_LIST[ch2], JONGSUNG_LIST[ch3]])
        else:
            r_lst.append([w])
    return r_lst


@F.udf(returnType=T.StringType())
def convert_kor_to_initial(kor_txt):
    w_to_k = {'ㄱ': 'K', 'ㄲ': 'G', 'ㄴ': 'N', 'ㄷ': 'D', 'ㄸ': 'D', 'ㄹ': 'R', 'ㅁ': 'M', 'ㅂ': 'B', 'ㅂ': 'V',
              'ㅃ': 'B', 'ㅅ': 'S', 'ㅅ': 'TH', 'ㅆ': 'SS', 'ㅈ': 'J', 'ㅉ': 'JJ', 'ㅊ': 'C', 'ㅋ': 'K',
              'ㅋ': 'C', 'ㅌ': 'T', 'ㅍ': 'P', 'ㅎ': 'H'}
    r_lst = []
    # todo : 'ㅋ' : K or C ???   => kc....?

    for i, w in enumerate(kor_txt):
        if w[0] in w_to_k.keys():
            r_lst.append(w_to_k[w[0]])
        else:
            if w[1] in ['ㅑ', 'ㅕ', 'ㅛ', 'ㅠ', 'ㅖ']:
                r_lst.append('Y')
            elif w[1] in ['ㅝ', 'ㅘ', 'ㅙ', 'ㅚ', 'ㅜ', 'ㅞ', 'ㅟ']:
                r_lst.append('W')
            elif w[1] in ['ㅔ', 'ㅡ', 'ㅢ']:
                r_lst.append('E')
            elif w[1] in ['ㅏ', 'ㅐ']:
                r_lst.append('A')
            elif w[1] in ['ㅓ']:
                r_lst.append('U')
            elif w[1] in ['ㅗ']:
                r_lst.append('O')
            elif w[1] in ['ㅣ']:
                if i == 0:
                    r_lst.append('L')
                else:
                    r_lst.append('I')
            else:
                return 'not applica'
    return ''.join(r_lst).lower()


@F.udf(returnType=T.DoubleType())
def get_jaccard_sim(str1, str2):
    # set 이유 : 중복성 무시
    a = set(str2)
    b = set(str1)
    itc = float(len(set(a).intersection(set(b))))      # 분자
    union = len(a) + len(b) - itc    # 분모
    return 0 if union == 0 else itc/union


# @F.udf(returnType=T.ArrayType(T.StringType()))
@F.udf(returnType=T.StringType())
def get_intersection_word(kong, eng):
    kong = list(kong) if type(kong) == list else list(kong)  # 2차원 리스트
    eng = list(eng) if type(eng) == list else list(eng)
    cndd = []
    for k in range(0, len(eng)):
        for i in range(0, len(kong)):
            for j in range(0, len(kong[i])):
                if kong[i][j].lower() == eng[k]:
                    cndd.append(kong[i][j])
                    eng[k] = "-"

    return "".join(cndd).lower() #cndd


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('jy_kim') \
        .master('local[4]') \
        .config('spark.sql.execution.arrow.pyspark.enabled', True) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.driver.memory', '32g') \
        .config('spark.driver.cores', '8') \
        .config('spark.executor.memory', '16g') \
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

    prod = (prod_1.union(prod_2)).distinct()
    prod_nm = prod.select(
        F.lower(F.col('상품명')).alias('prod_nm'),
        F.explode(
            F.split(
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(F.lower(F.col('상품명')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' '
                    )
                ), ' '
            )
        ).alias('prod_nm_token')
    ).withColumn(
        "only_kor",
        ~F.col("prod_nm_token").rlike("[^ㄱ-힣]")
    ).withColumn(
        "only_eng",
        ~F.col("prod_nm_token").rlike("[^a-z]")
    ).alias('prod_nm')

    '''
        step1. 상품명 토크나이징 후 하위 dataframe 생성
        대상1     |   후보들
        kor      |   eng
    '''
    get_kor_tkn = prod_nm \
        .where(F.col('only_kor') == True) \
        .groupby(
        F.col('prod_nm_token'),
        F.col('only_kor')
    ).agg(
        F.collect_list(F.col('prod_nm')).alias('prod_nms')
    ).where(
        (F.size(F.col('prod_nms')) > 1)
    ).alias('get_kor_tkn')

    get_kor_tkn = get_kor_tkn.select(F.col('prod_nm_token'), F.col('only_kor'),
                                     F.explode(F.col('prod_nms')).alias('prod_nm')).alias('get_kor_tkn')
    c = get_kor_tkn.select(
        F.col('prod_nm'),
        F.col('prod_nm_token'),
        F.explode(
            F.split(
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(F.lower(F.col('prod_nm')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' '
                    )
                ), ' '
            )
        ).alias('prod_nm_token_cndd'),
        (~F.col("prod_nm_token_cndd").rlike("[^a-z]")).alias("only_eng_cndd"),
        (~F.col("prod_nm_token_cndd").rlike("[^가-힣]")).alias("only_kor_cndd"),
    ).where(F.length(F.col('prod_nm_token_cndd')) > 3)  # .sample(0.5)

    # 후보군들이 영어인것만 매칭
    get_cndd_eng = c \
        .where(F.col('only_eng_cndd') == True) \
        .groupby(F.col('prod_nm_token'), F.col('prod_nm_token_cndd')) \
        .agg(F.count(F.col('prod_nm_token_cndd')).alias('cnt')) \
        .where(F.length(F.col('prod_nm_token')) < F.length(F.col('prod_nm_token_cndd'))) \
        .where(F.length('prod_nm_token') > 1) \
        .alias('get_cndd_eng')

    # 카테고리성 키워드 제거
    get_cate = (
        (prod.select(F.explode(F.split(F.col('대분류'), "/")))). \
            union(prod.select(F.explode(F.split(F.col('중분류'), "/")))). \
            union(prod.select(F.explode(F.split(F.col('소분류'), "/"))))
    ).distinct().alias('get_cate')

    except_cate = get_cndd_eng.join(get_cate, F.col('prod_nm_token') == F.col('col'), 'leftanti').alias("except_cate")
    # except_cate.show(1000, False)

    '''
        step2. 상품명 토크나이징 후 하위 dataframe 생성 (step1 과 반대)
        대상1     |   후보들
        eng      |   kor
    '''
    get_eng_tkn = prod_nm \
        .where(F.col('only_eng') == True) \
        .groupby(
        F.col('prod_nm_token'),
        F.col('only_eng')
    ).agg(
        F.collect_list(F.col('prod_nm')).alias('prod_nms')
    ).where(
        (F.size(F.col('prod_nms')) > 1)
    ).select(
        F.col('prod_nm_token'),
        F.col('only_eng'),
        F.explode(F.col('prod_nms')).alias('prod_nm')
    ).alias('get_eng_tkn')

    ca = get_eng_tkn.select(
        F.col('prod_nm'),
        F.col('prod_nm_token'),
        F.explode(
            F.split(
                F.trim(
                    F.regexp_replace(
                        F.regexp_replace(F.lower(F.col('prod_nm')), "[^A-Za-z0-9가-힣]", ' '), r"\s+", ' '
                    )
                ), ' '
            )
        ).alias('prod_nm_token_cndd'),
        (~F.col("prod_nm_token_cndd").rlike("[^a-z]")).alias("only_eng_cndd"),
        (~F.col("prod_nm_token_cndd").rlike("[^가-힣]")).alias("only_kor_cndd"),
    ).where(F.length(F.col('prod_nm_token_cndd')) > 3)  # .sample(0.5)
    '''
        ver1. step1 과 step2 에서 만든 df inner join 으로 바운더리 축소 후 빈도수로 자르기
            => 결과 : 한국어와 영어 동시 등장한 빈도수 상당히 작음(1 or 2..) 적용 불가 
    '''
    # 후보군들이 한글인것만 매칭
    get_cndd_kor = ca \
        .where(F.col('only_kor_cndd') == True) \
        .groupby(F.col('prod_nm_token'), F.col('prod_nm_token_cndd')) \
        .agg(F.count(F.col('prod_nm_token_cndd')).alias('cnt')) \
        .where(F.length(F.col('prod_nm_token')) >= F.length(F.col('prod_nm_token_cndd'))) \
        .where(F.length('prod_nm_token') > 2) \
        .alias('get_cndd_kor')
    get_cndd_kor = get_cndd_kor \
        .join(get_cate, F.col('prod_nm_token') == F.col('col'), 'leftanti').alias("get_cndd_kor")

    get_cndd_kor.write.format("parquet").mode("overwrite").save(
        "/Users/jy_kim/Documents/private/nlp-engineer/data/parquet/loan_word_cndd_kor/")
    except_cate.write.format("parquet").mode("overwrite").save(
        "/Users/jy_kim/Documents/private/nlp-engineer/data/parquet/loan_word_cndd_eng/")

    aa = get_cndd_kor \
        .join(except_cate, [F.col('get_cndd_kor.prod_nm_token') == F.col("except_cate.prod_nm_token_cndd"),
                            F.col('get_cndd_kor.prod_nm_token_cndd') == F.col("except_cate.prod_nm_token")], 'inner') \
        .select(
            F.col('get_cndd_kor.prod_nm_token').alias("prod_nm_token_1"),
            F.col('get_cndd_kor.prod_nm_token_cndd').alias("prod_nm_token_cndd_1"),
            F.col('get_cndd_kor.cnt').alias("cnt_1"),
            F.col('except_cate.prod_nm_token').alias("prod_nm_token_2"),
            F.col('except_cate.prod_nm_token_cndd').alias("prod_nm_token_cndd_2"),
            F.col('except_cate.cnt').alias("cnt_2"),
        ).withColumn("rnk", F.rank().over(window.Window.partitionBy(F.col('prod_nm_token_cndd_2')).orderBy(F.col('cnt_2').desc())))\
        .where(F.col('rnk') < 3)
    # .where(F.col("prod_nm_token_cndd_1") != F.col("prod_nm_token_2"))

    '''
        ver2. 
            - step1 과 step2 결과물에 초,중,종성 분리로 phonics 적용 
            - 파닉스 결과(자음만) join 영어 토큰 알파벳 => 자카드 유사도(intersection)
            - 결과 : 순서 보장 필요, 모음포함으로 구해보기 
    '''
    a = get_cndd_kor\
        .withColumn("jaso", get_jaso(F.col("prod_nm_token_cndd")))\
        .withColumn("initial", convert_kor_to_initial(F.col("jaso")))\
        .withColumn("konglish", get_konglish(F.col("jaso")))\
        .withColumn("initianl_jcd_sim", get_jaccard_sim(F.col("initial"), F.col("prod_nm_token")))\
        .withColumn("intersection_word", get_intersection_word(F.col("konglish"), F.col("prod_nm_token")))

    b = except_cate\
        .withColumn("jaso", get_jaso(F.col("prod_nm_token")))\
        .withColumn("initial", convert_kor_to_initial(F.col("jaso"))) \
        .withColumn("konglish", get_konglish(F.col("jaso"))) \
        .withColumn("intersection_word", get_intersection_word(F.col("konglish"), F.col("prod_nm_token_cndd"))) \
        .withColumn("initianl_jcd_sim", get_jaccard_sim(F.col("prod_nm_token_cndd"), F.col("initial")))
    result_1 = b.where(F.length(F.col("intersection_word")) > 1)\
        .where(F.col("initianl_jcd_sim") > 0.6)\
        .where(F.col("prod_nm_token_cndd").substr(0, 1) == F.col("initial").substr(0, 1))\
        # .orderBy(F.col("initianl_jcd_sim").desc())

    result_2 = b.where(F.length(F.col("intersection_word")) > 1)\
        .where(F.col("prod_nm_token_cndd") == F.col("intersection_word")) \
        .where(F.col("initianl_jcd_sim") < 0.6) \
        .where(F.col("prod_nm_token_cndd").substr(0, 1) == F.col("initial").substr(0, 1)) \
        # .orderBy(F.col("prod_nm_token").desc())
    # result_2.orderBy(F.col("prod_nm_token").desc()).show(1000, False)
    # b.where(F.col("prod_nm_token") == "나이키").orderBy(F.col("initianl_jcd_sim").desc()).show(1000, False)
    a.where(F.col("prod_nm_token").substr(0, 1) == F.col("initial").substr(0, 1)).orderBy(F.col("initianl_jcd_sim").desc()).show(100, False)


    # todo : 코닥, 모닝, 화이트, 헬시



    # a.where(F.col("prod_nm_token_cndd") == "나이키").orderBy(F.col("initial").desc()).show(100, False)
    #     .select(F.col("prod_nm_token"), F.col("prod_nm_token_cndd"), F.col("konglish"), F.col("intersection_word"),)\
    #     .orderBy(F.col("cnt").desc()).show(1000, False)
#

exit(0)