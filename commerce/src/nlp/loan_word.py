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
import os
os.chdir('../../../')

# todo : 한/영 사전 만들기
#  babootaeng, 바부탱 , codezip, 코드집
@F.udf(returnType=T.ArrayType(T.ArrayType(T.StringType())))
def get_konglish(kor_txt):
    ja = {'ㄱ': ['K','G', 'CK'], 'ㄲ': ['KK','GG'], 'ㄴ': 'N', 'ㄷ': 'D', 'ㄸ': ['D', 'TT'], 'ㄹ': ['R', 'L'], 'ㅁ': 'M', 'ㅂ': 'B',
          'ㅃ': 'B', 'ㅅ': ['S', 'TH'], 'ㅆ': 'SS', 'ㅈ': ['J', 'Z'], 'ㅉ': 'J', 'ㅊ': ['C', 'CH'], 'ㅌ': 'T', 'ㅍ': 'P',
          'ㅋ': ['C','K', 'CH'], 'ㅎ': ['H', 'WH'],
          'ㅇ': ['NG','E'],  # todo : 팔로잉, 스카잉일땐 ng 필요한데..  묵음 처리 어떻게 ?? ex ) 구스다운,
          }
    mo = {'ㅑ': 'Y', 'ㅕ': ['Y', 'TI'], 'ㅛ': 'Y', 'ㅠ': ["Y", "U"], 'ㅖ': 'Y',
          'ㅝ': 'W', 'ㅘ': 'W', 'ㅙ': 'W', 'ㅚ': 'W', 'ㅜ': 'W', 'ㅞ': 'W', 'ㅟ': 'W',
          'ㅔ': 'E', 'ㅡ': 'E', 'ㅢ': 'E',
          'ㅏ': 'A', 'ㅐ': 'A', 'ㅓ': ['U', 'ER'], 'ㅗ': 'O', 'ㅣ': ['I', 'Y', 'E']}
    r_lst = []

    for i, w in enumerate(kor_txt):
        lst = []
        if w[0] in ja.keys():   # 초성
            if list(ja.keys()):
                lst.append(ja[w[0]])
            else:
                lst.append(ja[w[0]])
        else:
            lst.append('0')

        if w[1] in mo.keys():     # 중성
            if list(mo.keys()):
                lst.append(mo[w[1]])
            else:
                lst.append(mo[w[1]])
        else:
            if list(ja.keys()):
                lst.append(ja[w[0]])
            else:
                lst.append('0')
        if w[2] in ja.keys():     # 종성
            # lst.append(ja.keys())
            lst.append(ja[w[2]])
        else:
            lst.append('0')
        r_lst.append(lst)
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


@F.udf(returnType=T.DoubleType())
def get_location_jaccard_sim(eng_cndd, konglish):
    # 위치 반영한 intersection 구하기
    res = []
    for k in eng_cndd:
        for i in konglish:
            for j in i:
                if k.lower() == j.lower():
                    res.append(k.lower())
                    k = "0"
                    j = "0"
    itc = float(len(set(res).intersection(set(eng_cndd))))  # 분자
    union = len(res) + len(konglish) - itc  # 분모
    return 0 if union == 0 else itc / union


@F.udf(returnType=T.DoubleType())
def get_jaccard_sim(str1, str2):
    # set 이유 : 중복성 무시
    a = set(str2)
    b = set(str1)
    itc = float(len(set(a).intersection(set(b))))      # 분자
    union = len(a) + len(b) - itc    # 분모
    return 0 if union == 0 else itc/union


# @F.udf(returnType=T.ArrayType(T.StringType()))
@F.udf(returnType=T.BooleanType())
def get_chosung_contain_word(lst, txt):
    # desc : 포함되는 글자 찾기
    # ex) lst[0][0] : [NG, E]       str: nike
    if lst[0] == '0':  # 종성이 없을 경우 중성 대체
        lst1 = lst[1]
    else:
        lst1 = lst[0]
    lst1 = lst1.replace('[', '').replace(']', '').split(', ')
    txt = list(txt) if type(txt) == list else list(txt)
    rst = []
    for word in lst1:
        if word.lower() == txt[0].lower():
            rst.append(rst)

    # 초성 없을시 중성비교  ex) 아,야,어,여
    if len(rst) == 0:
        lst1 = lst[1]
        for word in lst1:
            if word.lower() == txt[0].lower():
                rst.append(rst)

    if len(rst) == 0:
        return False
    else:
        return True

@F.udf(returnType=T.BooleanType())
def reverse_compare(lst, txt):
    # 종성 비교     ex) lst : ['N', '[U, ER]', '0']   str: nike
    if lst[-1] == '0':  # 종성이 없을 경우 중성 대체
        lst1 = lst[-2]
    else:
        lst1 = lst[-1]
    lst1 = lst1[::-1].replace('[', '').replace(']', '').split(' ,')  # list(lst) if type(lst) == list else list(lst)
    txt = txt[::-1].lower()
    rst = []
    for j in lst1:
        # for i in txt:
        if txt[0].lower() == j.lower():
            rst.append(txt[0].lower())
            break

    # 중성 compare 결과 없을 경우
    if len(rst) == 0:
        lst1 = lst[-3]
        for j in lst1:
            # for i in txt:
            if txt[0].lower() == j.lower():
                rst.append(txt[0].lower())
                break
    if len(rst) == 0:
        return False
    else:
        return True



# @F.udf(returnType=T.BooleanType())
# def get_jongsung_contain(lst, txt):
#     # 종성 비교     ex) lst : ['N', '[U, ER]', '0']   str: nike
#     if lst[-1] == '0':  # 종성이 없을 경우 중성 대체
#         lst = lst[-2]
#     else:
#         lst = lst[-1]
#     lst1 = lst.replace('[', '').replace(']', '').split(', ')  # list(lst) if type(lst) == list else list(lst)
#     txt = list(txt) if type(txt) == list else list(txt)
#
#     for word in lst1:
#         if word.lower() == (txt[-1].lower()):
#             return True
#     else:
#         return False

@F.udf(returnType=T.StringType())
def convert_eng_to_kor(eng_txt):
    w_to_k = {'K': 'ㄱ', 'G': 'ㄲ', 'N': 'ㄴ', 'D': 'ㄷ', 'D':'ㄸ' , 'R': 'ㄹ', 'L': 'ㄹ', 'M': 'ㅁ', 'B': 'ㅂ', 'V': 'ㅂ',
              'BB': 'ㅃ', 'S': 'ㅅ', 'TH': 'ㅅ', 'SS': 'ㅆ', 'J': 'ㅈ', 'JJ': 'ㅉ', 'C': 'ㅊ', 'K': 'ㅋ',
              'C': 'ㅋ', 'T': 'ㅌ', 'P': 'ㅍ', 'H': 'ㅎ',

              'Y': 'ㅑ', 'Y': 'ㅕ', 'TI': 'ㅕ', 'Y': 'ㅛ', "Y": 'ㅠ', "U": 'ㅠ', 'Y': 'ㅖ',
              'W': 'ㅝ', 'W': 'ㅘ', 'W': 'ㅙ', 'W': 'ㅚ', 'W': 'ㅜ', 'W': 'ㅞ', 'W': 'ㅟ',
              'E': 'ㅔ', 'E': 'ㅡ', 'E': 'ㅢ',
              'A': 'ㅏ', 'A': 'ㅐ', 'U': 'ㅓ', 'ㅗ': 'O', 'ㅣ': 'I'
              }
    r_lst = []
    eng_txt = list(eng_txt.upper())
    for i in eng_txt:
        if i in w_to_k.keys():
            r_lst.append(w_to_k[i])
    return ''.join(r_lst)


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

    return "".join(cndd).lower()    # cndd


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('Loan word Job') \
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
        csv("commerce/data/nvr_prod.csv") \
        .select(F.col('상품명'), F.col('대분류'), F.col('중분류'), F.col('소분류'))

    prod_2 = spark.read. \
        option('header', True). \
        csv("commerce/data/nvr_prod_2.csv") \
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

    get_kor_tkn = get_kor_tkn\
        .select(
            F.col('prod_nm_token'),
            F.col('only_kor'),
            F.explode(F.col('prod_nms')).alias('prod_nm')
        ).alias('get_kor_tkn')
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
            (prod.select(F.explode(F.split(F.col('대분류'), "/"))))
            .union(prod.select(F.explode(F.split(F.col('중분류'), "/"))))
            .union(prod.select(F.explode(F.split(F.col('소분류'), "/"))))
        ).distinct()\
        .alias('get_cate')

    except_cate = get_cndd_eng.join(get_cate, F.col('prod_nm_token') == F.col('col'), 'leftanti').alias("except_cate")

    '''
        step2. 상품명 토크나이징 후 하위 dataframe 생성 (step1 과 반대)
        대상1     |   후보들
        eng      |   kor
    '''
    get_eng_tkn = prod_nm\
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
        .join(except_cate,
              [F.col('get_cndd_kor.prod_nm_token') == F.col("except_cate.prod_nm_token_cndd"),
               F.col('get_cndd_kor.prod_nm_token_cndd') == F.col("except_cate.prod_nm_token")],
              'inner') \
        .select(
            F.col('get_cndd_kor.prod_nm_token').alias("prod_nm_token_1"),
            F.col('get_cndd_kor.prod_nm_token_cndd').alias("prod_nm_token_cndd_1"),
            F.col('get_cndd_kor.cnt').alias("cnt_1"),
            F.col('except_cate.prod_nm_token').alias("prod_nm_token_2"),
            F.col('except_cate.prod_nm_token_cndd').alias("prod_nm_token_cndd_2"),
            F.col('except_cate.cnt').alias("cnt_2"),
        ).withColumn("rnk", F.rank().over(window.Window.partitionBy(F.col('prod_nm_token_cndd_2')).orderBy(F.col('cnt_2').desc())))\
        .where(F.col('rnk') < 3)

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
    # b.where(F.col("prod_nm_token") == "나이키").orderBy(F.col("initianl_jcd_sim").desc()).show(1000, False)
    # a.where(F.col("prod_nm_token").substr(0, 1) == F.col("initial").substr(0, 1)).orderBy(F.col("initianl_jcd_sim").desc()).show(100, False)

    # todo: tf-idf 결과로 token(한국어) - prod_nm(영어만 남기기) df 생성후 이니셜 포함 확률 구해보기
    # 코닥, 모닝, 화이트, 헬시
    tf_idf = spark.read.parquet("data/parquet/tfidf/")\
        .select(
            F.trim(
                    F.regexp_replace(
                    F.regexp_replace(F.lower(F.col('token')), "[^가-힣]", ' '), r"\s+", ' '
                )
            ).alias("token"),
            F.explode(
                F.split(
                    F.trim(
                        F.regexp_replace(
                            F.regexp_replace(F.lower(F.col('prod_nm')), "[^A-Za-z]", ' '), r"\s+", ' '
                        )
                    ), ' '
                )
            ).alias('eng_cndd'),
            F.col("tf-idf")
        ).where((F.length(F.col("eng_cndd")) > 2) & (F.length(F.col("token")) > 1))\
        .where(~F.col("token").isin(" "))\
        .distinct()\
        .alias("tf_idf")

    tf_idf = tf_idf.join(get_cate, F.col('token').contains(F.col('col')), 'leftanti').alias("tf_idf")

    get_nitl = tf_idf.withColumn("jaso", get_jaso(F.regexp_replace(F.col("token"), " ", ""))) \
        .withColumn("initial", convert_kor_to_initial(F.col("jaso"))) \
        .withColumn("konglish", get_konglish(F.col("jaso"))) \
        .withColumn("intersection_word", get_intersection_word(F.col("konglish"), F.col("eng_cndd")))\
        .withColumn("initianl_jcd_sim", get_location_jaccard_sim(F.col("eng_cndd"), F.col("konglish")))

         # tf_idf.groupby(F.col("token")).agg(F.count(F.col("eng_cndd")).alias("cnt")).where(F.col("cnt") < 3).show(1000, False)
    # get_nitl.where(F.col('token') == "아디다스").orderBy(F.col("initianl_jcd_sim").desc()).show(1000, False)
    # a = get_nitl.where(F.col('token') == "화이트").where(F.length(F.col("eng_cndd")) >= F.length(F.col("token"))).where(F.length(F.col("eng_cndd"))>3).orderBy(F.col("initianl_jcd_sim").desc())
    a = get_nitl.where(F.length(F.col("intersection_word")) >= 1)\
        .where(F.length(F.col("eng_cndd")) >= F.length(F.col("intersection_word")))\
        .where(F.length(F.col("eng_cndd")) > 2) \
        .withColumn(
            'rank',
            F.rank().over(
                window.Window.partitionBy(
                    F.col('token')
                ).orderBy(F.col('initianl_jcd_sim').desc())
            )
        )
    b = a.withColumn("ja", convert_eng_to_kor(F.col("eng_cndd")))\
        .withColumn("jaso_jcd_sim", get_location_jaccard_sim(F.col("ja"), F.col("jaso"))) \
        .where(F.length(F.col("token")) < F.length(F.col("eng_cndd")))\
        .withColumn("contaion_wd", get_chosung_contain_word(F.col('konglish')[0], F.col('eng_cndd')))\
        .where(F.col('contaion_wd') == True)\
        .distinct()
    '''
        Reverse, middle Compare
        - eng_cndd, intersection_word 
        - eng_cndd, konglish
        # todo : 마지막 글자 vs 초/중/종?
    '''

    c = b.select(F.col('contaion_wd'), F.col('token'), F.col('eng_cndd'), F.col('konglish'),
              reverse_compare(F.element_at(F.col('konglish'), -1), F.col('eng_cndd')).alias("doing")
             )\
        .where(F.col('doing') == True).orderBy(F.col('token'))
    # testcase : 피스타치오, 크롭, 허브, 펀칭, 랜덤, 애플티비, 연말, 안경, 파워, 스팅어

    d = c.groupby(F.col("token"), F.col("eng_cndd")).agg(F.count(F.col("eng_cndd")).alias("eng_cndd_cnt"))    # 빈도수
    # d.orderBy(F.col("token").desc()).show(2000, False)

    d.coalesce(15).write.format("parquet").mode("overwrite") \
        .save("data/parquet/loan_word_cndd/")

    # b.where(F.col("token") == '닌텐도').distinct().orderBy(F.col("tf-idf").desc()).show(2000, False)
    '''
        나이키, 코닥, 모닝, 화이트, 헬시.  옵션, 오렌지, 악세서리, 캘린더,  사이즈, 키즈, 닌텐도, 블랙,  레드,  크리스마스, 아디다스,  디올 
        화이트, 퍼플, 오브엠, 플룻, 오피스, 핫트랙스, 브러쉬, 쿠폰, 밴드, 크리스탈, 패스, 멜란지, 뷰티, 베이비, 삼성, 코어
        
        제거 대상 : 남성, 여성, 출력향상, 흡연, 한글숫자, 기능성, 무료배송, 새제품, 생일판, 수동, 긴팔, 태양필터, 크롭에, 발이따뜻, 
                    부분배송, 상품은, 남성겨울, 본상품선택, 색상랜덤, 부착용, 반사, 선택안함, 마이클코어스, 세대, 방송
    '''

    '''
        빈도수 * tf-idf 결과로 보면 -> 안나옴
       중심 : 한국어 토큰 group
       ver1 : prod_nm에 영어 토큰만 남기고 남은 것과 이니셜이랑 가장 유사한 토큰만 보기
       ver2 : prod_nm에 영어 토큰만 남긴 것중 가장 많이 등장한 영어 토큰 보기
   '''

    '''
        todo : 한국어 토큰중 외래어 인것만 어떻게 뽑아낼것인가 ?
        영어 기준 tfidf 실행후 한글 토큰 기준이랑 join 해보기 => x
        영어 -> 한글 컨버터 개발 -> 자소 분리결과 & 영어 토큰 중복 문자 비교
        token 의 초성과 ja Jaccad Similarity 구하기
    '''

    ''' Ver2 - By ngram candidate '''
    loan_wd_cndd1_by_ngram = spark.read.parquet('data/parquet/loan_wd_cndd1_by_ngram')\
        .select(F.col('toks')[0].alias('eng_tkn'), F.col('toks')[1].alias('kor_tkn'))\
        .alias('loan_wd_cndd1_by_ngram')
    a = loan_wd_cndd1_by_ngram\
        .groupby(F.col('kor_tkn'),F.col('eng_tkn'))\
        .agg(F.count(F.col('eng_tkn')).alias('cnt'))\
        .withColumn(
            'rnk',
            F.rank().over(window.Window.partitionBy(F.col('kor_tkn')).orderBy(F.col('cnt').desc()))
        ).where(F.length(F.col('kor_tkn')) > 1).where(F.col('rnk') < 2)

    b = a.withColumn("jaso", get_jaso(F.regexp_replace(F.col('kor_tkn'), "[^가-힣]", ''))) \
        .withColumn("initial", convert_kor_to_initial(F.col("jaso"))) \
        .withColumn("konglish", get_konglish(F.col("jaso")))
    # b.where(F.col('eng_tkn').substr(0, 1) == F.col('initial').substr(0, 1))\
    #     .orderBy(F.col('kor_tkn'))\
    #     .show(1000, False)

    exit(0)