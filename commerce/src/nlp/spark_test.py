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
    ja = {'ㄱ': ['K','G'], 'ㄲ': ['KK','GG'], 'ㄴ': 'N', 'ㄷ': 'D', 'ㄸ': 'D', 'ㄹ': ['R', 'L'], 'ㅁ': 'M', 'ㅂ': 'B',
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
            lst.append('0')

        if w[1] in mo.keys():     # 중성
            if list(mo.keys()):
                lst.extend(mo[w[1]])
            else:
                lst.append(mo[w[1]])
        else:
            if list(ja.keys()):
                lst.extend(ja[w[0]])
            else:
                lst.append('0')
        if w[2] in ja.keys():     # 종성
            # lst.append(ja.keys())
            lst.append(ja[w[2]])
        else:
            lst.append('0')
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

    return "".join(cndd).lower() #cndd


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('Spark test Job') \
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
    prod_nm.where(F.col("only_kor") ==False).where(F.col("only_eng") ==False).show(1000, False)

