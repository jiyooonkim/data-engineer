# -*- coding: utf-8 -*-
"""
# title : compound_word
# doc : 합성어 추출
# desc : - 합성어 예시 : 클렌징 + 폼 = 클렌징폼, 메주 + 가루 = 메주가루, 휴대용 + 빨래판 = 휴대용빨래판, 수영 + 안경 = 수영안경


"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_token_ver1(crr_wd, cndd_wd):
    '''
    선글라스케이스 |케이스 |[케이스, 선글라] |
    '''
    tokens = []
    if cndd_wd in crr_wd:
        if len(crr_wd.strip(cndd_wd)) > 1:
            tokens.append(cndd_wd)
            tokens.append(crr_wd.replace(cndd_wd, ''))
    if crr_wd in cndd_wd:
        if len(cndd_wd.strip(crr_wd)) > 1:
            tokens.append(crr_wd)
            tokens.append(cndd_wd.replace(crr_wd, ''))
    return tokens


@F.udf(returnType=T.ArrayType(T.StringType()))
def get_token_ver2(crr_wd, cndd_wd):
    '''
    문제 : 선글라스케이스 |케이스 |[케이스, 선글라] |
    '''
    tokens = []
    if crr_wd.__contains__(cndd_wd):
        if len(crr_wd.strip(cndd_wd)) > 1:
            tokens.append(cndd_wd)
            if len(crr_wd.split(cndd_wd)[0]) > 1 or len(crr_wd.split(cndd_wd)[1]) > 1:
                tokens.extend(crr_wd.split(cndd_wd))
            # else:
            #     tokens.append(crr_wd)

    if cndd_wd.__contains__(crr_wd):
        if len(cndd_wd.strip(crr_wd)) > 1:
            tokens.append(crr_wd)
            if len(cndd_wd.split(crr_wd)[0]) > 1 or len(cndd_wd.split(crr_wd)[1]) > 1:
                tokens.extend(cndd_wd.split(crr_wd))
            # else:
            #     tokens.append(crr_wd)
    return list(filter(None, tokens))


def rmove_txt(word, tkns):
    for j in tkns:
        word = word.replace(j, ' ')
    return len(word.replace(" ", ""))


@F.udf(returnType=T.StringType())
def get_log_txt(word1, word2):
    if len(word1) < len(word2):
        word = word2
    else:
        word = word1
    return word


@F.udf(returnType=T.BooleanType())
def check_token_correction(tkns1, tkns2, word):
    # 토크나이징 버전1, 버전2 동일여부 체크
    # if len(set(tkns1) - set(tkns2)) == 0:
    #     return True
    # else:
    if (rmove_txt(word, tkns1) == 0) & (rmove_txt(word, tkns2) == 0):
        return True
    else:
        return False

@F.udf(returnType=T.ArrayType(T.StringType()))
def check_token_correction(tkns1, tkns2, word):
    # todo : 후부군 2개 이상인것 동일한것은 1개만 리턴
#  아닌 거는 끝에 글자 비교해서 1개만 추출
    '''
    |가공안주류              |[[주류, 가공안], [가공, 안주류]]                        |

    |pc케이블                |[[케이블, pc], [pc케, 이블]]                    |

    |가방세트                |[세트, 가방]                |[세트, 가방]                |true            |
    |가방세트                |[가방, 세트]                |[가방, 세트]                |true            |

    '''


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName('jy_kim') \
        .master('local[*]') \
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

    # setp1 . 자카드 유사도 이용해 토근 후보들 추출 - 단어의 유사성 이용하여 seed 생성
    compound_word_candidate = spark.read.parquet("hdfs://localhost:9000/compound_word_candidate")\
        .select(F.col('prod_nm'), F.col('count_w').alias('prod_nm_cnt'), F.col('cate'))\
        .where(F.col('jaccard_sim') > 0.4)\
        .withColumn('compound_word_v2', get_token_ver2(F.col('prod_nm'), F.col('cate')))\
        .withColumn('compound_word_v1', get_token_ver1(F.col('prod_nm'), F.col('cate')))\
        .withColumn('target_word', get_log_txt(F.col('prod_nm'), F.col('cate')))\
        .withColumn('check_correction', check_token_correction(F.col('compound_word_v2'), F.col('compound_word_v1'), F.col('target_word')))

    # compound_word_candidate.where(F.col('prod_nm').like('가루')).where(F.size(F.col('compound_word_v2')) > 1).show(100, False)

    # setp2 . 자카드 유사도 확률, 2가지 방식 토크나이징 이용하여 조건
    condition = compound_word_candidate\
        .where(F.size(F.col('compound_word_v1')) > 1)\
        .where(F.length(F.col('cate'))>1)\
        .where(F.col('check_correction') == True)\
        .select(F.col('target_word'), F.col('compound_word_v2'), F.col('compound_word_v1'), F.col('check_correction'))\
        .distinct()
    # condition.orderBy(F.col('target_word')).show(1000, False)
    # .where(F.col('target_word') == '가제손수건8장')
    # .where(F.col('target_word').like('%검도용%'))
    aa = condition.groupby(F.col('target_word')).agg(F.collect_set(F.col('compound_word_v2')))
    aa.show(1000, False)




    exit(0)