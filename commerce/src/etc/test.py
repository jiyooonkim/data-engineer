# algorithm : cosine_similarity
tkns1 = ['바베큐', '1800', '루시아', '전기', '오븐', '베이킹', '오븐']
tkns2 = ['테이블용', '전기', '상업용', '소형튀김기', '분체', '튀김기', '우성', 'ef010']

# step1 : 리스트 합치기
tkns = list(set(tkns1 + tkns2))  # 합친 토큰은 중복 제거
# tkns = sum(tkns1, tkns2)
# print("tkns : ", tkns)

# step2 : 합친 리스트 단어 빈도수 리스트 구하기
tkns_cnt1 = []
tkns_cnt2 = []
# todo : 함수 전환 get (최종 토큰들, 대상토큰들) 중복제거 하기
for i in range(0, len(tkns)):
    cnt = 0
    for j in range(0, len(tkns1)):
        if tkns[i] == tkns1[j]:
            cnt = cnt + 1
    tkns_cnt1.append(cnt)

for i in range(0, len(tkns)):
    cnt = 0
    for j in range(0, len(tkns2)):
        if tkns[i] == tkns2[j]:
            cnt = cnt + 1
    tkns_cnt2.append(cnt)

# print("tkns_cnt1 : ", tkns_cnt1)
# print("tkns_cnt2 : ", tkns_cnt2)

# step3. cs 공식 대입
# todo : business logic 처리  -> 따로 함수 생성 할 것
cs_val1 = sum([i ** 2 for i in tkns_cnt1])
cs_val2 = sum([i ** 2 for i in tkns_cnt2])

# 분모(demominator) : 두 벡터의 곱
demom = (cs_val1 * cs_val2) ** (1 / 2)
# print("cs_val1 : ", cs_val1)
# print("cs_val2 : ", cs_val2)
# print("demom : ", demom)

# 분자(numerator) : 두 벡터의 내적
numerator = 0
for i in range(0, len(tkns_cnt1)):  # 토큰 카운트 리스트 받아옴
    numerator = numerator + (tkns_cnt1[i] * tkns_cnt2[i])
# print("numerator : ", numerator)

# 최종
cs = numerator / demom


# print("cs : ", cs)


# step1. 길이 맞추기
def get_match_length(crr_wd, cndd_wd):
    if len(crr_wd) > len(cndd_wd):
        for i in range(0, len(crr_wd) - len(cndd_wd)):
            cndd_wd.append("nn")
    else:
        for i in range(0, len(cndd_wd) - len(crr_wd)):
            crr_wd.append("nn")
    return crr_wd, cndd_wd


# step2-1. 정타가 기준이 되어 후보 리스트 비교 (deletion)
crr_wd = ['a', 'c', 't', 'r', 'e', 's', 's']
cndd_wd = ['a', 'c', 'r', 'e', 's']
crr_txt = []
err_txt = []
posision = []
if len(crr_wd) > len(cndd_wd):
    crr_wd, cndd_wd = get_match_length(crr_wd, cndd_wd)
    for i in range(0, len(crr_wd)):
        if cndd_wd[i] != crr_wd[i]:
            crr_txt.append(crr_wd[i])
            cndd_wd.insert(i, crr_wd[i])
            posision.append(i)

# step2-2. insertion
crr_wd = ['a', 'c', 'r', 'e', 's', 'e']
cndd_wd = ['a', 'c', 'c', 'e', 's', 's']
crr_txt = []
err_txt = []
posision = []
if len(crr_wd) < len(cndd_wd):
    crr_wd, cndd_wd = get_match_length(crr_wd, cndd_wd)
    for i in range(0, len(crr_wd)):
        if cndd_wd[i] != crr_wd[i]:
            err_txt.append(cndd_wd[i])
            crr_wd.insert(i, cndd_wd[i])
            posision.append(i)

# step2-3. transposition, substitution
else:
    for i in range(0, len(crr_wd)):
        # if cndd_wd[i] == crr_wd[i]:
        #     cndd_wd[i].replace(cndd_wd[i], 'nn')
        #     crr_wd[i].replace(crr_wd[i], 'nn')
        if cndd_wd[i] != crr_wd[i]:
            crr_txt.append(crr_wd[i])
            err_txt.append(cndd_wd[i])
            posision.append(i)


# print("crr_txt : ", crr_txt)
# print("err_txt : ", err_txt)
# print("posision : ", posision)


# if len(crr_wd) > len(cndd_wd):  # 정타가 더 길다면 : deletion
#     for i in range(0, len(cndd_wd)):
#         if cndd_wd[i] != crr_wd[i]:
#             crr_txt.append(crr_wd[i])
#             cndd_wd.insert(i, crr_wd[i])
#             posision.append(i)
#             print(i, "crr_wd : ", crr_wd)
#             print(i, "cndd_wd : ", cndd_wd)
#     crr_txt.extend(crr_wd[len(cndd_wd):])
#     crr_txt.extend(crr_wd[len(cndd_wd):])

# for i in range(0, len(cndd_wd)):
#     if cndd_wd[i] != crr_wd[i]:     # ex) acress(err) -> actress(crr)
#         # crr_txt.append(crr_wd[i])
#         cndd_wd.insert(i+1, crr_wd[i])
#         crr_txt.append(cndd_wd[i])
#         # crr_txt.append(crr_wd[i])
#         posision.append(i)
#         crr_wd.pop(i)


# elif len(crr_wd) < len(cndd_wd):  # 오타가 더 길다면 :insertion
#     for i in range(0, len(crr_wd)):
#         if cndd_wd[:i] != crr_wd[:i]:
#             # ex) acress(err) -> acres(crr)
#             err_txt.append(crr_wd[i + 1])


def get_konglish(kor_txt):
    ja = {'ㄱ': ['K', 'G'], 'ㄲ': ['KK', 'GG'], 'ㄴ': 'N', 'ㄷ': 'D', 'ㄸ': 'D', 'ㄹ': 'R', 'ㅁ': 'M', 'ㅂ': 'B',
          'ㅃ': 'B', 'ㅅ': ['S', 'TH'], 'ㅆ': 'SS', 'ㅈ': 'J', 'ㅉ': 'J', 'ㅊ': 'C', 'ㅌ': 'T', 'ㅍ': 'P', 'ㅎ': ['H', 'WH'],
          'ㅋ': ['C', 'K', 'CH']}
    mo = {'ㅑ': 'Y', 'ㅕ': ['Y', 'TI'], 'ㅛ': 'Y', 'ㅠ': ["Y", "U"], 'ㅖ': 'Y',
          'ㅝ': 'W', 'ㅘ': 'W', 'ㅙ': 'W', 'ㅚ': 'W', 'ㅜ': 'W', 'ㅞ': 'W', 'ㅟ': 'W',
          'ㅔ': 'E', 'ㅡ': 'E', 'ㅢ': 'E',
          'ㅏ': 'A', 'ㅐ': 'A', 'ㅓ': 'U', 'ㅗ': 'O', 'ㅣ': 'I'}
    r_lst = []
    for i, w in enumerate(kor_txt):
        lst = []
        # print(" i : ", i)
        # print(" w : ", w)
        if w[0] in ja.keys():  # 초성
            lst.append(ja[w[0]])
        else:
            lst.append(' ')
        if w[1] in mo.keys():  # 중성
            # lst.append(mo.keys())
            lst.append(mo[w[1]])
        else:
            lst.append(' ')
        if w[2] in ja.keys():  # 종성
            # lst.append(ja.keys())
            lst.append(ja[w[2]])
        else:
            lst.append(' ')
        r_lst.append(lst)
    return r_lst


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

    return "".join(cndd).lower()  # cndd


def get_jaccard_sim(str1, str2):
    # set 이유 : 중복성 무시
    # a = set(str2)
    # b = set(str1)
    print("a : ", ' '.join(str1))
    print("str2 : ", str2)
    # itc = float(len(set(a).intersection(set(b))))      # 분자
    # union = len(a) + len(b) - itc    # 분모
    # return 0 if union == 0 else itc/union


eng_cndd = "gold"  # 골드
konglish = [['K', 'G', 'O', 'R'], ['D', 'E', ' ']]
res = []
for k in eng_cndd:
    for i in konglish:
        for j in i:
            if k == j.lower():
                res.append(k)
                k = "0"
                j = "0"
itc = float(len(set(res).intersection(set(eng_cndd))))  # 분자
union = len(res) + len(konglish) - itc  # 분모
print("itc : ", itc)


def convert_eng_to_kor(eng_txt):
    w_to_k = {'K': 'ㄱ', 'G': 'ㄲ', 'N': 'ㄴ', 'D': 'ㄷ', 'D': 'ㄸ', 'R': 'ㄹ', 'M': 'ㅁ', 'B': 'ㅂ', 'V': 'ㅂ',
              'BB': 'ㅃ', 'S': 'ㅅ', 'TH': 'ㅅ', 'SS': 'ㅆ', 'J': 'ㅈ', 'JJ': 'ㅉ', 'C': 'ㅊ', 'K': 'ㅋ',
              'C': 'ㅋ', 'T': 'ㅌ', 'P': 'ㅍ', 'H': 'ㅎ',
              # mo
              'Y': 'ㅑ', 'Y': 'ㅕ', 'TI': 'ㅕ', 'Y': 'ㅛ', "Y": 'ㅠ', "U": 'ㅠ', 'Y': 'ㅖ',
              'W': 'ㅝ', 'W': 'ㅘ', 'W': 'ㅙ', 'W': 'ㅚ', 'W': 'ㅜ', 'W': 'ㅞ', 'W': 'ㅟ',
              'E': 'ㅔ', 'E': 'ㅡ', 'E': 'ㅢ',
              'A': 'ㅏ', 'A': 'ㅐ', 'U': 'ㅓ', 'ㅗ': 'O', 'ㅣ': 'I'

              }
    r_lst = []
    eng_txt = list(eng_txt.upper())
    for i in eng_txt:
        print("i : ", i)
        if i in w_to_k.keys():
            r_lst.append(w_to_k[i])
    return ''.join(r_lst)


eng_txt = "camac"


# print("convert_eng_to_kor : ", convert_eng_to_kor(eng_txt))
# initianl_jcd_sim = get_intersection_word(kor_txt, eng)
# print("initianl_jcd_sim : ", initianl_jcd_sim)
# print("get_jaccard_sim : ", get_jaccard_sim(initianl_jcd_sim, kor_txt))
# import numpy as np
# print(np.log((4+1) / (1+1)) + 1)


def get_contain_word(lst, txt):
    # desc : 포함되는 글자 찾기
    # ex) lst[0][0] : [NG, E]       str: nike
    if lst[0] == '0':  # 종성이 없을 경우 중성 대체
        lst1 = lst[1]
    else:
        lst1 = lst[0]
    lst1 = lst1.replace('[', '').replace(']', '').split(', ')
    print("lst1 : ", lst1)
    txt = list(txt) if type(txt) == list else list(txt)
    print("get_contain_word lst1 : ", lst1)
    print("get_contain_word txt : ", txt)
    rst = []
    for word in lst1:
        if word.lower() == txt[0].lower():
            rst.append(rst)
            # todo  : break ??


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


def reverse_compare(lst, txt):
    # reverse
    # 종성 비교     ex) lst : ['N', '[U, ER]', '0']   str: nike
    if lst[-1] == '0':  # 종성이 없을 경우 중성 대체
        lst1 = lst[-2]
    else:
        lst1 = lst[-1]
    lst1 = lst1[::-1].replace('[', '').replace(']', '').split(' ,')  # list(lst) if type(lst) == list else list(lst)
    txt = txt[::-1].lower()  # list(txt) if type(txt) == list else list(txt)
    # print("lst[-1] : ", lst1)
    # print("txt : ", txt)
    rst = []
    for j in lst1:
        # for i in txt:
        if txt[0].lower() == j.lower():
            rst.append(txt[0].lower())
            break
                
    # 중성 compare 결과 없을 경우 
    if len(rst) == 0:
        # print(" lstlstlst : ", lst)
        lst1 = lst[-3]
        for j in lst1:
            # for i in txt:
            if txt[0].lower() == j.lower():
                rst.append(txt[0].lower())
                break
    print("rst : ", rst )
    if len(rst) == 0:
        return False
    else:
        return True

    # for word in lst1:
    #     if word.lower() == (txt.lower()):
    #         return True



lst = ['[S, TH]', '[I, Y, E]', '0']
str = 'ktown'
# print(get_contain_word(lst, str))
# print(reverse_compare(lst, str))


def ngram(token, n):
    lst = []
    for i in range(0, len(token)-n):
        lst.append(token[i:i+n])
    # print( "lst : ", lst)

token = ['nusskati', '크런치', '땅콩버터', '350g', '9팩', '땅콩버터', '땅콩잼']
n = 3
print(ngram(token, n))




def get_kor(wd):
    import re
    reg = re.compile(r'[가-힣a-zA-Z]')
    hangul = re.compile(u'[^a-z]+')
    res = hangul.sub(u'', wd)
    res = reg.sub(u'', res)
    # print("res  : ", res)
    if res: # kor+ eng

        return True
    else:   # only kor
        return False


def get_triple_token(tks, tk):
    output_total = []
    contain_tk_set = []
    for i in range(0, len(tks)):
        for j in range(i, len(tks)):
            for k in range(j, len(tks)):
                if (tks[i].__ne__(tks[j])) & (tks[j].__ne__(tks[k])) & (tks[k].__ne__(tks[i])):
                    if (tk.__eq__(tks[i])) | (tk.__eq__(tks[j])) | (tk.__eq__(tks[k])):
                        contain_tk_set.append(([tks[i], tks[j], tks[k]]))
                    else:
                        output_total.append(([tks[i], tks[j], tks[k]]))
    return contain_tk_set, output_total
tk = '10개입'
tks = ['ph', '내열', '높은', '2칸', '일회용도시락', '10개입', '1개']
# print("tks : ", get_triple_token(tks, tk))


def get_intersection_word(arr1, arr2):
    return list(set(arr1) & set(arr2))

arr1 = ['모델', '나이키', '수량']
arr2 = ['모델', '에브리데이', '수량']
# print(get_intersection_word(arr1, arr2))


def sliding_window(tokens=[], center = "", window_size=1):
    '''
        :param  i : 입력1(embedding layer1)   j : 입력2(embedding layer2)
        window size = 1
    '''
    def skip_grams(center, tkns):
        grams= list()
        for i in tkns:
            if center != i:
                grams.append([center, i])
        print("grams : ", grams)
        return grams



    res = list()
    for idx in range(len(tokens)):
        if center == tokens[idx]:
            print("tokens[idx] : ", tokens[idx])
            if idx - window_size < 0:
                # skip_gram.append(tokens[0: idx + window_size + 1])
                # skip_grams(tokens[idx], tokens[0: idx + window_size + 1])
                res.extend(skip_grams(tokens[idx], tokens[0: idx + window_size + 1]))
            else:
                # skip_gram.append(tokens[idx - window_size: idx + window_size + 1])
                # skip_grams(tokens[idx], tokens[idx - window_size: idx + window_size + 1])
                res.extend(skip_grams(tokens[idx], tokens[idx - window_size: idx + window_size + 1]))
        # print("skip_gram : ", skip_gram)

    return res


# print("sliding_window : ",
#       sliding_window(
#           ['홀더', '스탠드', '삼각대', '플렉시블', '헤드', '핫슈', '조인트', '듀얼'],
#           '핫슈', 3)
#       )



# todo : 
def negative_sampling(tokens=[], center = "", window_size=1):
    res = list()
    for idx in range(len(tokens)):
        if center == tokens[idx]:
            print("tokens[idx] : ", tokens[idx])
            if idx - window_size < 0:
                # print("1 : ", tokens[0: idx + window_size + 1])
                print("1 : ", [x for x in tokens if x not in tokens[0: idx + window_size + 1]] )
            else:
                # print("2 : ",tokens[idx - window_size: idx + window_size + 1])
                print("2 : ",[x for x in tokens if x not in tokens[idx - window_size: idx + window_size + 1]] )
    def get_negative_sample(all_tokens=[], center = "", negative_tokens=[]):


print("sliding_window : ",
      negative_sampling(
          ['홀더', '스탠드', '삼각대', '플렉시블', '헤드', '핫슈', '조인트', '듀얼'],
          '핫슈', 3)
      )




