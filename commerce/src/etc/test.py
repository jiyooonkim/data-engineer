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
# print("itc : ", itc)


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
    print( "lst : ", lst)

token = ['nusskati', '크런치', '땅콩버터', '350g', '9팩', '땅콩버터', '땅콩잼']
n = 3
print("ngram ; " , ngram(token, n))



def get_shingle(token, n):
    lst = []
    if token.__str__():
        token = list(token)

    for i in range(0, len(token)):
        if len(token[i:i+n]) == n:
            lst.append(token[i:i+n])
    print("get_shingle : ", lst)
get_shingle("도어락", 2)

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
    # todo: 네거티브 샘플링
    # def get_negative_sample(all_tokens=[], center = "", negative_tokens=[]):

'''
|살균소독스프레이                                                  |[[12, 스프], [163, 스프레이], [17, 레이], [19, 이], [20, 독], [280, 소], [32, 소독], [36, 스], [62, 살균]]         
|분수기계                                                          |[[10, 분], [187, 기계], [25, 분수], [3, 수기], [32, 기], [57, 수]]      
|사무실청소용품                                                    |[[106, 청소], [14, 청소용품], [15, 실], [19, 청], [19, 품], [240, 사무실], [262, 용], [28, 무], [280, 소], [46, 사무], [499, 용품], [5, 청소용], [9, 사]]     
|사이드메뉴                                                |[[110, 뉴], [19, 이], [32, 드], [7, 메뉴], [7, 사이], [70, 사이드], [9, 사]]     
|삭스앵클부츠                                                      |[[10, 부], [10, 앵클부츠], [134, 부츠], [136, 삭스], [35, 앵클], [36, 스], [7, 클]]  
산모용쿠션용품                                                    |[[14, 모], [19, 품], [262, 용], [27, 산], [28, 산모], [399, 쿠션], [4, 산모용], [499, 용품], [9, 쿠]]                                                                                                                                                                                                                                                              |[]                                                                            |

|삶기세탁세제                                              |[[17, 세], [24, 제], [26, 세탁세제], [32, 기], [4, 삶기], [51, 세제], [60, 세탁]]      
산소공급                                                  |[[100, 공], [12, 산소], [27, 산], [280, 소], [34, 급], [37, 공급]]      
|산업용냉풍기                                                      |[{1, 풍기}, {2, 풍}, {12, 냉풍기}, {19, 냉}, {22, 산업}, {27, 산}, {30, 업}, {32, 기}, {66, 산업용}, {262, 용}]                                                                                                                                                                                                                                                                                                                                                               |[1, 풍기]    |
|산업용석고                                                        |[{6, 석}, {6, 석고}, {21, 고}, {22, 산업}, {27, 산}, {30, 업}, {66, 산업용}, {262, 용}]                                                                                                                                                                                                                                                                                                                                                                                       |[6, 석]      |
|산업용전기히터                                                    |[{2, 히}, {3, 터}, {9, 전기히터}, {20, 전}, {22, 산업}, {27, 산}, {30, 업}, {32, 기}, {57, 히터}, {66, 산업용}, {262, 용}, {314, 전기}]                                                                                                                                                                                                                                                                                                                                       |[2, 히]      |
사각연필꽂이                                         |[[11, 필], [13, 연필꽂이], [19, 이], [190, 사각], [22, 연필], [23, 각], [42, 꽂이], [7, 연], [9, 사]]   
|상의정리커버                                                      |[[12, 의], [124, 정리], [21, 정], [24, 상], [5, 리커버], [606, 커버], [81, 상의]]     

|브레이브복싱글러브                                                |[[10, 복], [119, 복싱], [12, 브레이브], [165, 글러브], [17, 레이], [171, 싱글], [19, 러브], [19, 이], [31, 복싱글]]        
|생과일주스                                                        |[[136, 과일], [34, 주], [36, 스], [42, 생], [53, 주스], [8, 생과], [8, 일], [9, 과일주]]   
|울체크치마바지                                                    |[[102, 체크], [11, 지], [151, 울], [16, 마], [189, 바지], [34, 치마], [71, 바]] 
'''
# txt = '산업용냉풍기'
# txt = '삶기세탁세제'
txt = '레노버사운드바'
# txt = '체크치마바지'
# txt = '산업용석고'
# txt = '산업용석고'
tkn_list = [[15, '사운드바'], [16, '레노버'], [32, '드'], [4, '레'], [5, '운'], [7, '노'], [71, '바'], [71, '사운드'], [9, '사']]
# tkn_list = [[14, '모'], [19, '품'], [262, '용'], [27, '산'], [28, '산모'], [399, '쿠션'], [4, '산모용'], [499, '용품'], [9, '쿠']]
# tkn_list = [[6, '석'], [6, '석고'], [21, '고'], [22, '산업'], [27, '산'], [30, '업'], [66, '산업용'], [262, '용']]
# tkn_list =[[17, '세'], [32, '기'], [60, '세탁'], [24, '제'], [26, '세탁세제'],  [4, '삶기'], [51, '세제']]
# tkn_list = [[10, '부'], [10, '앵클부츠'], [134, '부츠'], [136, '삭스'], [35, '앵클'], [36, '스'], [7, '클']]
# tkn_list = [[110, '뉴'], [19, '이'], [32, '드'], [7, '메뉴'], [7, '사이'], [70, '사이드'], [9, '사']]
# tkn_list = [[11, '필'], [13, '연필꽂이'], [19, '이'], [190, '사각'], [22, '연필'], [23, '각'], [42, '꽂이'], [7, '연'], [9, '사']]
# tkn_list = [[10, '분'], [187, '기계'], [25, '분수'], [3, '수기'], [32, '기'], [57, '수']]
# tkn_list = [[12, '스프'], [163, '스프레이'], [17, '레이'], [19, '이'], [20, '독'], [280, '소'], [32, '소독'], [36, '스'], [62, '살균']]
# tkn_list = [[12, '의'], [124, '정리'], [21, '정'], [24, '상'], [5, '리커버'], [606, '커버'], [81, '상의']]
# tkn_list = [[10, '복'], [119, '복싱'], [12, '브레이브'], [165, '글러브'], [17, '레이'], [171, '싱글'], [19, '러브'], [19, '이'], [31, '복싱글']]
# tkn_list = [[102, '체크'], [11, '지'], [151, '울'], [16, '마'], [189, '바지'], [34, '치마'], [71,'바']]

'''
리스트중 제일 많이 등장한 단어 일 가능성 
todo : 후보들중 포함하지 않는 토큰에 대해서 추가 되지 않는 현상 => 워드- 후보 = 남은 토큰 으로 해결 가능 ??
앵클부츠 -> 앵클 + 부츠 안되는 현상 !!
'''
def get_tokns(tkn_list, txt):
    total = []
    for i in range(0, len(tkn_list)):
        tkn_cndd = []
        max_len_txt = '' 
        for j in range(0, len(tkn_list)):
            if  (tkn_list[j][1]!= (tkn_list[i][1])):
                if (tkn_list[j][1].__contains__(tkn_list[i][1])) :
                    if len(max_len_txt) <= len(tkn_list[j][1]):
                        # if tkn_list[i][0] < tkn_list[j][0]:
                        max_len_txt = tkn_list[j][1]
                    tkn_cndd.append(tkn_list[j][1]) 
        if max_len_txt != "":
            txt = txt.strip(max_len_txt)
            total.append(max_len_txt)
    if txt !='':
        total.append(txt)

    return list(set(total))

print("get_tokns 결과 : ", get_tokns(tkn_list, txt))

# tkn_list = [[11, '필'], [13, '연필꽂이'], [19, '이'], [190, '사각'], [22, '연필'], [23, '각'], [42, '꽂이'], [7, '연'], [9, '사']]

cndds = ['글러브', '브레이브', '복싱글']
def sub_tokenize(cndds, tkn_list):
    sub_tokens = []
    for i in cndds:     # ['사각', '연필꽂이']
        for j in tkn_list:      #
            if i.__ne__(j):
                if (len(i) > len(j)) &(len(j[1])>1):
                    if i[0:len(j[1])] == j[1]:
                        sub_tokens.append(i[0:len(j[1])])
                        # print("시작 : ", j, i[0:len(j[1])])
                    elif i[len(i) - len(j[1]):] == j[1]:
                        sub_tokens.append(i[len(i) - len(j[1]):])
    return list(set(cndds + (sub_tokens)))

print("sub_tokenize 결과 : ", sub_tokenize(cndds, tkn_list))

# def get_tokns(tkn_list, ):
#     tkn_cndd = []
#     for i in range(0, len(tkn_list)):
#         for j in range(i, len(tkn_list)):
#             if (tkn_list[i][1].__contains__(tkn_list[j][1])) | (tkn_list[j][1].__contains__(tkn_list[i][1])):
#                 if (tkn_list[i][1]!=(tkn_list[j][1])):
#                     if int(tkn_list[i][0]) < int(tkn_list[j][0]):
#                         if len(tkn_list[i][1]) >= len(tkn_list[j][1]):
#                             tkn_cndd.append(tkn_list[i])
#
#
#     return tkn_cndd
# print(get_tokns(tkn_list))