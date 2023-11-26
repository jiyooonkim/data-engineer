


# txt = "비비안카드지갑"
# lis = ["비비", "비비안", "지갑", "카드"]
# txt = "회전매직빈구슬피젯큐브스피너토이손장난감매직퍼즐스트레스해소"
# lis = ["큐브", "구슬", "매직", "매직빈", "매직퍼즐", "스피너", "장난감", "토이", "레스", "퍼즐", "해소", "회전", "트레", "스트레스", "피젯"]

# lit = ["큐빅", "케이", "갤럭시","케이스", "럭시"]


# txt = "남성반팔티셔츠빅사이즈"
# lis = ["반팔", "셔츠", "이즈", "남성", "사이즈", "티셔츠"]

# txt = "울트라드래곤레고"
# lis = ["울트라", "레고", "드래곤", "라드"]
# res =[]
# for i in range(0, len(lis)):
#     for j in range(i, len(lis)):
#         if (lis[i]) != (lis[j]):
#             if lis[i].__contains__(lis[j]):
#                 if len(lis[i]) < len(lis[j]):
#                     res.append(lis[j])
#                 elif len(lis[i]) > len(lis[j]):
#                     res.append(lis[i])
#                 # print("lis[i] : ", lis[i])
#                 # print("lis[j] : ", lis[j])
#                 # if len(lis[i]) < len(lis[j]):
#                 #     lis.remove(lis[i])
#                 #     res.append(lis[j])
#                 # else:
#                 #     lis.remove(lis[j])
#                 #     res.append(lis[i])
#
#
# print("res : ", res)

# aa = list(set(set(lis) - set(res)))
# for i in aa:
#     print("i : ", i)
#     if i in txt:
#         txt= txt.replace(i, '')
#     # if txt is None:
#     #     print("OK")
#     if len(txt) == 0:
#         print("완료")
#     print("txt : ", txt )
#     print("=====================")
# print("aa : ", aa)

# kwd = "울트라드래곤레고"
# lst = []
# for i in aa:
#     for j in aa:
#         create_kwd = i+j
#         if create_kwd in kwd:
#             lst.append(i)
#             lst.append(j)
# # print("lst : ", set(lst))





# origin_wd = ""
# tmp = ""
# for i in range(0, len(txt)):
#     res = []
#     tmp = tmp + (txt[i])
#     origin_wd = origin_wd + (txt[i])
#     print("tmp : ", tmp)
#     for li in lit:
#         if tmp.__contains__(li):
#             lit.remove(li)
#             tmp = tmp.removeprefix(li)
#             res.append(li)
#     # if res == []:
#     #     res.remove(li)
#     print("res : ", res)

# emt = []
# for i in range(0, len(lit)):
#     print("lit[i] : ", lit[i])
#     for j in range(i, len(lit)):
#         print("lit[j] : ", lit[j])
#         if lit[j] in lit[i]:
#             if len(lit[i]) < len(lit[j]):
#                 emt.append(lit[j])
#                 emt.remove(lit[i])
#             else:
#                 emt.append(lit[i])
#
#     print("emt : ", set(emt))
#     print("=====================")


# from konlpy.tag import Okt, Kkma
#
# okt = Okt()  ## 단어 개별 분석
# kkma = Kkma()  ## 단어 중복 분석
# txt = "명사이다"
# tweet_okt = okt.nouns(str(txt))
# # tweet_kkma = kkma.nouns(tweet_message)
# print("tweet_okt : ", tweet_okt)


# str1 = '더랄라 케이크망또 여아 데일리 외출복 얼집 유천 등원룩 망토 스카프 모 더랄라'
# str2 = '미술놀이 유아미술놀이 유아학습 캐릭터 가위 오리기 등원룩'
# a = set(str1.split())
# b = set(str2.split())
# c = a.intersection(b)
# res = float(len(c)) / (len(a) + len(b) - len(c))
# print("a : ", a)
# a_set = []
# for wd in a:
#     if len(wd) > 1:
#         # print("a : ", wd )
#         a_set.append(wd)
# print("a_set : ", set(a_set))
# print("b : ", b, len(b))
# print("c : ", c, len(c))
# print("res : ", res)


'''
    선글라스케이스 |케이스 |[케이스, 선글라] |
홈바테이블세트                |2          |테이블
|체크올인원패딩                |1          |올인원
|검도용                        |2          |기타검도용품
니트스커트                       |1          |니트
'''
tokens = []
crr_wd = '청바지용'
cndd_wd = '바지'
if cndd_wd in crr_wd:
    if len(crr_wd.strip(cndd_wd)) > 1:
        tokens.append(cndd_wd)
        tokens.append(crr_wd.strip(cndd_wd))
if crr_wd in cndd_wd:
    if len(cndd_wd.strip(crr_wd)) > 1:
        tokens.append(crr_wd)
        tokens.append(cndd_wd.strip(crr_wd))

print("tokens : ", tokens)
# for i in range(0, len(crr_wd)+1):

crr_wd = '청바지용'
cndd_wd = '바지'
tokens = []
if crr_wd.__contains__(cndd_wd):
    if len(crr_wd.strip(cndd_wd)) > 1:
        print("2 : ", cndd_wd)
        print("2 : ", crr_wd.split(cndd_wd))
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

print("ver2 : ", tokens)

'''
|케이          |아이스스케이트    |[케이, 아이스스, 트]       |[케이, 아이스스트] 
|word1       |word2         |tkns1                  |tkns2 
'''
word1 = '손수건'
word2 = '가제손수건8장'
tkns1 = ['케이', '아이스스', '트']
tkns2 = ['케이', '아이스스트']
# def check_token_correction(tkns1, tkns2, word1, word2):
word = ''

if len(word1) < len(word2):
    word = word2
else:
    word = word1
def aa(word, tkns):
    for j in tkns:
        word = word.replace(j, ' ')
    return len(word.replace(" ", ""))

# print('wrod22 : ', word)
# print(aa(word, tkns1))

lists = [ㄱ, ㅏ, ㄱ]

print(lists.type())
