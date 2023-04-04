


# txt = "비비안카드지갑"
# lis = ["비비", "비비안", "지갑", "카드"]
# txt = "회전매직빈구슬피젯큐브스피너토이손장난감매직퍼즐스트레스해소"
# lis = ["큐브", "구슬", "매직", "매직빈", "매직퍼즐", "스피너", "장난감", "토이", "레스", "퍼즐", "해소", "회전", "트레", "스트레스", "피젯"]

# lit = ["큐빅", "케이", "갤럭시","케이스", "럭시"]


# txt = "남성반팔티셔츠빅사이즈"
# lis = ["반팔", "셔츠", "이즈", "남성", "사이즈", "티셔츠"]

txt = "울트라드래곤레고"
lis = ["울트라", "레고", "드래곤", "라드"]
res =[]
for i in range(0, len(lis)):
    for j in range(i, len(lis)):
        if (lis[i]) != (lis[j]):
            if lis[i].__contains__(lis[j]):
                if len(lis[i]) < len(lis[j]):
                    res.append(lis[j])
                elif len(lis[i]) > len(lis[j]):
                    res.append(lis[i])
                # print("lis[i] : ", lis[i])
                # print("lis[j] : ", lis[j])
                # if len(lis[i]) < len(lis[j]):
                #     lis.remove(lis[i])
                #     res.append(lis[j])
                # else:
                #     lis.remove(lis[j])
                #     res.append(lis[i])


print("res : ", res)

aa = list(set(set(lis) - set(res)))
for i in aa:
    print("i : ", i)
    if i in txt:
        txt= txt.replace(i, '')
    # if txt is None:
    #     print("OK")
    if len(txt) == 0 :
        print("완료")
    print("txt : ", txt )
    print("=====================")
print("aa : ", aa)

kwd = "울트라드래곤레고"
lst = []
for i in aa:
    for j in aa:
        create_kwd = i+j
        if create_kwd in kwd:
            lst.append(i)
            lst.append(j)
print("lst : ", set(lst))






# print(txt.removeprefix(emt))

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


from konlpy.tag import Okt, Kkma

okt = Okt()  ## 단어 개별 분석
kkma = Kkma()  ## 단어 중복 분석
txt = "사고싶다"
tweet_okt = okt.nouns(str(txt))
# tweet_kkma = kkma.nouns(tweet_message)
print("tweet_okt : ", tweet_okt)


