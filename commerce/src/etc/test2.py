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

print(tkns_cnt1)