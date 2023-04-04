from konlpy.tag import Okt, Kkma

okt = Okt()  ## 단어 개별 분석
kkma = Kkma()  ## 단어 중복 분석
txt = "사고싶다"
tweet_okt = okt.nouns(str(txt))
# tweet_kkma = kkma.nouns(tweet_message)
print("tweet_okt : ", tweet_okt)