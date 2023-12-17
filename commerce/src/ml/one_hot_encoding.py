# -*- coding: utf-8 -*-
"""
    # title : 원-핫 인코딩
    # date : 2023-12-16
    # desc : - https://velog.io/@hoegon02/%EC%9E%90%EC%97%B0%EC%96%B4%EC%B2%98%EB%A6%AC-14-%ED%85%8D%EC%8A%A4%ED%8A%B8-%EC%A0%84%EC%B2%98%EB%A6%AC-%EC%9B%90-%ED%95%AB-%EC%9D%B8%EC%BD%94%EB%94%A9
    #        - keras library 에서 제공해주는 원-핫 인코딩 함수를 이해하고 알고리즘 개발
    # doc :
        - 원핫 인코딩 : 복잡한 데이터를 그대로 사용하지 않고 컴퓨터가 처리하기 쉽게 숫자로 변형해 주는 것
        ex) '아파트', '연립주택', '다세대주택', '단독주택', '다중주택', '다가구주택', '기타'
            '아파트'는 '100000'으로 '연립주택'은 '010000'으로 변환
        -
"""

import tensorflow as tf
from keras.preprocessing.text import Tokenizer
from keras.utils import to_categorical


class Tokeniz:
    def __init__(
            self,
            sentence=None,
            num_words=None,
            lower=True,
            split=" ",
    ):
        self.split = split
        self.lower = lower
        self.num_words = num_words
        self.sentence = sentence
        self.tokens = []
        self.word_index = {}
        self.sub_word_index = []

    def tonkenizing(self, texts):
        import re
        if texts is not None:
            self.sentence = texts
            self.sentence = re.sub('[^A-Za-z0-9가-힣 ]', '', self.sentence.lower())
            self.tokens = re.sub(' +', ' ', self.sentence).split(self.split)
            return self.tokens

    def get_index(self):
        """ get word count after sorting word count(word count descending, kor descending)"""
        counts = dict()
        for wd in self.tokens:
            if wd in counts:
                counts[wd] += 1
            else:
                counts[wd] = 1

        result = sorted(counts.items(), key=lambda va: va[1], reverse=True)
        self.word_index = dict((value[0], i + 1) for i, value in enumerate(result))

    def texts_to_sequences(self, texts):
        """
        :return:
            sub_text = "점심 먹으러 갈래 메뉴는 햄버거 최고야"
            encoded = tokenizer.texts_to_sequences([sub_text])[0]
            print("encoded : ", encoded)
            [2, 5, 1, 6, 3, 7]
            새로 유입된 키워드에 대한 위치 구하기
        """

        sub_sentence = self.tonkenizing(texts)
        print("sub_sentence : ", sub_sentence)
        print("self.word_index : ", self.word_index)
        for i in sub_sentence:
            for j, value in enumerate(self.word_index):
                if i == value:
                    self.sub_word_index.append(j + 1)
        # print("self.sub_word_index : ", self.sub_word_index)

    def get_to_categorical(self):
        """
            :return: sequences 결과에 대한 벡터 리스트 구하기
        """

        return 0


text = "나랑 점심 먹으러 갈래 점심 메뉴는 햄버거 갈래 갈래 햄버거 최고야"
sub_text = "점심 먹으러 갈래 메뉴는 햄버거 최고야"

""" by keras library """
tokenizer = Tokenizer()
tokenizer.fit_on_texts([text])
encoded = tokenizer.texts_to_sequences([sub_text])[0]
one_hot = to_categorical(encoded, 15)
print("one_hot : ", one_hot)
print("encoded : ", encoded)

""" delvelop one hot ending  """
self_Tokeniz = Tokeniz()
self_Tokeniz.tonkenizing(text)
self_Tokeniz.get_index()

self_Tokeniz.texts_to_sequences(sub_text)
# print(Tokeniz().tonkenizing("cc","bb"))

# sub_text = "점심 먹으러 갈래 메뉴는 햄버거 최고지만 아닌데"
# encoded = tokenizer.texts_to_sequences([sub_text])[0]
# print("encoded : ", encoded)
#
# one_hot = to_categorical(encoded)
# print("one_hot: ", one_hot)
