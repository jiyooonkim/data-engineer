# -*- coding: utf-8 -*-
"""
    # title : 정수 인코딩
    # date : 2023-12-21
    # desc : - 텍스트를 숫자로 컨버팅 하는 기법
    # doc : - https://velog.io/@hoegon02/%EC%9E%90%EC%97%B0%EC%96%B4%EC%B2%98%EB%A6%AC-13-%ED%85%8D%EC%8A%A4%ED%8A%B8-%EC%A0%84%EC%B2%98%EB%A6%AC-%EC%A0%95%EC%88%98-%EC%9D%B8%EC%BD%94%EB%94%A9
        - 다양한 기법들 존재 : 각 토큰(단어)들 마다 인덱스 부여 (랜덤 또는 빈도수 desc 기준)
"""

from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords


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
        self.encoding_index = 0

    def tonkenizing(self, texts):
        # todo : 소문자화, 불용어와 단어길이 1이하 제거,
        import re
        if texts is not None:
            self.sentence = texts
            self.sentence = re.sub('[^A-Za-z0-9가-힣 ]', '', self.sentence.lower())
            self.tokens = re.sub(' +', ' ', self.sentence).split(self.split)
            return self.tokens

    def get_word_count(self):
        """
            단어 빈도수 구하기
            딕셔너리 구조로  {apple : 3 }
        """
        return 0

    def check_out_of_voca(self):
        """
            :return: 단어 집합에 없는 단어 찾기 (OOV : out of index)
        """


    def get_index(self):
        """  """
        counts = dict()
        for wd in self.tokens:
            if wd in counts:
                counts[wd] += 1
            else:
                counts[wd] = 1

        result = sorted(counts.items(), key=lambda va: va[1], reverse=True)
        self.word_index = dict((value[0], i + 1) for i, value in enumerate(result))
        print("get_index: ", self.word_index)

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
        print("texts_to_sequences: ", max(self.sub_word_index))
        return self.sub_word_index

    def get_to_categorical(self, **kwargs):
        import numpy as np
        """
            :return: sequences 결과에 대한 벡터 리스트 구하기
        """
        if 'index' in kwargs:
            self.sub_word_index = kwargs.get("index")

        arr = np.array(self.sub_word_index)
        res = np.zeros((len(self.sub_word_index), max(self.sub_word_index)+1), dtype=int)
        res[np.arange(arr.size), self.sub_word_index] = 1


text = "나랑 점심 먹으러 갈래 점심 메뉴는 햄버거 갈래 갈래 햄버거 최고야"
sub_text = "점심 먹으러 갈래 메뉴는 햄버거 최고야"

""" by keras library """
tokenizer = Tokenizer()
tokenizer.fit_on_texts([text])
encoded = tokenizer.texts_to_sequences([sub_text])[0]
one_hot = to_categorical(encoded, 15)
# print("one_hot : ", one_hot)
# print("encoded : ", encoded)

""" delvelop one hot ending  """
self_Tokeniz = Tokeniz()
self_Tokeniz.tonkenizing(text)
self_Tokeniz.get_index()


encd = self_Tokeniz.texts_to_sequences(sub_text)
self_Tokeniz .get_to_categorical( text="test")
# print(Tokeniz().tonkenizing("cc","bb"))

# sub_text = "점심 먹으러 갈래 메뉴는 햄버거 최고지만 아닌데"
# encoded = tokenizer.texts_to_sequences([sub_text])[0]
# print("encoded : ", encoded)
#
# one_hot = to_categorical(encoded)
# print("one_hot: ", one_hot)
