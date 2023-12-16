# -*- coding: utf-8 -*-
"""
    # title : 원-핫 인코딩
    # date : 2023-12-16
    # desc : https://velog.io/@hoegon02/%EC%9E%90%EC%97%B0%EC%96%B4%EC%B2%98%EB%A6%AC-14-%ED%85%8D%EC%8A%A4%ED%8A%B8-%EC%A0%84%EC%B2%98%EB%A6%AC-%EC%9B%90-%ED%95%AB-%EC%9D%B8%EC%BD%94%EB%94%A9
    # doc : keras library 에서 제공해주는 원-핫 인코딩 함수를 이해하고 알고리즘 개발
"""

import tensorflow as tf
from keras.preprocessing.text import Tokenizer
from keras.utils import to_categorical


"""
나랑 점심 먹으러 갈래 점심 메뉴는 햄버거 갈래 갈래 햄버거 최고야
갈래 3
점심 2
햄버거 2 

나랑

먹으러

메뉴는

최고야

"""


def get_sequences():
    '''
        :return: word_index 결과로 부터 포함하는 단어 인덱스 Return 하기

    '''
    return 0


class Tokeniz:
    def __init__(
            self,
            sentence=None,
            num_words=None,
            lower=True,
            split=" ",
            **kwargs
    ):
        # self.word_counts = collections.OrderedDict()
        # self.word_docs = collections.defaultdict(int)
        # self.filters = filters
        self.split = split
        self.lower = lower
        self.num_words = num_words
        self.sentence = sentence
        self.tokens = []
        # self.document_count = document_count
        # self.char_level = char_level
        # self.oov_token = oov_token
        # self.index_docs = collections.defaultdict(int)
        # self.word_index = {}
        # self.index_word = {}

    def tonkenizing(self):
        import re
        self.sentence = re.sub('[^A-Za-z0-9가-힣 ]', '', self.sentence.lower())
        self.tokens = re.sub(' +', ' ', self.sentence).split(self.split)
        # return self.tokens

    def get_index(self):
        """ get word count after sorting word count(word count descending, kor descending)"""
        counts = dict()
        for wd in self.tokens:
            if wd in counts:
                counts[wd] += 1
            else:
                counts[wd] = 1

        result = dict(sorted(counts.items(), key=lambda va: va[1], reverse=True))
        print("result : ", result)

    def get_to_categorical(self):
        """
            :return: sequences 결과에 대한 벡터 리스트 구하기

        """
        return 0

    def texts_to_sequences(self):
        """
        :return:
            sub_text = "점심 먹으러 갈래 메뉴는 햄버거 최고야"
            encoded = tokenizer.texts_to_sequences([sub_text])[0]
            print("encoded : ", encoded)
            [2, 5, 1, 6, 3, 7]
            새로 유입된 키워드에 대한 위치 구하기
        """
        return 0


text = "나랑 점심 먹으러 갈래 점심 메뉴는 햄버거 갈래 갈래 hambuger 최고야 !! 13 2434"

tokenizer = Tokenizer()
tokenizer.fit_on_texts([text])
print('단어 집합2 :', tokenizer.word_index)


a = Tokeniz(text)
a.tonkenizing()
a.get_index()
# print(Tokeniz().tonkenizing("cc","bb"))

# sub_text = "점심 먹으러 갈래 메뉴는 햄버거 최고지만 아닌데"
# encoded = tokenizer.texts_to_sequences([sub_text])[0]
# print("encoded : ", encoded)
#
# one_hot = to_categorical(encoded)
# print("one_hot: ", one_hot)
