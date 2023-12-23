# -*- coding: utf-8 -*-
"""
    # title : 정수 인코딩
    # date : 2023-12-21
    # desc : - 텍스트를 숫자로 컨버팅 하는 기법
    # doc : - https://velog.io/@hoegon02/%EC%9E%90%EC%97%B0%EC%96%B4%EC%B2%98%EB%A6%AC-13-%ED%85%8D%EC%8A%A4%ED%8A%B8-%EC%A0%84%EC%B2%98%EB%A6%AC-%EC%A0%95%EC%88%98-%EC%9D%B8%EC%BD%94%EB%94%A9
        - 다양한 기법들 존재 : 각 토큰(단어)들 마다 인덱스 부여 (랜덤 또는 빈도수 desc 기준)
"""
import re

from nltk.tokenize import (sent_tokenize, word_tokenize)
from nltk.corpus import stopwords


class NltkTokenize:
    def __init__(self):
        self.sentences = ""
        self.sentence = ""
        self.tokens = []
        self.nltk_pakage_name = "all"

    def word_tokenize(self, sentences):
        """
            공백, 특수문자, 단위로 토크나이징
            :return: ['나는', 'ML', '머신Learning', 'Developer', '입니다', '감사합니다', '!!...', ',', '..', '.', '(', ')', '!!', '.', '~~!@#', '$', '%', '^*^']
        """
        def remove_spaces(stc):
            return re.sub('\s+', " ", stc).strip().split(" ")

        norm_char = re.sub("[^ㄱ-힣a-zA-Z0-9]", " ", sentences)
        special_char = re.sub("[ㄱ-힣a-zA-Z0-9]", " ", sentences)
        res = remove_spaces(norm_char) + remove_spaces(special_char)
        return res

    def sent_tokenize(self, sentences):
        """
            문장단위 토크 나이징
            문장의 마침에 올 수 있는 특수문자 : .!?
        """
        return re.findall('.*?[.!?] .*?', sentences)

    def download_nltk_lib(self, nltk_pakage_name):
        """ Download nltk lib"""
        import nltk
        nltk.download(self.nltk_pakage_name)  # nltk 내에 모든 라이브러리 다운받기
        # nltk.download('stopwords')  # nltk 내에 일부 라이브러리 다운받기


if __name__ == "__main__":
    texts = "나는!!...데이터 엔지니어 입니다, ..NLP 개발자 입니다. ML(머신Learning) Developer 입니다!! 감사합니다. ~~!@# $ % ^*^"
    self_nltk = NltkTokenize()

    # print("origin : ", self_nltk.sent_tokenize(sentences=texts))



    print("custom   : ",  self_nltk.word_tokenize(texts))
    print("lib      : ", word_tokenize(texts))
