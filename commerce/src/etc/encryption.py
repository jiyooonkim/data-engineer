"""
    # title : 개인정보에 적용할 암/복호화 테스트
    # doc :
    # desc :
    # Refer :
        - https://openuiz.tistory.com/121
        - https://velog.io/@wijoonwu/ModuleNotFoundError-No-module-named-Crypto-%EC%98%A4%EB%A5%98-%ED%95%B4%EA%B2%B0%EB%B0%A9%EB%B2%95
        - DB : https://createseok.tistory.com/entry/MARIA-DBDB-%EC%95%94%ED%98%B8%ED%99%94-%EB%B3%B5%ED%98%B8%ED%99%94
"""
import crypto
import sys
sys.modules['Crypto'] = crypto
from crypto.Random import get_random_bytes
from crypto.Cipher import AES

import base64
import hashlib

BS = 16
pad = (lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS).encode())
unpad = (lambda s: s[:-ord(s[len(s) - 1:])])


class AESCipher(object):
    def __init__(self, key):
        self.key = hashlib.sha256(key.encode()).digest()

    def encrypt(self, message):
        message = message.encode()
        raw = pad(message)
        cipher = AES.new(self.key, AES.MODE_CBC, self.__iv().encode('utf8'))
        enc = cipher.encrypt(raw)
        return base64.b64encode(enc).decode('utf-8')

    def decrypt(self, enc):
        enc = base64.b64decode(enc)
        cipher = AES.new(self.key, AES.MODE_CBC, self.__iv().encode('utf8'))
        dec = cipher.decrypt(enc)
        return unpad(dec).decode('utf-8')

    def __iv(self):
        return chr(0) * 16

key = "this is key"
data = "암호화 대상문장 입니다"

aes = AESCipher(key)

encrypt = aes.encrypt(data)
print("암호화:", encrypt)
print("-"*100)

decrypt = aes.decrypt(encrypt)
print("복호화:", decrypt)
print("-"*100)