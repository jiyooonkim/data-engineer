# -*- coding: utf-8 -*-
"""
# title : smtp 를 통한 메일 알림 개발 (for Airflow)
# desc :
"""
import smtplib
from email.message import EmailMessage

# STMP 서버의 url과 port 번호
SMTP_SERVER = 'smtp.gmail.com'

SMTP_PORT = 465

smtp = smtplib.SMTP('smtp.gmail.com', 587)

smtp.ehlo()

smtp.starttls()
EMAIL_ADDR = 'write email....'

# 2. SMTP 서버에 로그인
smtp.login(EMAIL_ADDR, EMAIL_PASSWORD)

# 3. MIME 형태의 이메일 메세지 작성
message = EmailMessage()
message.set_content('이메일 본문')
message["Subject"] = "이메일 제목"
message["From"] = EMAIL_ADDR  # 보내는 사람의 이메일 계정
message["To"] = 'write email....'

# 4. 서버로 메일 보내기
smtp.send_message(message)

# 5. 메일을 보내면 서버와의 연결 끊기
smtp.quit()