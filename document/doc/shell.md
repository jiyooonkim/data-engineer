# Shell

> 터미널 환경을 이해하기 위해 자주 나오는 셸 개념을 간단히 정리한 문서

## 문서 정보

- 작성일: 2023-07-15

## 핵심 포인트

- Shell은 사용자와 운영체제 사이의 인터페이스임
- Bash는 가장 널리 쓰이는 유닉스 계열 셸 중 하나임
- 로그인 셸과 비로그인 셸의 설정 파일 로드 방식은 다름

## Shell이란

- 운영체제에서 사용자 명령을 읽고 해석해 실행하는 프로그램임
- CLI 환경에서 가장 직접적으로 다루는 인터페이스임

## Bash Shell이란

- Bourne Again Shell의 약자임
- 유닉스 계열 환경에서 많이 사용됨

## 자주 보는 설정 파일

### `.bashrc`

- 로그인 이후 새 터미널 세션에서 주로 로드됨

### `.bash_profile`

- 로그인 셸 시작 시 로드됨

### `.profile`

- 공통 초기화 파일로 사용되기도 함

```bash
if [ -f ~/.bashrc ]; then
  . ~/.bashrc
fi
```

## Login Shell vs Non-Login Shell

- Login Shell: 로그인 과정과 함께 시작되는 셸
- Non-Login Shell: 로그인 없이 실행되는 셸
