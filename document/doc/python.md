# Python

> 파이썬 언어의 실행 방식과 기본 개념을 빠르게 훑기 위한 요약 문서

## 문서 정보

- 작성일: 2023-09-03

## 핵심 포인트

- Python은 대표적인 인터프리터 기반 언어임
- 동적 타이핑과 높은 생산성 덕분에 데이터 엔지니어링에서 자주 사용됨
- 구현체와 런타임 특성을 이해하면 실행 환경 차이를 설명하기 쉽음

## Python이란

- 인터프리터 언어임
- 코드 실행 시점에 타입이 결정되는 동적 타이핑 언어임
- Linux, Unix, Windows, macOS 등 다양한 운영체제에서 동작함

## Compiled Language vs Script Language

### Compiled Language

- 소스 코드를 기계어 또는 바이트코드로 변환한 뒤 실행함
- 일반적으로 실행 속도가 빠름

### Script Language

- 인터프리터가 코드를 읽고 실행함
- 개발 속도와 유연성이 높음

## 구현체

- CPython: 가장 널리 쓰이는 기본 구현체
- Jython: JVM 위에서 동작하는 구현체
- PyPy: JIT 최적화를 지원하는 구현체

## 문자열 prefix

- `r`: raw string
- `b`: bytes literal
- `u`: unicode 표기 호환성
- `f`: f-string

```python
name = "JY"
print(f"My name is {name}")
```

## 용어 정리

- Module: `.py` 파일 단위 코드
- Package: 여러 모듈을 묶은 디렉터리
- Library: 재사용 가능한 코드 집합
- Framework: 애플리케이션 구조를 제공하는 개발 골격
