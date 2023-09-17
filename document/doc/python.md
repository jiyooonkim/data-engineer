### Python
#### Date : 2023-09-03  

## Python 이란?  
+ 인터프리터 언어 
  + 기계어 변환과정 없이 한 줄씩 해석하여 실행하는 언어
  + Build 과정 없음
  + 컴파일 언어비해 속도 느림 (한줄씩 읽기 때문)
  + 스크립트 언어 : 소스코드(바이트코드로 컴파일) -> 인터프리터 (번역/실행) -> cpu/memory
+ 동적타이핑(Dynamic Typing)
  + 변수 자료형 불필요 : 코드가 실행되는 시점에 결정(실행도중 타입 안맞을 경우 오류 발생)
    + 동적 타입 : 자료형 명시 필요
+ 독립적 플랫폼
  + Linux, Unix, Windows, Mac.. 대부분 운영체제(OS)에서 동작가능
  + 별도 컴파일 할필요없어 어떠한 OS에서도 활용 가능
  + 

#### Compile Language vs Script Language 
+ Compile Language  
  + compile 과정 통해 사람이 작성한 코드를 기계어로 번역하여 실행하는 언어
    + compile : 고수준 언어(C, C++, Java, Go..)를 저수준 언어(기계어)로 변환하는 작업  
  + OS 고려
  + 실행속도 빠름
+ Script Language  
  + 컴파일러 없이 한줄씩 읽어 바로 실행하는 인터프리터 방식 (JavaScript, python, JSP, jQuery...)
  + 소스코드가 그대로 실행파일이 되어 메모리 적재
  + OS 미고려
  + 실행속도 느림

#### Cpython/Jython/pypy
+ Cpython: python 내부는 보통 C언어로 구현되어있음
+ Jython : Java로 구현한 python, JVM 에서 실행할 수 있음
+ pypy : python 으로 구현한 python
 