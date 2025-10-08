+ delta
+ 기본적으로 append 
+ Delta Lake 란?
+ Delta table 이란 ?
델타 레이크가 제공하는 저장 형식을 이용해 데이터를 테이블 형태로 나타낸 것
델타 레이크는 데이터 레이크의 데이터 관리 및 안정성을 향상시키는 오픈 소스 스토리지 계층입니다. 델타 테이블은 델타 레이크에 저장된 데이터를 표현하는 테이블의 한 형태



#### Deltalake vs Data Lake
+ Data Lake     
스키마 온 리드(Schema on Read) 방식, 데이터를 읽을 때 스키마가 유추
스키마가 변경되는 데이터를 지원할 때 스키마 온 리드 방식은 불리

+ Deltalake 
Delta Lake 테이블은 쓰기 시 스키마(schema on write) 방식, 스키마 정의 후 데이터 읽는 방식 
Delta Lake는 다른 스키마를 가진 데이터가 추가된 경우 읽기 가능
Parquet 파일을 개별적으로 열지 않고 트랜잭션 로그를 쿼리 후 테이블의 최종 스키마를 생성



