# Apache Tez

> Hive와 Pig 실행 성능을 높이기 위해 등장한 DAG 기반 실행 엔진 정리

## 문서 정보

- 작성일: 2023-08-03

## 핵심 포인트

- Tez는 DAG 기반으로 작업을 구성해 MapReduce 오버헤드를 줄인음
- Hive와 Pig의 실행 엔진으로 자주 사용됨
- 중간 결과를 매번 HDFS에 쓰지 않아 지연 시간을 줄일 수 있음

## Tez란

- MapReduce 기반 처리의 한계를 줄이기 위한 오픈소스 실행 프레임워크임
- YARN 위에서 동작함
- 낮은 지연 시간과 높은 처리량을 목표로 함

## Tez vs MapReduce

- Tez는 DAG 기반으로 복잡한 작업 흐름을 더 자연스럽게 표현함
- 중간 데이터의 불필요한 디스크 쓰기를 줄인음
- 컨테이너 재사용이 가능해 실행 효율이 높음

![Tez vs MapReduce](img/img_47.png)

## 왜 사용하는가

- 작업 시작 오버헤드 감소
- 불필요한 HDFS I/O 감소
- Hive 쿼리 실행 성능 개선

## Hive에 적합한 파일 형식

- ORC
- Parquet
- Avro
- Text

## 파일 형식 메모

- Parquet: 열 기반, 읽기 최적화
- Avro: 행 기반, 쓰기와 직렬화에 유리
- ORC: 열 기반, 압축 효율이 좋고 Hive와 궁합이 좋음
