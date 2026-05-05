# 데이터 엔지니어 면접 질문 정리

> 하둡, 스토리지, 스트리밍, 워크플로우 영역에서 자주 받는 질문을 주제별로 묶어 정리한 문서

## 문서 성격

- 실무 경험을 바탕으로 답변 초안을 쌓아두는 메모
- 정답집이 아니라 설명 흐름을 다듬기 위한 초안

## 데이터 플랫폼 기초

### 1. 하둡과 전통적인 RDBMS의 차이점은?

- 데이터 타입
  - 하둡: 정형, 반정형, 비정형 데이터 처리
  - RDBMS: 정형 데이터 처리 중심
- 스키마
  - 하둡: Schema on Read
  - RDBMS: Schema on Write
- 사용처
  - 하둡: 대용량 배치 처리, 데이터 레이크
  - RDBMS: OLTP, 강한 트랜잭션 처리
- 속도
  - 하둡: 쓰기와 확장성에 강함
  - RDBMS: 정형 데이터 조회와 트랜잭션에 강함

### 2. 빅데이터의 4V 또는 5V는 무엇인가요?

- Volume: 데이터 양
- Velocity: 데이터 생성·처리 속도
- Variety: 데이터 다양성
- Veracity: 데이터 신뢰성
- Value: 데이터가 만드는 가치

### 3. 대용량 데이터 분석에 하둡이 유리한 이유는?

- 분산 파일 시스템 기반 저장이 가능함
- 여러 Node에 부하를 나누어 처리할 수 있음
- 스케일 아웃 구조에 유리함
- 상용 RDBMS 대비 비교적 저렴한 인프라로 확장 가능함

### 4. 정형, 반정형, 비정형 데이터의 차이는?

- 정형 데이터: 스키마가 명확하고 행·열 구조를 가진 편임
- 반정형 데이터: XML, JSON, 로그처럼 메타데이터 구조가 포함됨
- 비정형 데이터: 이미지, 영상, 자유 텍스트처럼 정해진 스키마가 없음

### 5. 하둡은 어떤 개념으로 동작하나요?

- HDFS와 MapReduce를 중심으로 동작함
- 데이터가 있는 위치로 연산을 가져가는 구조를 지향함

### 6. Hadoop의 핵심 구성 요소는?

- Hadoop Common
- HDFS
- YARN
- MapReduce

## 데이터 처리와 저장

### 7. 하둡 스트리밍이란?

- Java 외 언어로 MapReduce 작업을 작성할 수 있게 해주는 인터페이스임
- Python, Ruby, Shell 스크립트 등을 사용할 수 있음

### 8. 하둡 입력 포맷에는 무엇이 있나요?

- Text Input Format
- Key Value Input Format
- Sequence File Input Format

### 9. OLAP와 OLTP의 차이는?

- OLTP: 실시간 트랜잭션 처리 중심
- OLAP: 저장된 데이터를 분석하는 목적 중심

### 10. ETL과 ELT의 차이는?

- ETL: 변환 후 적재
- ELT: 적재 후 변환
- ELT는 Data Lake와 결합할 때 유연성이 높음

### 11. Avro와 Parquet의 차이는?

- Avro: 행 기반, 쓰기와 직렬화에 유리
- Parquet: 열 기반, 분석 조회에 유리

### 12. 사용해본 저장소는?

- Elasticsearch
- HDFS
- S3

## Spark / Hive / MapReduce

### 13. Spark와 Hive의 차이는?

- Hive는 SQL 기반 데이터 웨어하우스 계층에 가까운 편임
- Spark는 범용 분산 처리 엔진임
- Hive는 배치 분석, Spark는 ETL·ML·스트리밍까지 범위가 넓은 편임

### 14. Spark와 MapReduce의 차이는?

- Spark는 메모리 기반 처리에 강함
- MapReduce는 디스크 기반 배치 처리에 강함
- 반복 연산은 Spark가 유리함

### 15. MapReduce는 왜 쓰나요?

- 대규모 데이터를 여러 Node에 분산 처리하기 위해 사용함
- Map 단계에서 가공하고 Reduce 단계에서 집계함

### 16. Spark Session과 Spark Context의 차이는?

- Spark Session은 Spark 2.x 이후의 통합 진입점임
- 내부적으로 Spark Context와 SQL Context를 감싼음

### 17. 파티셔닝과 버킷팅의 차이는?

- Partition: 디렉터리 단위 분리
- Bucketing: 해시 기반 파일 단위 분산
- 필터링 위주면 파티션, 조인/균등 분산이 중요하면 버킷팅을 본음

### 18. Spark에서 JDBC와 Connector의 차이는?

- 커넥터는 대용량 병렬 처리에 유리함
- JDBC는 단순 연결과 소규모 작업에 적합함
- Redshift 같은 시스템은 전용 커넥터가 보통 더 효율적임

## Kafka / Streaming

### 19. Kafka를 설명해보세요

- 분산 메시지 큐 또는 이벤트 스트리밍 플랫폼임
- Producer, Broker, Topic, Consumer 구조로 동작함

### 20. Kafka 로드밸런싱을 하는 이유는?

- 트래픽 분산
- 처리량 확보
- 장애 대응

### 21. Spark Streaming과 Kafka를 함께 쓰는 이유는?

- 실시간 데이터 수집과 처리 파이프라인을 만들기 쉽음
- 이벤트 기반 아키텍처에 잘 맞음

## 인프라 / 운영

### 22. 내·외부 네트워크를 나누는 이유는?

- 보안 분리
- 트래픽 제어
- 운영 안정성 확보

### 23. 모니터링 서비스 사용 경험은?

- Kibana를 통한 시각화
- Teams, Mail, Slack 알림 연동
- 장애 상황별 후처리 자동화

### 24. Airflow worker가 죽었을 때 어떻게 대응하나요?

- 로그 확인
- task retry 정책 확인
- executor / worker 상태 점검
- 큐 적체와 리소스 부족 여부 확인

### 25. Airflow worker는 어떻게 동작하나요?

- Scheduler가 작업을 큐에 넣음
- Worker가 큐에서 작업을 가져와 실행함
- Executor 종류에 따라 실행 방식이 달라짐

## 검색

### 26. 검색 시스템의 전체 흐름을 설명해보세요

- 데이터 수집
- 전처리 및 정규화
- 색인
- 검색 API 제공
- 백엔드 응답 가공
- 프론트엔드 노출

### 27. 검색 API 설계 시 중요한 점은?

- 응답 시간
- 정렬·필터·집계 지원
- 페이징 전략
- 장애 시 fallback 방식
