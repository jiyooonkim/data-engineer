## Build shopping dictionary and data engineering Project

### Good basic data brings good results!!
Developers with the ability to create good data in big databases.

- skill : hadoop, pyspark, python
- version and env : python3.9, pyspark3.3.2, hadoop3.3.4, airflow, jenkins

### 목적 및 방향성
- 좋은 기초(학습) 데이터 생성 목적(Only by Data Engineering)
- 데이터 연관성 및 특징 파악 훈련
- AI/ML 지식 획득 및 데이터 훈련 
- 안정적인 플랫폼(Hadoop, python, spark, hive..)구축 및 운영 방법 모색 



### 구성 및 작업 내용
- Develop
  - (도량형)속성 사전 구축
  - 검색 내부 키워드(inner keyword)
  - cosine similarity
  - Jaccard Similarity : 추천 키워드, 연관 상품, 연관 키워드
  - Noisy Channel Model : 정타<->오타 매핑 사전(오타사전), 카테고리 중심 유사 단어 추출(get noise keyword, stopword)
  - tf-idf
  - word2vec(with skip-gram & cbow)

- Platfrom
  - Hadoop 
  - Spark 
  - CI/CD(Jenkins)
  - Airflow

#### todo list  ...ing  
- 개발
   - ai/ml 적용하여 학습 예정(커버리지 늘리기 위해)
   - Flask 로 간단한 API 구성
   - 유의어, 동의어, 외래어 사전
   - 브랜드, 시리즈, 모델명 사전
   - 불용어(stopword) 
   - ngram

- 환경 세팅(platform)
     - Docker 설치
     - Local 에서 하둡 운영 방식 정하기 (script 구성 예정)
     - ElasticSearch 설치
     - Kibana 설치
     - Docker 설치 및 기본기 이해
     - MLOPS
   
 - 이론 공부
    - BigData
      - Kafka
      - spark
      - hadoop
      - hive
      - streaming(sparkstreaming, Hudi, Kafka)
      - sqoop
      - 