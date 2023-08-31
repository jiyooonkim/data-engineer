## Build shopping dictionary and data engineering Project

### Good basic data brings good results!!
Developers with the ability to create good data in big databases.

- skill : hadoop, pyspark, python, shell, hive
- version and env : python3.9, pyspark3.3.2, hadoop3.3.4, airflow, jenkins

### 목적 및 방향성
- 좋은 기초(학습) 데이터 생성 목적(Only by Data Engineering)  
- NLP 기반으로 가설 수립 후 개발 주도    
- 데이터 연관성 및 특징 파악 훈련  
- AI/ML 지식 획득 및 데이터 훈련   
- 안정적인 플랫폼(Hadoop, python, spark, hive..)구축 및 운영 방법 모색  

### 구성 및 작업 내용
- NLP
  - (도량형)속성 사전 구축
  - 검색 내부 키워드(inner keyword)
  - cosine similarity
  - Jaccard Similarity : 추천 키워드, 연관 상품, 연관 키워드
  - Noisy Channel Model : 정타<->오타 매핑 사전(오타사전), 카테고리 중심 유사 단어 추출(get noise keyword, stopword)
  - tf-idf
  - word2vec(with skip-gram & cbow)

- Develop
  - Dictionary     
    - 합성어
    - 모델명
    - 외래어 
  
- Platfrom & Doc
  - Hadoop 
  - Spark 
  - CI/CD(Jenkins)
  - Airflow 
  - Docker
  - sqoop
  - Elasticsearch
  - kibana
  - shell
  - Hive

### Todo list & ...ing  
- Develop  
  - Dictionary      
    - 유의어
    - 동의어
    - 브랜드 
    - 시리즈
    - 불용어(stopword)
  - NLP
    - ngram 
    - Negative Sampling
  - ML
    - ML 적용하여 학습 예정(커버리지 늘리기 위해)
  - Search
    - Elasticsearch 검색엔진 구축 및 이해 
    - 상품 검색 Inner Keyword 매핑
    - 오타교정
    - 형태소 분석기 
  - 그 외
    - Flask 로 간단한 API 구성

- 환경 세팅(platform)
     - MLOPS
     - Mongodb
- Language    
  - Java

 - 이론 공부
    - BigData
      - Kafka
      - streaming(spark streaming, Hudi, Kafka)
      - devops
 
##### 번외      
 - Data Engineer로서 겪은 고뇌의 Moment and Future(Q&A)  
    
      