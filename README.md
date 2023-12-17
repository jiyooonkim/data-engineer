## Build shopping dictionary and data engineering Project

### Good basic data brings good results!!
Developers with the ability to create good data in big databases.

- skill : hadoop, pyspark, python, shell, hive
- version : python3.9, pyspark3.3.2, hadoop3.3.4, airflow, jenkins

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
  - Ngram (with Liked Prediction) 

- Develop
  - Dictionary     
    - 합성어
    - 모델명
    - 외래어 
    - 불용어(stopword)
  - scraping
  - ML
    - one_hot_encoding
  
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
    
  - NLP
    - Negative Sampling
    - Stemming(어간추출)
  - ML
    - RNN
    - TenserFlow
    - keras
    - Scikit-Learn
    - 선형회귀분석
    - 딥 러닝, 랜덤 포레스트, 그래디언트 부스팅 트리, k-평균 클러스터링 등)과 해당 알고리즘의 개발, 검증 및 평가에 대한 깊은 이해
  - Search
    - Elasticsearch 검색 엔진 구축 및 이해 
    - 상품 검색 Inner Keyword 매핑 
    - 오타 교정
    - 형태소 분석기

- 환경 세팅(Platform)
     - MLops
     - Mongodb
- Language    
  - Java

- 이론 공부
   - BigData
     - Kafka
     - streaming(spark streaming, Hudi, Kafka)
     - devops
     - dataops
     - airflow with ci/cd
 
##### 번외      
 - Data Engineer로서 겪은 고뇌의 Moment and Future(Q&A)  
    
      