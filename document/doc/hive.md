### Title: Apache Hive
#### Date : 2023-08-01  

## hive  
- 동작방식  
         ![img_41.png](..%2Fplatform%2Fimg%2Fimg_41.png)      
</br></br>  


#### 개념정리/Architecture   
- HQL : Java에 통합하는 데 필요한 SQL 추상화를 제공
- metastore :  Hive가 구동될때 필요한 테이블의 스키만 구조나 다양한 설정값이 저장    
       기본적으로 Hive는 포함된 Apache Derby 데이터베이스에 메타데이터를 저장(MySQL 과 같은 다른 클라이언트/서버 데이터베이스를 선택적으로 사용)        
      Meta Store에 장애가 발생할 경우 Hive는 정상적으로 구동되지 않음, MySQL, Oracle, PostgreSQL 등에 저장, 3가지 실행모드로 동작   
      - 임베디드(Embedded) : 더비DB를 이용한 모드이며 한번에 한명의 유저만 접근이 가능(derby: metastore 저장하는 db, default)         
         ![img_38.png](..%2Fplatform%2Fimg%2Fimg_38.png)   
      - 로컬(Local): 별도의 데이터베이스를 가지고 있지만 하이브 드라이버와 같은 JVM에서 동작        
         ![img_39.png](..%2Fplatform%2Fimg%2Fimg_39.png)      
      - 리모트(Remote) : 별도의 데이터베이스를 가지고, 별도의 JVM에서 단독으로 동작하는 모드, 리모트로 동작하는 메타스토어를 HCat서버 있음       
         ![img_40.png](..%2Fplatform%2Fimg%2Fimg_40.png)      
- hivewearhouse   
</br></br>  

#### hive query tunning solution
- Vectorized Query Execution(벡터화) :  일반적인 쿼리 작업의 CPU 사용량을 크게 줄이는 기능, 한 번에 1024행 블록을 처리    
 설정방법 : _set hive.vectorized.execution.enabled = true;_  
 사용가능 타입 : tinyint, smallint, int, bigint, boolean, float, double, decimal, date, timestamp, string    
- Hive 버킷팅: 대형 데이터 집합을 클러스터 또는 세그먼트하여 쿼리 성능을 최적화    
- 조인 최적화  
- 리듀서 증가 
</br></br>  

##### HQL vs SQL 
- SQL(Structured Query Language) : RDBMS라고도 하는 관계형 데이터베이스 관리 시스템에 저장된 데이터를 관리       
- Hive 쿼리 언어(HiveQL) : HiveQL은 메타 저장소에서 구조화된 데이터를 분석하고 처리하기 위한 Hive용 쿼리 언어     
, TEXT FILE, SEQUENCE FILE, ORC 및 RC FILE(Record Columnar File)의 네 가지 파일 형식을 지원      
![img_42.png](..%2Fplatform%2Fimg%2Fimg_42.png)
</br></br>  

##### Solved Problem 
- 사건 :데이터 건수는 8만건정돈데 vertex error 가 발생 한다.  
특정 : 총 column 개수는 20개, row는 8만, 특정 column value 전부 null 값      
환경 : tez, hive   
해결방법 : - "set hive.vectorized.execution.enabled=true;" 으로 백터화 disable 처리      
        - 전부 null value 로만 된 컬럼 찾아 case when으로 공백 처리    
![img_37.png](..%2Fplatform%2Fimg%2Fimg_37.png)
- 
- 

