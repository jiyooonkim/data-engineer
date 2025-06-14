## Apache Hive
##### 2023-08-01  

#### Hive  
- 동작방식  
         ![img_41.png](..%2Fplatform%2Fimg%2Fimg_41.png)      
</br></br>  


#### Hive Architecture   
+ Hive는 HDFS 파일 형식을 소유하지 않는다.     
  + I/O 방식   
    + 사용자는 다른 도구를 사용하여 Hive 테이블에서 HDFS 파일을 직접 read
    + 다른 도구를 사용하여 "CREATE EXTERNAL TABLE"을 통해 Hive에 load
    + "LOAD DATA INPATH"를 통해 hivewarehouse 에 load   
+ HQL(Hive-SQL) : Java에 통합하는 데 필요한 SQL 추상화를 제공  
+ Hive Warehouse : 실제 메타데이터(테이블 데이터)가 저장된 위치, Hive를 사용하기 전 HDFS에서 디렉터리 생성 필요    
+ Metastore   
  + Hive가 구동될 때 필요한 테이블의 스키만 구조나 다양한 설정값이 저장      
  + 기본적으로 Hive는 포함된 Apache Derby 데이터베이스에 Metadata 저장(MySQL 과 같은 다른 클라이언트/서버 데이터베이스 선택적 사용)           
  + Meta Store에 장애가 발생할 경우 Hive는 비정상적 구동, MySQL, Oracle, PostgreSQL 등에 저장, 3가지 실행모드로 동작     
    + 임베디드(Embedded) : 더비DB를 이용한 모드이며 한번에 한명의 유저만 접근이 가능(derby: metastore 저장하는 DB, default)         
    ![img_38.png](..%2Fplatform%2Fimg%2Fimg_38.png)   
    + 로컬(Local): 별도의 데이터베이스를 가지고 있지만 하이브 드라이버와 같은 JVM에서 동작        
    ![img_39.png](..%2Fplatform%2Fimg%2Fimg_39.png)      
    + 리모트(Remote) : 별도의 데이터베이스를 가지고, 별도의 JVM에서 단독으로 동작하는 모드, 리모트로 동작하는 메타스토어를 HCat서버 있음       
    ![img_40.png](..%2Fplatform%2Fimg%2Fimg_40.png)      
  
#### Hive 내부구조
+ <img src="./img/img_54.png" title="hive 내부구조"/>
+ <img src="./img/img_55.png" title="hive 내부구조"/> 
  

##### HQL & SQL 
- SQL(Structured Query Language) : RDBMS라고도 하는 관계형 데이터베이스 관리 시스템에 저장된 데이터를 관리       
- Hive 쿼리 언어(HiveQL) : HiveQL은 메타 저장소에서 구조화된 데이터를 분석하고 처리하기 위한 Hive용 쿼리 언어, equal join 만 가능        
, TEXT FILE, SEQUENCE FILE, ORC 및 RC FILE(Record Columnar File)의 네 가지 파일 형식을 지원      
![img_42.png](..%2Fplatform%2Fimg%2Fimg_42.png)
</br></br>  

##### Tunning HQL & SQL   
+ HQL      
  + 조건절 내의 UDF 제거    
  + DISTINCT COUNT 연산 피하기    
   COUNT(DISTINCT column) 함수를 이용하면 1개 리듀서에서 정보를 처리, GROUP BY 함수를 이용한 방법 지향     
  + JOIN 사용시 고려할 점    
   가장 큰 데이터의 테이블을 마지막에 놓거나 /*+STREAMTABLE(a)*/ 옵션을 이용     
   아웃 조인시 조인 수행후 WHERE 조건을 처리하기 때문에 중첩 SELECT 문을 이용해 먼저 데이터를 필터링 후 조인을 진행하도록 처리    
  + SELECT 사용 시 고려사항     
   *를 이용해 모든 데이터를 가져오지 않고, 필요한 칼럼 데이터만 선택    
  + Vectorized Query Execution(벡터화) :  일반적인 쿼리 작업의 CPU 사용량을 크게 줄이는 기능, 한 번에 1024행 블록을 처리      
   설정방법 : set hive.vectorized.execution.enabled = true;     
   사용가능 타입 : tinyint, smallint, int, bigint, boolean, float, double, decimal, date, timestamp, string    
  + Hive 버킷팅: 대형 데이터 집합을 클러스터 또는 세그먼트하여 쿼리 성능을 최적화    
  + 조인 최적화  
  + 리듀서 증가 

+ SQL
  + 불필요한 인덱스 줄이기
  인덱스는 DDL문의 성능에 안 좋은 영향을 주고, Nested Loops Join을 이용하게 될 수 있기 때문에 적절한 수의 인덱스를 선언
  + WHERE 조건에서 함수 피하기
  + OR 조건 피하기
</br></br>  


##### Solved Problem
+ 문제 :데이터 건수는 약 8만건 정도 vertex error 가 발생 한다.  
  + 특징 : 총 column 개수는 20개, row는 8만, 특정 column value 전부 null 값   
  + Null은 카디널리티가 매우 높고 맵 측 집계가 제대로 작동하지 않는 경우 맵 축소 경계에서 폭발이 발생할 수 있음  
   Thrift는 맵에서 null을 지원하지 않으므로 객체 관계형 매핑(ORM)에서 검색된 맵에 있는 null은 가지치기하거나 빈 문자열로 변환 필요
  + 환경 : apache tez, apache hdfs, apache hive   
  + 해결방법 
    + "set hive.vectorized.execution.enabled=true;" 으로 백터화 disable 처리      
    + 전부 null value 로만 된 컬럼 찾아 "case when"으로 공백 처리   
    ![img_37.png](..%2Fplatform%2Fimg%2Fimg_37.png)

+ 문제 : hive external table에는 partitionby 존재, hivewarehouse에는 파티션단위 파일이 아닐경우 데이터가 dbeaver에서 보이지 않는다.
  + 환경 : apache tez, apache hdfs, apache hive   
  + 해결방법 
    1. DDL에 partitionby를 제외
    2. hivewarehouse에 파티션단위(디렉토리)단위 적재

+ 문제 : select count(*) from temp_table 시, 데이터는 있는데 카운팅이 되지 않은 경우   
  + 특징 : 결과가 0건으로 나옴 
  + 환경 : apache tez, apache hdfs, apache hive   
  + 해결방법
    + "Analyze table temp_table;", 메타데이터가 갱신되지 않아 통계정보를 못 읽는 현상으로 Analyze 명령 통해 수동으로 통계 정보 업데이트 필요      

+ 문제 :  select count(*) ... 개수 != select * from .. 의 전체 개수, 단순 select 시 전체 Row 개수와 count(*) 쿼리 실행시 개수가 다름    
  + 환경 : apache tez, apache hdfs, apache hive      
  + 해결방법
    + "ANALYZE TABLE  table_name  PARTITION(partition_name) COMPUTE STATISTICS;" 

+ 문제 : DDL생성(hive,glue), 데이터 적재(hivewarehouse, S3) 후 파티션있는 테이블 조회(Select)시 결과 안나오는 경우   
  + 특징 : 
  + 환경 : apache tez, apache hdfs, apache hive  / AWS : S3, Athena, Glue 
  + 해결방법
    + "MSCK REPAIR table_name" 쿼리 수행
    **+ 카탈로그의 메타데이터를 업데이트 필요



##### Managed table & External table
+ Managed table
  + hive.metastore.warehouse.dir 경로에 존재, 해당 디렉터리 하위에 테이블의 데이터가 저장
  + 해당 경로에 테이블이 만들어지고 테이블을 삭제하는 경우 hdfs 경로에 있는 데이터 역시 함께 삭제
+ External table
  + hive.metastore.warehouse.dir 경로에 생성되지 않음   
  + 테이블 생성시 Location을 지정   
  + hive 테이블 제거해도 실제 hdfs상에 있는 데이터는 지워지지 않고 그대로 유지


##### Hive Transcation(Up Hive 0.13, Hive3.0 이상 추천)
+ HDFS에 파일은 수정 불가능 하나, warehouse tool 사용으로 HDFS에 있는 데이터 조회, 삽입, 삭제 변경 가능하도록 함
  + base
  + delta
    + 변경분 데이터 저장 
    + 테이블 수정작업 늘수록 delta 파일 계속 쌓임, NameNode부하 (해결방안: HDFS 성능 위해 압축)
+ 트랜잭션
  + 데이터베이스 연산
+ ACID    
  ┗ Atomicity(원자성) : 성공적인 트랜잭션 처리, 아닐 경우 미처리     
  ┗ Consistency(일관성) : (분산된) 데이터를 일관된 상태로 전환해주는지 여부     
  ┗ Isolation(독립성) : 한꺼번에 동시에 운영되는 다른 트랜젝션과 무관하게 실행 가능한지 여부     
  ┗ Durability(지속성) : 트랜젝션 처리 후에도 결과가 그대로 유지되는지 여부     
+ Hive 트랜잭션 처리 순서    
  ① 테이블 및 파티션 데이터 기본 파일 세트에 저장    
  ② insert/update/delete 결과 델타 파일로 저장    
  ③ read 시점에 기본파일과 델타 파일 합쳐 최신 데이터 반환       
+ 트랜잭션 가능한 Hive DDL 생성 옵션   
  + 예시)    
  ``` 
    CREATE TABLE test_hive_table 
      (id int) 
    CLUSTERED BY (id) INTO 10 BUCKETS STORED AS ORC TBLPROPERTIES (
      "transactional"="true",
      "compactor.mapreduce.map.memory.mb"="2048",
      "compactorthreshold.hive.compactor.delta.num.threshold"="4",
      "compactorthreshold.hive.compactor.delta.pct.threshold"="0.5"
    )  
  ``` 
+ 트랜잭션 설정 (/etc/hive/conf/hive-site.xml)
  + 예시)    
  ``` 
    hive.compactor.initiator.on=true (for metastore)
    hive.compactor.worker.threads=10 (for metastore)
    hive.support.concurrency=true (for hive-server2,client)
    hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager (for hive-server2, client)
    hive.exec.dynamic.partition.mode = nonstrict (for hive-server2, client)  
  ``` 

##### hive 쿼리 동작과정
![../img/hive_qry_working.png](../img/hive_qry_working.png)    
1. Execute Query 
- 사용자가 Hive web이나 커맨드라인을 통해 하이브 데이터베이스로 쿼리를 날린다. (데이터베이스와의 연결은 JDBC같은 아무런 드라이버나 사용해도 된다.)
2. Get Plan
-  드라이버는 컴파일러에게 쿼리 플랜을 요청한다. 쿼리 컴파일러는 쿼리를 받아서 쿼리를 어떻게 처리할 것인지 쿼리 플랜을 작성한다. 
3.4. Get Metadata
- 쿼리 컴파일러는 Metastore로부터 쿼리를 처리하는데 필요한 메타정보를 받는다. 
5. Send Plan
- 컴파일러는 쿼리 플랜을 작성해서 드라이버에게 전달한다. 
6. Execute Plan
- 드라이버는 Execution Engine에게 쿼리 플랜을 전달한다. 
7. Execute Job
- 이제부터는 쿼리가 내부적으로 맵리듀스 잡으로 변환되어 실행된다. Execution Engine은 네임노드에 있는 JobTracker에게 잡을 전달한다. 그리고 JobTracker는 데이터 노드에 있는 TaskTracker에게 잡을 임명한다. 
8. Fetch Result
- Execution Engine은 맵 리듀스 처리 결과를 데이터노드로부터 받는다. 
9. Send Results
- Execution Engine은 드라이버에게 데이터노드로 부터 받은 결과들을 전달한다.
10. Send Results
- 드라이버는 하이브 인터페이스에게 결과들을 전달한다.


##### 용어정리
+ Serde  
  + Serializer(직렬화) 및 Deserializer(역직렬화) 약자
  + 파일 읽을 때 FileFormat 이용
  + Serde 과정
    + Deserializer로 원천 데이터를 테이블 포맷에 맞는 로우로 변환(HDFS files --> InputFileFormat --> [key, value] --> Deserializer --> Row object)    
    + Serializer로 Key, Value 형태로 변환후 FileFormat 이용하여 저장 위치에 씀(Row object --> Serializer --> [key, value] --> OutputFileFormat --> HDFS files)   
  + serde 종류(7가지) :Avro, ORC, RegEx, Thrift, Parquet, CSV, JsonSerDe  
  + 이 외 기본 LazySimpleSerDe 또는 Custom serde도 구현 가능
  
+ Serializaion(직렬화) : 객체를 저장, 전송할 수 있는 특정 포맷 상태로 바꾸는 과정    
+ Deserialization(역직렬화) : 특정 포맷 상태의 데이터를 다시 객체로 변환하는 것     
+ Hive Client 
  + Thrift server : Hive server, client 요청 받아 Hive Driver에 제공하는 역할 
  + JDBC Driver : Hive 와 Java 연결 역할 
  + ODBC Driver : Hive 와 ODBC 프로토콜 지원하는 역할  
+ Hive Services   
  + Hive Web UI(HWI) : 인터페이스 기반 작업, Hive2.2.0 이전만 사용가능 
  + Hive Server : 원격 클라이언트가 Hive에 연결하여 쿼리 사용 방식, 최신 버전 사용가능        
               HiveServer1, HiveServer2 있음 
               Beeline
      + HiveServer1 VS HiveServer2
        + HiveServer1
          + Hive CLI를 사용
          + Thrift API 사용
        + HiveServer2
          + Beeline 클라이언트를 사용
          + Thrift API 외에도 JDBC와 ODBC를 통해 클라이언트와 통신 
  + CLI : HiveServer1 하고만 통신      
  + Hive Driver : HiveQL 파싱, 컴파일, 최적화, 실행, 관리    
        <img height="440" src="img/img_46.png" width="350"/>    
+ Ranger : 하둡 플랫폼 전반에서 포괄적인 데이터 보안을 지원, 모니터링 및 관리하는 프레임워크     
+ Log4j : 자바 기반 로깅 유틸리티, 로깅 활성화하면 문제의 위치를 정확히 파악할 수 있음, 5단계의 로그 레벨 제공 
  + DEBUG < INFO < WARN < ERROR <  FATAL
  + DEBUG: 애플리케이션의 내부 실행 상황 추척할 수 있는 상세정보
  + INFO: 애플리케이션의 주요 실행 정보 
  + WARN: 잠재적 위험 상태. 경고.
  + ERROR: 오류 발생했지만, 애플리케이션 실행은 가능한 상태 
  + FETAL: 애플리케이션을 중지해야할 심각 오류 
  + TRACE: debug보다 상세한 정보를 찍고 싶을때.
+ Bucketing : 지정된 칼럼의 값을 해쉬 처리하고 지정한 수의 파일로 나누어 저장하는 방법, Join을 하거나 샘플링 작업을 할 경우 성능 향상
  + Partition vs Bucketing
    + Partition: 데이터를 디렉토리로 나누어 저장
    + Bucketing: 데이터를 파일별로 나누어 저장 
+ Skew : 데이터 편향 현상
  + Skew vs Partition 
    + 파티션: 주로 데이터를 크게 구분하는 용도로 사용, 보통 일자별로 구분할 때 많이 사용    
    + 스큐: 칼럼의 데이터를 구분할 때 사용, 하나의 칼럼에 특정 데이터가 몰려서 생성될 때 사용, 특정컬럼 이외 컬럼 묶어 관리 할 수 있어 Namenode 효율적 사용 가능   
+ HiveContext : Hive 데이터로 작업할 수 있는 Spark SQL 모듈(Spark과 연결시 사용)