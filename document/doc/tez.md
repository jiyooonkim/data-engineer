### Apache Tez
#### Date : 2023-08-03
## Tez
+ MapReduce 기술을 기반으로 하는 빅 데이터 처리를 위한 오픈 소스 프레임워크
+ Hive & PIG의 실행 엔진
+ YARN 위에서 실행
+  낮은 대기 시간, 높은 처리량 성능 및 사용 편의성

+ 동작방식
+ Tez on hive, Spark
  +  Pig 나 Hive 같은 application 은 TEZ 와 Spark 위에서 동작 가능   
+ Tez VS MR   
  + MapReduce 및 Tez는 대규모 데이터 처리를 위해 Apache Hive에서 널리 사용되는 두 가지 실행 엔진
  + tez는 intermediate map task 없다
  + Tez : Map -> Memory(결과저장) -> Reduce -> Reduce(최종 결과 저장)
  + MR : Mape -> Reduce ->hdfs(결과저장) -> Map -> Reduce(최종 결과 저장)
  <img height="440" src="img/img_47.png" width="350"/>

### Map Reduce 대신 Apache Tez 사용 이유
- MapReduce 엔진에서 단일 작업으로 DAG(방향성 비순환 그래프)를 실행    
- Tez는 해당 제약 조건이 없으며 작업 시작 오버 헤드를 최소화하는 하나의 작업으로 복잡한 DAG를 처리 가능  
- 불필요한 쓰기를 방지       
 MapReduce는 중간 데이터에 대한 HDFS에 write 과정 있음       
 Tez는 각 Hive 쿼리에 대한 작업 수를 최소화하므로 불필요한 없음
- 시작 지연을 최소화, Tez는 시작하는데 필요한 매퍼의 수를 줄여 시작 지연 시간을 최소화할 수 있음
- 컨테이너를 다시 사용, 가능한 경우 Tez는 컨테이너를 다시 사용하여 컨테이너 시작 대기 시간 감소
- 연속 최적화 기술

### Hive에 적합한 파일 형식 
- ORC/Parquet: 성능에 가장 적합 
- 텍스트: 기본 파일 형식, 
- Avro: 상호 운용성 시나리오에 대해 적합
