### Title: spark 설치
#### Env : macbook pro14 m2(silicon)
#### Date : 2023-02-04
#### Desc : Spark version 3.4.0, Python 3.9.16, Jdk(Zulu11.64)  

## spark
- 대규모 데이터 처리 통합 분산 엔진  
- 다양한 언어와 함께 사용가능
- 인메모리 아키텍처
- 오픈소스 프레임워크
- RDD(탄력적 뷴산형 데이터셋)
- 아키텍처
- 사용이유
<br/><br/>

###  <Spark Cluster 개념 (동작방식)>
- Step 1. 자원할당 : spark context -> cluster manager -> executor 할당  
- Step 2. Task 실행 : spark context -> 실행 파일, 설정을 executor 전달 -> task 실행  
<br/><br/>

### Spark Install Flow(Stand Alone) & Running  
1. https://spark.apache.org/downloads.html 에 Download Spark > 압축풀기  
![img/img_16.png](img/img_16.png)  
2. 설치파일 경로 이동 : cd /Users/jy_kim/Downloads/spark-3.4.0-bin-hadoop3  
3. spark-env.sh 파일설정   
     - cd /Users/jy_kim/Downloads/spark-3.4.0-bin-hadoop3/conf  
     - vi spark-env.sh   
       ```  
        export PYSPARK_PYTHON=/usr/bin/python3.9
        export PYSPARK_DRIVER_PYTHON=/usr/bin/ipython
        export SPARK_WORKER_INSTANCES=3   # worker 개수    
        ```  
4. worker 실행  : cd /Users/jy_kim/Downloads/spark-3.4.0-bin-hadoop3/sbin  
     4-1. 마스터 실행 : ./start-master.sh 후 http://localhost:8080/ 접속     
     4-2. 메모리 & 코어수 지정 : ./start-slave.sh spark://jy-kimui-MacBookPro.local:7077  -m 256M -c 2  
        <img src = "img/img_17.png" width = "470" height = "190" title = "SPARK_WORKER_INSTANCES 개수만큼 worker 생성" />
5. pyspark shell 을 실행시키거나 python file 실행  
    <img src = "img/img_18.png" width = "470" height = "190" title = "실행화면" />
 
<br/><br/>
### 용어 정리/개념 설명
- #### Shuffle
    발생 하는 경우 : 파티션에 데이터 재배치 될 때 발생, 맵리듀스에서 리듀스 단계 중 물리적 데이터 이동시
- #### <파티션의 개념 및 차이점>  
  - partition() : 코어수에 따라 할당
  - paritionby(column) : 디스크 데이터를 분산 할 때 , 속도 향상 , write 함수  
  - repartition(partition count, column)  : 메모리에서 데이터 분산할 때  
  - coalesce: 디폴트는 파티션 수 감소할 때만 사용, numofpartition = true 시에는 파티는 증가도 가능  
               데이터 개수에 따라 다르지만, 분할보다 병합 비용이 더 큼
- #### <메모리 설정 팁>  
  - 전체 Executor 개수 설정
  - Executor 당 Core 개수 와 Memory 크기 설정
  - 셔플 읽기+쓰기 사이즈 < excutor 수  
  - (Spark Executor 수) X (Spark Core 수) < (서버 Node 개수) X (서버 Core 개수)
- #### <인메모리(memory, spark) vs 분신병렬(disk, hadoop)>  
  맵리듀스가 메모리에서 기반인가, 디스크에서 기반인가 의 차이   
- #### <Sql 보다, spark 엔진을 사용해야하는 이유>  
    Spark 은 다양한 엔진 및 라이브러리를 제공  
    python, java, R 등 개발 언어와 함께 사용 할 수 있음  
    Sql 단순 데이터베이스 조회에 적합-> sql 만 쓰면 그것만 하게 됨 spark 만큼 확장성 없어    


- Executor : task 들이 수행, 디스크/메모리에 동작하는 곳
- In-Memory(인메모리) : 메모리 내에서 데이터 저장, 연산 병렬 처리 하는 것, 전력 소모 낮은편
- REPL : Read Eval Print Loop 
- master 인스턴스