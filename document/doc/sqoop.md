### Title:  Sqoop 설치 
#### Env : macbook pro14 m2(silicon)
#### Date : 2023-07-01  
<br/>

## Sqoop 
- Rdb 와 hdfs 간에 파일 전송 사용을 위한 툴
- export, import 가능 

#### Sqoop 동작방법
- Import(Rdb -> hdfs)
  1. Client로 부터 sqoop import 요청 
  2. select * from *** 를 통해 RDBMS로 부터 메타데이터 확보 (1개의 row에 대한 정보 호출)
  3. MapReduce 실행
  4. Map task 동작(map key 생성)
  5. Reduce 방식으로 hdfs에 적재

  
- Export(hdfs -> Rdb)
  1. Client로 부터 sqoop client 요청 
  2. 최종 rdbms로 부터 select * from *** 를 통해 메타데이터 및 건수 확보
  3. MapReduce 실행 
  4. n개의 map task 단위로 insert table1 (select * from ***) 쿼리로 Map task 동작 
  5. Reduce 방식으로 rdbms에 적재