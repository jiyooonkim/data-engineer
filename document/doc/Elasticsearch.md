### Elasticsearch
#### Date : 2023-09-21  
#### Doc : https://coding-start.tistory.com/176      
https://esbook.kimjmin.net/02-install/2.1   
 

###  Elasticsearch    
+ 분산 시스템 
+ 클러스터 최소 Node 수 : 3개

         


#### 시스템 구조
+ Elasticsearch Cluster     
<img src = "img/Elasticsearch_cluster.png" width = "350" height = "140"/>   
  + Cluster
    + 3개 이상의 Node가 모여 있는 것 
    + 각 Node는 단일 Elasticsearch 인스턴스 말함
    + 중간 규모 최적화
      + Data Node
      + Master Node
      + Codinator Node : 데이터 노드 부담 감소하여 성능 향상, 실행쿼리/로드밸런싱 실행쿼리 조정
      + 수집 노드 : Logstash 처럼 사용
      + 기계 학습 노드
    + 대규모 최적화(중대형 노드 40개 이상 경우)
      + 
  + Replica
    + 일반적으로 샤드 1개, 복재본 1개 자동 생성 
    + 모든 프로덕션 클러스터에 백업용, 하나의 복제본 갖추는 것이 좋음
    + 검색 성능에도 도움, 높은 처리량과 추가 용량 제공 
    + 추후 수정 가능 
  + Shard
    + 인덱스(=문서모음) 하위 요소로 분할
    + Cluster 여러 Node에 분산
    + 샤드가 Node에 배열되는 방식을 자동 관리, 균형 유지
    + 나중에 수정 불가(수정시, ReIndexing 필요)
    + CPU 리소스 차지
    + 검색 요청시 모든 샤드와 상호작용 발생
    + 

##### Index & Shards



##### Node
+ Master Node
  + 
+ Data Node 
  + 


##### 형태소 분석(Stemming)

##### 토크나이저(Tokenizer)
  + 퍼포먼스 궁금
#####  

##### 용어정리
+ term : 
+ bucket : 
+ metrics : 
+ query dsl : 

