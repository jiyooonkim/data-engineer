### Data Engineer로서 겪은 고뇌의 시간들



**Airflow**    
* 요구 사항 : 직접 개발하지 않고 UI 로 컨트롤 할 수 있는 Airflow 기능 만들어 달라
  * Q. https://github.com/lattebank/airflow-dag-creation-manager-plugin 사용가능한가요?    
    A. 불가능 합니다. Airflow 1.x 에서만 사용 가능한 것이고 2.x에서는 불가능 합니다.    
        Airflow 에서 제공하는 Rest Api 로는 간단한 CRUD 작업은 가능해보이나 실무에 적용할 만한 대상은 아닌것 같습니다.    
        (Airflow Rest API 와 Flask를 이용하여 Airflow 웹 컨트롤러?는 해볼만 하다 생각합니다. )    
  </br>
  * Q. 서버에 접속하지 않고, 직접 개발 하지 않고 Airflow UI에서 마우스 클릭으로만 작업을 하고 싶습니다. 가능한가요?   
    A. 참... 글쎄요.. 저도 그런게 있으면 좋겠습니다. 위 기능을 만드신 분이 있다면 저도 공유 부탁드립니다.    
    </br>
  * Q. 운영에 있어 Data pipeline 및 Airflow 쉬운 관리 방법이 있을까요?    
    A. 현재 상황은 하나의 Dag 안에 많은 task 과정들이 있습니다.     
       그런데 일부의 Dag들을 보면 유사한 부분이 많습니다. (생산적이 못한 부분입니다. 시간이 흐를수록 수면위로 올라오게 될 것 입니다.)     
       먼저, 표준이 (공통 소스나 스크립트)가 있어야 한다고 생각합니다.(현재도 존재하긴 하지만 과연 진짜 common 한지 판단이 필요해보입니다.)    
       다음으로, 과정 간소화 작업이 필요하다 생각합니다. 모든 작업의 일련과정을 모니터링 또는 log를 남기기 위해 세분화 해두었다지만   
       정말 필요한 요소인지 판단이 필요해보입니다.     
       좋은 도구와 기능은 많습니다. 현재 상황에 적합한 도구인지 판단이 중요한 것 같습니다.   
    </br>
  * Q. 세분화 하여 Airflow task 마다 실행시키고 있는데요. 효율적으로 바꿀 수있는 방안이 있을까요?   
    A. Airflow는 데이터를 원하는 시간에 순서와 작업방식에 따라 순차적으로 실행시켜주는 Data 오케스트레이터입니다.    
       또한, 많은 메모리를 수용하지 못하고, 단순 데이터 처리 프레임워크가 아닙니다.    
       빅데이터를 다룰땐 Task 의 효율적인 배치도 중요하기 때문에 불필요한 Job 생성은 적적한 방법이 아니라 생각합니다.    
    </br>
  * Q. Airflow에서 안정적인 ETL 방법이 있을까요?    
    A. 데이터를 ETL 할 땐 리소스 분배가 중요합니다. 타겟 DB가 무엇인지 적재 방법이 어떻게 되는지 알고 작업을 진행하는게 중요하다 생각합니다.   
       예를들어, Hive를 사용할 경우, Yarn을 통해 작성한 쿼리가 Hadoop 위에서 어떻게 동작하는지 파악하고 그에 맞는 Hivequery 기반 다양한 튜닝 방법이 필요합니다.    
    </br>
  * Q.      
    A. 
    </br>

  * Airflow 는 단순 스케줄링 툴입니다. 내부에 어떤 작업이 어떻게 돌아가고 있는지가 중요합니다.     
    이상입니다.   


**Hive**
  * Q. hdfs put은 overwrite 가 되는데, hivewarehouse에 hdfs로 파일 이동시 append parquet file을 하고 싶습니다.
    A. copyFromLocal 을 사용해보세요.
  </br>






