### Apache hadoop 설치
#### env : macbook pro14 m2(silicon)
#### date : 2023-02-04
#### desc : hadoop ver. 3.3.4, jdk ver. zulu-11.jdk  
#### reference   
<br/><br/>

## Hadoop
<br/><br/>

## Hadoop Install  
1. hadoop 설치 : brew install hadoop    
    ** homebrew install 명령어 : /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"  
2. 버전 및 경로확인 : brew info hadoop      
     <img src = "img/img_9.png" width = "350" height = "140"/>  
3. jdk 설치  
4. 환경 변수 설정      
   4-1. 환경변수 디렉토리 이동 : cd /opt/homebrew/Cellar/hadoop/3.4.0/libexec/etc/hadoop  
    <img src = "img/img_10.png" width = "350" height = "140"/>    
   4-2. JAVA_HOME 추가 : vi hadoop-env.sh  >  export JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"    
   4-3. 파일설정    
   * vi core-site.xml   
       ```    
       <configuration>
         <property>
           <name>fs.defaultFS</name>
           <value>hdfs://localhost:9000</value>
         </property>
       </configuration>
       ```  
   * vi hdfs-site.xml
      ```    
      <configuration>
        <property>
          <name>dfs.replication</name>
          <value>1</value>
        </property>
      </configuration>
     ```     
   * vi mapred-site.xml
      ```    
      <configuration>
        <property>
          <name>mapreduce.framework.name</name>
          <value>yarn</value>
        </property>
        <property>
          <name>mapreduce.application.classpath</name>
          <value
          >      $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value
        >
        </property>
      </configuration>
      ```  
   * vi yarn-site.xml  
     ```    
        <configuration>
          <property>
            <name>yarn.nodemanager.aux-services</name>
            <value>mapreduce_shuffle</value>
          </property>
          <property>
            <name>yarn.nodemanager.env-whitelist</name>
            <value
          >      JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value
        >
          </property>
      </configuration>
     ```    
5. ssh 확인 및 실행  
    5-1.   
        <img src = "img/img_11.png" width = "350" height = "40"/>      
    5-2. mac 에서 5-1 이 실행안된다면 : 환경설정 > 공유 > 원격로그인 활성   
    5-3. ssh keygen 발급  
    ```   
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 0600 ~/.ssh/authorized_keys  
    ```  
6. hadoop 실행 명령  
   - cd /opt/homebrew/Cellar/hadoop/3.4.0/libexec/sbin    
   - 실행 : ./start-all.sh  
   - 실행 확인   
     - jps     
        <img src = "img/img_12.png" width = "350" height = "140"/>      
     - namenode 확인 : http://localhost:9870/dfshealth.html#tab-overview    
        <img src = "img/img_13.png" width = "350" height = "140"/>     
     - Secondary NameNode 확인 : http://localhost:9868/status.html  
        <img src = "img/img_15.png" width = "350" height = "140"/>     
     - 리소스 매니저 확인 : http://localhost:8088/cluster  
        <img src = "img/img_14.png" width = "350" height = "140"/>     
<br/><br/> 

## Hadoop 제어 명령어  
- 네임노드 포멧 : hadoop namenode -format ([hadoop java.net.connectexception:연결이 거부됨] 에러 발생시 해결방법)
- jdk 경로 확인 : cd /Library/Java/JavaVirtualMachines  
<br/>

## Hadoop with Hive
+ java8, hive3.1.3, hadoop3.4.0 버전 기준 (2024.10 기준)       


## NameNode(NN) 이중화    
- NameNode 비정상 종료, 데이터노드 포멧, 서버가 내려갈 때 등 대비하고자    
  2대의 서버를 이용해 이중화로 데이터 노드 구성 [Active-Standby]   
- Active node 에서 Stanby node 로 상태 전이  
<br/><br/>

## Hadoop 정합성 확보 전략
+ Hadoop Snapshot 생성 : Stage Area, Data wearhouse load 직전/후, data Source 등 스냅샷 생성/관리
  + hdfs snapshot은 파일은 시스템 읽기 전용 복사본   
  + 데이터가 복사되는 것 아님, 파일 블록 목록과 크기를 기록 
+ Count 나 집계 쿼리 사용 : 특정 컬럼의 데이터 개수, 전체 개수, 합산 집계 등을 통한 비교
+ Hdfs size check : 디렉터리 크기, 파일 사이즈, 유무 체크, checksum(체크섬)    
<img src = "img/checksum.png" width = "350" height = "40"/>     
+ Hdfs 상태 확인 : fsck 명령 ```hdfs fsck /user/hive/warehouse/test_check```  또는 ```hdfs fsck /directory_name/```      
<img src = "img/hadoop_fsck.png" width = "350" height = "240" alt="checksum 예시"/>        
+ hadoop replication(복제) : 복제 노드 개수 확장






### 용어 정리  
- hdfs : 하둡 분산형 파일 시스템(Hadoop Distributed File System), HDFS는 데이터에 대한 액세스를 제공하는 하둡의 파일 시스템, 하둡의 모듈  
- hadoop : 데이터 저장, 처리 및 분석할 수 있는 오픈 소스 프레임워크  
- datanode : 데이터 처리
- namenode : 데이터 정보와 속한 블록 크기, 데이터의 이동 위치, 권한(읽기, 쓰기, 제거, 복제) 갖고 있는 노드
- Secondary NameNode : HDFS 작업을 트랜잭션 로그를 기록하는 역할, Stand by Namenode 와는 다른 것    
- tez : 비동기 사이클 그래프 프레임 워크, excution engine 
- mapreduce : 수천대 기계에서 병렬 처리 위한 프레임워크, 배치지향성, map운 (Key, Value) 형태 생성, Reduce는 map 에서 나온 key값으로 데이터 추출하는 역할 ,reduce 과정에서 shuffle 발생 
- yarn : Task들을 관리 및 스케쥴링하고 각각의 작업에 사용될 자원을 적절히 분산하여 관리해주는 기능
yarn의 구성은 Resource Manager, Node Manager, Timeline Server
- Stand alone(Local) : 독립실행모드, 단일 시스템에만 설치한다는 의미이기도, 가장 빠르게 작동, 
- Pseudo-Distributed Mode : 독립실행모드, 단일 노드 클러스터, 클러스터가 시뮬레이션됨, Namenode와 Resource Manager는 Master로 사용하고 Datanode와 Node Manager는 Slave로 사용
- Fully-Distributed Mode : 완전분산모드   
- JVM :  자바 가상 머신,  자바 프로그램 실행환경을 만들어 주는 소프트웨어     
- yarn : yarn 메뉴들 설명/역할           
- hive on mr : I/O 과정에서 reduce 과정에서 write 발생        
- hive on tez : I/O 과정에서 write 미발생, 속도향상   
     <img src = "img/img_35.png" width = "350" height = "140"/>