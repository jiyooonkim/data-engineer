# Apache Hadoop 설치

> macOS 로컬 환경에서 Hadoop 단일 노드 구성을 맞추는 절차 정리

## 문서 정보

- 작성일: 2023-02-04
- 환경: macbook pro14 m2 (silicon)
- 버전 메모: Hadoop 3.3.x / JDK Zulu 11

## 핵심 포인트

- 로컬 테스트는 pseudo-distributed 모드 기준으로 접근하면 됨
- `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml` 설정이 핵심임
- SSH 설정이 빠지면 로컬 단일 노드 실행이 막히는 경우가 많음

## 설치 순서

### 1. Hadoop 설치

```bash
brew install hadoop
brew info hadoop
```

### 2. JDK 설치 및 JAVA_HOME 설정

```bash
cd /opt/homebrew/Cellar/hadoop/3.4.0/libexec/etc/hadoop
vi hadoop-env.sh
```

```bash
export JAVA_HOME="/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home"
```

### 3. 설정 파일 수정

#### `core-site.xml`

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

#### `hdfs-site.xml`

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

#### `mapred-site.xml`

```xml
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
```

#### `yarn-site.xml`

```xml
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
```

### 4. SSH 설정

```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
```

### 5. 실행

```bash
cd /opt/homebrew/Cellar/hadoop/3.4.0/libexec/sbin
./start-all.sh
jps
```

## 확인 포인트

- NameNode: `http://localhost:9870`
- Secondary NameNode: `http://localhost:9868`
- ResourceManager: `http://localhost:8088`

## 운영 메모

- `hadoop namenode -format`은 필요 시에만 수행
- Snapshot, count, checksum, `fsck`를 같이 써야 정합성 점검이 쉬움

## 용어 메모

- HDFS: 분산 파일 시스템
- NameNode: 메타데이터 관리
- DataNode: 실제 블록 저장
- YARN: 리소스 관리와 스케줄링
- MapReduce: 배치 병렬 처리 프레임워크
