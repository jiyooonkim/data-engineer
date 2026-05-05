# Apache Hive 설치

> Hadoop과 MySQL Metastore를 기준으로 Hive 로컬 환경을 구성하는 절차 정리

## 문서 정보

- 작성일: 2024-09-25
- 환경: macbook pro14 m2 (silicon)
- 버전 메모: Hive 3.1.3 / Hadoop 3.3.x / JDK 8 / MySQL 9.x

## 핵심 포인트

- Hive는 Hadoop과 Metastore DB가 먼저 준비되어야 함
- 로컬에서는 MySQL을 Metastore로 붙이는 구성이 관리하기 쉬움
- `hive-site.xml` 설정이 실제 연결 성공 여부를 좌우함

## 사전 준비

- Hadoop 설치 완료
- MySQL 설치 완료
- Java 8 준비

## 설치 순서

### 1. Hive 다운로드

- [Hive 3.1.3 다운로드](https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz)

### 2. 환경 변수 등록

```bash
export HIVE_HOME=/Users/jy_kim/Documents/apache-hive-3.1.3/bin
```

### 3. MySQL Metastore 준비

```sql
CREATE DATABASE metastore;
CREATE DATABASE hive;
CREATE USER 'hive'@'%' IDENTIFIED BY 'hive';
GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@'%';
FLUSH PRIVILEGES;
```

### 4. HDFS 디렉터리 생성

```bash
hadoop fs -mkdir /tmp
hadoop fs -mkdir -p /user/hive/warehouse
hadoop fs -chmod g+w /tmp
hadoop fs -chmod g+w /user/hive/warehouse
```

### 5. `hive-site.xml` 설정

```xml
<configuration>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>hdfs://master:9000/user/hive/warehouse</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:3306/hive?createDatabaseIfNotExist=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hive</value>
  </property>
</configuration>
```

## 실행 메모

- `hive`
- `schematool` 초기화 필요 여부 확인
- Metastore 연결 오류가 나면 MySQL 계정과 JDBC URL부터 점검

## 함께 보는 문서

- [Hadoop 설치](./install_hadoop.md)
- [MySQL 설치](./install_mysql.md)
