# Apache Spark 설치

> Standalone 기준 Spark 로컬 실행 환경을 맞추는 절차와 핵심 개념 정리

## 문서 정보

- 작성일: 2023-02-04
- 환경: macbook pro14 m2 (silicon)
- 버전 메모: Spark 3.4.0 / Python 3.9.16 / Zulu JDK 11
- 관련 문서: [Spark](/Users/jykim/Documents/private/data-engineer/document/doc/spark.md)

## 핵심 포인트

- 로컬 실습은 Standalone 모드가 가장 단순함
- `spark-env.sh`와 Worker 설정이 핵심임
- 설치보다도 Shuffle, Skew, Spill 개념을 같이 이해해야 운영에 도움됨

## 설치 순서

### 1. 다운로드

- [Spark Download](https://spark.apache.org/downloads.html)

### 2. 압축 해제 후 경로 이동

```bash
cd /Users/jy_kim/Downloads/spark-3.4.0-bin-hadoop3
```

### 3. `spark-env.sh` 설정

```bash
cd conf
vi spark-env.sh
```

```bash
export PYSPARK_PYTHON=/usr/bin/python3.9
export PYSPARK_DRIVER_PYTHON=/usr/bin/ipython
export SPARK_WORKER_INSTANCES=3
```

### 4. Master / Worker 실행

```bash
cd ../sbin
./start-master.sh
./start-slave.sh spark://jy-kimui-MacBookPro.local:7077 -m 256M -c 2
```

### 5. 확인

- Master UI: `http://localhost:8080`
- `pyspark` 또는 Python 파일로 실행 테스트

## 개념 메모

### Shuffle

- 파티션 간 데이터 재배치 시 발생
- `join`, `groupBy`, `sort`, `repartition`에서 자주 발생

### Skew

- 특정 파티션으로 데이터가 몰리는 현상임
- repartition, bucketing, broadcast join으로 완화 가능함

### Spill

- 메모리가 부족해 디스크로 임시 밀어내는 현상임
- Task 지연과 성능 저하 원인이 됨

## 설정 메모

- `Spark Executor 수 x Core 수`와 서버 자원을 함께 봐야 함
- 파티션 수는 데이터량과 셔플 양 기준으로 조정하는 편이 좋음
