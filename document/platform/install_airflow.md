# Airflow 설치

> 로컬 환경과 Docker 환경에서 Airflow를 띄우는 기본 절차 정리

## 문서 정보

- 작성일: 2023-05-04
- 환경: macbook pro14 m2 (silicon)

## 핵심 포인트

- Airflow는 워크플로우 오케스트레이션 도구임
- 빠른 테스트는 `airflow standalone`, 운영형 로컬 구성은 Docker Compose가 편함
- Webserver, Scheduler, Executor, Worker 역할을 분리해서 이해하는 게 중요함

## Airflow 구성

- Webserver: UI 제공
- Scheduler: DAG 스케줄링
- Executor: Task 실행 전략 관리
- Worker: Task 실제 실행
- Database: 메타데이터 저장

## 로컬 설치

### 빠른 실행

```bash
export AIRFLOW_HOME=~/airflow
AIRFLOW_VERSION=2.7.2
PYTHON_VERSION="$(python --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
airflow standalone
```

### 수동 구성

```bash
airflow db migrate
airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com
airflow webserver --port 8080
airflow scheduler
```

## Conda 기반 설치 메모

```bash
conda create --name airflow_env python=3.8 -y
conda activate airflow_env
pip install "apache-airflow==2.2.3" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-no-providers-3.8.txt"
airflow db init
```

## Docker Compose 설치

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.6.3/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
docker compose up
```

## 설정 메모

### DAG 폴더 지정

```ini
dags_folder = /Users/jy_kim/airflow/dags
```

### 예제 DAG 숨기기

- `airflow.cfg`에서 `load_examples = False`로 변경

## MySQL 연동 메모

1. MySQL 설치
2. `airflow.cfg`의 database URL 변경
3. 메타데이터 테이블 생성 확인

## 용어 메모

- DAG: Directed Acyclic Graph
- Scheduler: 실행 시점 관리
- Executor: 실행 방식 결정
- Worker: Task 실행 주체
