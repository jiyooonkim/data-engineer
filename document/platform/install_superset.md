# Superset 설치

> Docker 기반으로 Apache Superset을 띄우고 기본 로그인까지 확인하는 절차 정리

## 문서 정보

- 작성일: 2024-10-17
- 환경: macbook pro14 m2 (silicon)

## 핵심 포인트

- Superset은 시각화와 BI 대시보드 도구임
- 로컬 환경은 Docker Compose 기반 설치가 가장 단순함
- Hive 같은 외부 엔진 연결은 설치 이후 별도 설정 단계로 보는 편이 좋음

## 설치 순서

### 1. 저장소 clone

```bash
git clone https://github.com/apache/superset.git
cd superset
```

### 2. Docker 준비

- Docker Desktop 실행
- 리소스 제한이 너무 낮지 않은지 확인

### 3. Compose 실행

```bash
docker-compose -f docker-compose-non-dev.yml pull
docker-compose -f docker-compose-non-dev.yml up
```

## 로그인 메모

- 기본 정보 예시: `admin / admin`

## 활용 메모

- Hive, MySQL, PostgreSQL 같은 외부 데이터 소스 연결 가능함
- 대시보드 용도면 쿼리 성능과 캐시 전략을 같이 봐야 함
