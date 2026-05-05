# Kafka 설치

> Kafka 단일 브로커 로컬 테스트 환경을 띄우는 기본 절차 정리

## 문서 정보

- 작성일: 2024-08-23
- 환경: macbook pro14 m2 (silicon)
- 버전: `kafka_2.12-3.8.0`
- 참고 자료
  - [velog 참고](https://velog.io/@kimview/MacOS-kafka-%EC%84%A4%EC%B9%98)
  - [tistory 참고](https://gray-hat.tistory.com/95)

## 핵심 포인트

- 로컬 테스트는 단일 브로커와 기본 토픽으로 시작하면 됨
- 구버전 방식은 ZooKeeper를 함께 사용함
- 설치보다도 실행 확인과 토픽 테스트가 중요함

## 설치 순서

### 1. 다운로드

- [Apache Kafka Download Link](https://www.apache.org/dyn/closer.cgi?path=/kafka/3.8.0)

### 2. ZooKeeper 실행

```bash
./bin/zookeeper-server-start.sh config/zookeeper.properties
```

### 3. Kafka Broker 실행

```bash
./bin/kafka-server-start.sh config/server.properties
```

### 4. 토픽 생성

```bash
./bin/kafka-topics.sh --create \
  --topic new-topic \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1
```

### 5. 토픽 목록 확인

```bash
./bin/kafka-topics.sh --list --zookeeper localhost:2181
```

### 6. Producer 실행

```bash
./bin/kafka-console-producer.sh --topic new-topic --bootstrap-server localhost:9092
```

### 7. Consumer 실행

```bash
./bin/kafka-console-consumer.sh \
  --topic new-topic \
  --from-beginning \
  --bootstrap-server localhost:9092
```

## 확인 포인트

- 브로커 기동 여부
- 토픽 생성 여부
- 메시지 publish / consume 정상 동작 여부
