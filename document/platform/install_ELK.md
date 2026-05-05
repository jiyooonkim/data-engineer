# ELK 설치

> Docker 기반으로 Elasticsearch와 Kibana를 올리는 기본 흐름 정리

## 문서 정보

- 작성일: 2023-07-09
- 환경: macbook pro14 m2 (silicon)
- 참고 자료
  - [Elastic Guide Book](https://esbook.kimjmin.net/)
  - [Docker on macOS 설치 참고](https://medium.com/@teeppiphat/install-elasticsearch-docker-on-macos-m1-7dfbb8876b99)

## 핵심 포인트

- ELK는 Elasticsearch, Logstash, Kibana 조합임
- 로컬 테스트 용도면 Docker 단일 노드 구성이 가장 단순함
- 버전에 따라 보안 초기화 방식이 다르므로 7.x와 8.x를 구분해서 보는 편이 좋음

## ELK란

- Elasticsearch: 저장, 검색, 집계
- Logstash: 수집과 변환
- Kibana: 시각화와 조회

## 설치 순서

### 1. Elasticsearch 실행

```bash
docker pull docker.elastic.co/elasticsearch/elasticsearch:7.9.1
docker run -d -p 9200:9200 -p 9300:9300 \
  -e "discovery.type=single-node" \
  --name elasticsearch7 \
  docker.elastic.co/elasticsearch/elasticsearch:7.9.1
```

### 2. 실행 확인

```bash
docker ps
docker image list
docker exec -it elasticsearch7 cat /usr/share/elasticsearch/config/elasticsearch.yml
```

### 3. Kibana 실행

```bash
docker pull docker.elastic.co/kibana/kibana:7.9.1
docker run -d --link elasticsearch7:elasticsearch -p 5601:5601 \
  --name kibana7 docker.elastic.co/kibana/kibana:7.9.1
```

### 4. 접속 확인

- Kibana: `http://localhost:5601`
- Elasticsearch API: `http://localhost:9200`

## 8.x 계열 보안 초기화 메모

```bash
docker exec -it <elasticsearch_container_id> /bin/sh
bin/elasticsearch-create-enrollment-token --scope kibana
bin/elasticsearch-reset-password -a -u elastic

docker exec -it <kibana_container_id> /bin/sh
bin/kibana-verification-code
```

## 자주 쓰는 명령

```text
GET _cat/indices
GET <index_name>/_search
PUT <index_name>
POST <index_name>/_update/<id>
DELETE <index_name>
```

## 용어 메모

- Document: 저장되는 최소 단위
- Index: Document 집합
- Inverted Index: 검색용 역색인 구조
- Primary Shard: 원본 샤드
- Replica: 복제 샤드
