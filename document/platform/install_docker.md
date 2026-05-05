# Docker 설치

> macOS 환경에서 Docker Desktop을 설치하고 기본 개념을 정리한 문서

## 문서 정보

- 작성일: 2023-03-04
- 환경: macbook pro14 m2 (silicon)

## 핵심 포인트

- macOS에서는 보통 Docker Desktop 설치가 가장 단순함
- Docker는 컨테이너 기반 실행 환경임
- `Dockerfile`과 `docker-compose` 역할을 분리해서 이해해야 함

## 설치

```bash
brew install --cask docker
```

- 설치 후 Docker Desktop 실행
- 로컬에서 `docker ps`로 정상 동작 확인

## 자주 쓰는 명령

```bash
docker -v
docker pull hello-world
docker run hello-world
docker image list
docker ps -a
docker stop <container>
docker rm <container>
docker rmi <image>
```

## Docker Compose

- 여러 컨테이너 서비스를 YAML로 묶어 관리하는 도구임
- 보통 `docker-compose.yaml` 또는 `compose.yaml` 파일을 기준으로 실행함

```bash
docker compose up -d
docker compose down
```

## Dockerfile

- 이미지를 빌드하기 위한 정의 파일임
- 앱 실행 환경을 코드처럼 관리할 수 있음

```bash
docker build -t my-image:latest .
```

## Docker vs VM

- VM: Guest OS 포함
- Docker: Host OS 커널 공유
- 다양한 OS까지 완전히 분리해야 하면 VM이 유리함
- 빠른 배포와 가벼운 실행은 Docker가 유리함

## 용어 메모

- Image: 컨테이너 실행 원본
- Container: 실행 중인 이미지
- Hypervisor: VM 실행 계층
- Kernel: 하드웨어와 소프트웨어 사이 핵심 계층
