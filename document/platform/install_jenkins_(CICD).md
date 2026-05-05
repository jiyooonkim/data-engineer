# Jenkins 설치와 CI/CD 구성

> 로컬 Jenkins 설치, GitHub 연동, ngrok 기반 webhook 테스트 흐름 정리

## 문서 정보

- 작성일: 2023-05-31 ~ 2023-06-04
- 환경: macbook pro14 m2 (silicon)
- 참고 자료: [GitHub webhook localhost 이슈](https://medium.com/eum-tech-lab/github-webhooks%EC%97%90%EC%84%9C-localhost%EB%A5%BC-%EC%82%AC%EC%9A%A9%ED%95%A0-%EC%88%98-%EC%97%86%EB%8A%94-%EC%9D%B4%EC%9C%A0-9c0d2aecf118)

## 핵심 포인트

- Jenkins는 빌드, 테스트, 배포 자동화 도구임
- 로컬 Jenkins를 GitHub와 연결하려면 외부에서 접근 가능한 webhook 주소가 필요함
- 간단한 실습은 ngrok 조합이 가장 편함

## Jenkins 설치

```bash
brew install jenkins
brew services start jenkins
```

## 초기 접속

- UI: `http://localhost:8080`
- 초기 비밀번호 확인

```bash
cat /Users/jy_kim/.jenkins/secrets/initialAdminPassword
```

## ngrok 사용 이유

- GitHub webhook은 `localhost`를 직접 호출할 수 없음
- 로컬 Jenkins를 외부에 잠시 노출하려면 ngrok 같은 터널링이 필요함

```bash
ngrok http 8080
```

## GitHub 연동

### GitHub 설정

1. Repository > Settings > Webhooks
2. ngrok URL 기반 payload URL 입력
3. Personal Access Token 발급

### Jenkins 설정

1. 시스템 설정에 GitHub 정보 입력
2. Credentials 등록
3. Freestyle project 또는 Pipeline 생성

## Pipeline 메모

- GitHub project URL 등록
- Pipeline script 작성
- webhook으로 자동 트리거 확인

## 제어 명령

```bash
brew services start jenkins
brew services stop jenkins
brew services restart jenkins
```

## 용어 메모

- CI: Continuous Integration
- CD: Continuous Delivery / Deployment
