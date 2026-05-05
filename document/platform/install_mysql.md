# MySQL 설치

> Homebrew 기반으로 MySQL을 설치하고 기본 명령을 정리한 문서

## 문서 정보

- 작성일: 2023-03-04, 2024-09-20
- 환경: macbook pro14 m2 (silicon)

## 핵심 포인트

- macOS에서는 Homebrew 설치가 가장 단순함
- 설치 직후 `mysql_secure_installation`으로 초기 보안 설정을 맞추는 편이 좋음
- 버전별 서비스 시작 명령을 구분해서 기억해두면 편함

## 설치

```bash
brew install mysql
mysql.server start
mysql_secure_installation
mysql -u root -p
brew services start mysql
```

## 자주 쓰는 명령

```bash
mysql -u root -p
brew services stop mysql
brew install mysql@8.4
brew services start mysql@8.4
brew services stop mysql@8.4
```

## 경로 메모

```text
/opt/homebrew/Cellar/mysql/
```

## 인덱스 메모

- 인덱스는 조회 성능 향상에 유리함
- 조건절과 정렬에 자주 쓰는 컬럼에 우선 적용하는 편이 좋음
- 변경이 잦은 테이블에 과도하게 두면 역효과가 날 수 있음
