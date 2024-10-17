## mysql 설치
### env : macbook pro14 m2(silicon)
### date : 2023-03-04, 2024-09-20
### desc :  homebrew 이용한 Mysql 설치 방법


### Install
1. brew install mysql     
2. mysql.server start
3. mysql_secure_installation # setting
4. mysql -u root -p
5. root    # password
6. brew services start mysql   # always running setting, 최초 설치시에는 실행 동시에 되기 때문에 안해줘도 됨          
** mysql 경로         
```   /opt/homebrew/Cellar/mysql/ ```     

### Mysql 명령어
+ Mysql 접속(password : root )    
 ```  mysql -u root -p root ```    
+ Mysql server 종료     
 ``` brew services stop mysql ```   
+ 특정버전 설치    
 ``` brew install mysql@8.4 ```    
+ 특정버전 시작/종료     
```brew services start mysql@8.4```       
``` brew services stop mysql@8.4```      
```brew services start mysql ```     





#### 옹어정리
+ idex(인덱스) : 주로 검색(SELECT) 쿼리의 수행 속도를 높이기 위해 데이터베이스의 데이터 위치 (조건 검색, Order by 속도향상)
인덱스를 남발하면 되려 역효과가 생기기 때문에 인덱스는 조건문에 자주 사용되는 컬럼에 생성하고 최대한 중복이 사용 할 것,  변경이 빈번하게 발생하는 테이블 자제


