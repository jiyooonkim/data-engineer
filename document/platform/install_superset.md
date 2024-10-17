### Superset 설치
#### env : macbook pro14 m2(silicon), docker
#### date : 2024-10-17
  
#### reference :  
+  
## Superset
+  
<br/><br/>

## Superset Install Flow
1. [hive 3.1.3 설치 Link](https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz)     
    사전 hadoop 설치 반드시 필요, [Hadoop 설치 참조](./install_hadoop.md)   
    사전 mysql 설치 반드시 필요, [Hadoop 설치 참조](./install_mysql.md)      
    [JAVA8 설치 URL](https://www.azul.com/core-post-download/?endpoint=zulu&uuid=7b991cc1-0d5e-403f-a271-7cd622f1cb03)        
    [JAVA8 설치 방법](https://velog.io/@jiisuniui/%EC%8B%B8%ED%94%BCssafy%EC%9D%B8%EB%93%A4%EC%9D%84-%EC%9C%84%ED%95%9C-%EB%A7%A5%EB%B6%81mac-OS-M1-Monterey-Java8-Zulu-%EC%84%A4%EC%B9%98-%EB%B0%8F-%ED%99%98%EA%B2%BD%EB%B3%80%EC%88%98-%EC%84%A4%EC%A0%95)
2. ~/.bashrc hive 파일 경로 등록       
      ```export HIVE_HOME=/Users/jy_kim/Documents/apache-hive-3.1.3/bin```    
3. mysql 설정
   1. mysql root 계정 접속
         ```mysql -u root -p root```
   2. hive metastore database 생성 
    ```
        CREATE DATABASE metastore;
        USE metastore;
        create user 'hive@'%' identified by 'hive'; 
        grant all privileges on metastore.* to hive@'%' identified by 'hive';
        flush privileges;  
        exit;                    # root 계정 나온 뒤   
        mysql -u hive -p hive;   # hive 계정 로그인    
        create database hive;    # hive metastore DB 생성
    ``` 
   
  

## Superset 응용  
+ Hive 연동
<br/>


## Solved Problem
+
<br/>



### 용어 정리
+ 
<br/>