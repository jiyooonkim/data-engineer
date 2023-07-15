### Title: Shell
#### Date : 2023-07-15  


## Shell 이란?
- OS 에서 사용자 명령을 읽고/해석 하여 실행해주는 프로그램
- 사용자와 OS 인터페이스 역할

## Bash Shell 이란?
- 유닉스에서 사용하는 커맨드 쉘
- bourne-again-shell 이라고 하나 줄여서 bash 라고..

#### 개념 정리
* Q. .bashrc 와 .bash_profile 의 차이   
A.  .bashrc : 이미 로그인 한 상태에서 새터미널 실행시킬 때 로드     
     .bash_profile : 로그인 할 때마다 로드       
   .profile : 로그인할 때 로드,  Mac에서 새 터미널 창을 열 때마다 .bashrc를 로드하고 싶다면 .bash_profile에서 .bashrc를 로드 
    ```  
    # Source bashrc
    if [ -f ~/.bashrc ]; then
        . ~/.bashrc
    fi
    ```      
* Q. Login Shell 과 Non-Login Shell          
A. Login Shell : ID/Pasword 입력통해 shell 실행 또는 로컬에서 GUI를 통해 접속시, .profile과 .bash_profile 모두 Login할 때 로드되는 파일                   
  Non-Login Shell : 로그인 없이 실행하는 Shell        