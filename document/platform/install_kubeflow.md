### title: Kubeflow 설치
#### env : macbook pro14 m2(silicon)
#### date : 2024-03-19
#### desc : Kubernetes, kustomize 5.0.0, kubectl    
#### reference : https://otzslayer.github.io/mlops/2023/07/13/install-kubeflow-in-apple-silicon.html   
<br/><br/> 

## Install Flow
1. Docker 설치
   ``` https://docs.docker.com/desktop/install/mac-install/ ```
2. kustomize 설치
    ```
        $ wget https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv5.0.0/kustomize_v5.0.0_darwin_arm64.tar.gz
        $ tar -zxvf kustomize_v5.0.0_darwin_arm64.tar.gz
        $ chmod +x kustomize
        $ sudo mv kustomize /usr/local/bin
        $ kustomize version     # Check kustomize version
    ```
3. minikube 설치 
    ``` $ brew install minikube ```
4. Kubeflow 설치
    ``` 
        git clone https://github.com/kubeflow/manifests.git --branch v1.7.0
        cd manifests 
    ```
5. minikube 설치
    ``` 
        minikube start --driver=docker --kubernetes-version=1.24.1 --disk-size 20g --memory 10240 --cpus 4 --profile kubeflow
    ``` 


