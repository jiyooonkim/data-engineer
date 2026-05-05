# Kubeflow 설치

> Apple Silicon 환경에서 Minikube 기반 Kubeflow를 올리는 절차 정리

## 문서 정보

- 환경
  - Sonoma 14.3.1 (M2 Pro)
  - Minikube 1.32.0
  - Kubernetes 1.24.1
  - Docker 4.24.0
- 참고 자료: [Apple Silicon Kubeflow 설치 참고](https://otzslayer.github.io/mlops/2023/07/13/install-kubeflow-in-apple-silicon.html)

## 핵심 포인트

- Kubeflow는 Kubernetes 위에 올라가는 ML 워크플로우 플랫폼임
- Apple Silicon에서는 이미지 호환성 이슈를 먼저 의심해야 함
- 설치보다도 `ImagePullBackOff`, `PodInitializing` 대응이 더 중요함

## 설치 순서

### 1. Docker 설치

- Docker Desktop 준비

### 2. `kustomize` 설치

```bash
wget https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv5.0.0/kustomize_v5.0.0_darwin_arm64.tar.gz
tar -zxvf kustomize_v5.0.0_darwin_arm64.tar.gz
chmod +x kustomize
sudo mv kustomize /usr/local/bin
kustomize version
```

### 3. Minikube 설치

```bash
brew install minikube
```

### 4. 클러스터 실행

```bash
minikube start \
  --driver=docker \
  --kubernetes-version=1.24.1 \
  --disk-size 60g \
  --memory 10240 \
  --cpus 8 \
  --profile kubeflow
```

### 5. Kubeflow Manifest 적용

```bash
git clone https://github.com/kubeflow/manifests.git --branch v1.7.0
cd manifests
while ! kustomize build example | awk '!/well-defined/' | kubectl apply -f -; do
  echo "Retrying to apply resources"
  sleep 10
done
```

## ARM 환경 메모

- ARM CPU에서 이미지 호환성 문제로 `ImagePullBackOff`가 자주 남
- 필요한 경우 ARM 호환 커밋이나 수정된 manifest를 적용해야 함

## 확인과 접속

```bash
kubectl get pods --all-namespaces
kubectl port-forward svc/istio-ingressgateway -n istio-system 8080:80
```

- 기본 계정
  - Email: `user@example.com`
  - Password: `12341234`

## 자주 쓰는 명령

```bash
kubectl logs -n istio-system authservice-0
minikube dashboard -p kubeflow
minikube profile kubeflow
docker ps
```
