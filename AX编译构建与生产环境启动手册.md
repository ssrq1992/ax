# AX 编译、构建与生产环境启动手册

适用对象：平台管理员。适用代码：本目录 `ax/` 的受管运行时扩展版本。更新时间：2026-10-01。

本文按 **Linux → Kubernetes → Substrate → AX → TaskGroup → kagent 联调** 的顺序说明真实环境安装。所有集群写操作是供操作者执行的步骤，本次编写没有执行这些操作。配套业务层见 [kagent 手册](kagent编译构建与生产环境启动手册.md)。

## 0. 先明确能够交付什么

- 这是面向生产环境基础设施的安装和验收流程，**不是已经获得生产可用性认证的发行版**。锁定的 Substrate README 明确声明项目尚处早期；该版本 ate-api 有认证，但没有完整业务授权/RBAC，必须处于平台信任域内。
- 本机已完成的编译、测试范围见 [软件实现.md](软件实现.md)。真实 Linux 容器构建、集群调度、MITM、快照恢复、MicroVM 和灾难恢复仍须在目标环境执行并签收。
- 当前 AX managed backend 使用 `NewInClusterPlatform`，读取 Pod 的 Kubernetes ServiceAccount 投射文件。**生产 ax-server 必须运行于 Kubernetes Pod；不能仅在任意 Linux 主机执行二进制就得到完整运行平台。** Linux 主机承担构建、CLI 和运维工作。
- 当前代码未包含 Kubernetes Secret credential provider 的服务实现和发行镜像。平台必须提供满足第 5 节契约的 provider；缺少它时停在平台准备阶段，不能以关闭鉴权或手工注入伪造身份头绕过。
- AX 当前 Redis 客户端仅配置地址、密码，不支持原生 TLS/Sentinel/Redis Cluster 配置。需要加密链路时由已验收的透明传输代理/网络层提供；不能把 `rediss://` 填入地址并假设生效。不得把无认证示例 Redis 直接暴露到生产共享网络。

## 1. 环境清单与部署顺序

### 1.1 参考拓扑

```text
Linux 构建/运维机 ── 私有镜像仓库
         │ kubectl / Helm / AX CLI
         ▼
真实 Kubernetes（生产控制面 HA，CNI、DNS、CSI 已配置）
  ate-system                 ax-system                   kagent
  Substrate API              AX Server :8443 mTLS         controller :8083 HTTPS
  ate-controller             AX Redis + PVC/备份          UI / OIDC 入口
  atelet / atenet             配置、客户端授权             业务 PostgreSQL
  credential provider        外置 ledger epoch            Harness / Agent
  worker Pods ◄────────────── TaskGroup 映射
       │
       └── gVisor / MicroVM；独立快照对象存储
```

Substrate PostgreSQL、kagent PostgreSQL、AX Redis 是三份不同状态存储。不能用其中一份替代另外两份。生产可以共享数据库服务集群，但必须使用独立 database、账号和备份恢复边界。

### 1.2 操作者必须提供的参数

| 参数 | 示例/约束 |
|---|---|
| Linux 构建机 | Ubuntu 24.04 LTS、amd64；以下 shell 使用 Bash |
| Kubernetes context | 显式填写，如 `agent-prod`；禁止沿用未知 current-context |
| 节点 | Linux；控制平面和运行不可信代码的 worker 分池；MicroVM 节点需 `/dev/kvm` |
| Kubernetes 版本 | 按锁定 Substrate 兼容约束：1.36 需显式开启证书 beta API/feature gates；1.37+ 仍检查 API discovery，不能只比较版本号 |
| 存储 | 支持持久卷的 StorageClass、外部 PostgreSQL、对象存储、备份位置 |
| 镜像 | 私有仓库、推送身份、节点拉取身份、每个最终镜像的 digest |
| 证书 | AX server CA、AX client CA、controller HTTPS CA、Substrate service/pod identity CA、MITM CA |
| 凭据服务 | `credprovider.ate-system.svc:50051` 的已验收 mTLS 服务及 Secret 授权策略 |
| 运行容量 | gVisor worker image、sandbox config、节点资源和预热容量 |

顺序：Kubernetes → 外部数据库/对象存储 → Substrate/证书控制器 → credential provider 与 MITM → AX Redis 初始化 → AX Server → TaskGroup → kagent → 业务验收。

### 1.3 建立工作目录

以下示例路径是 Linux 路径，与此前 macOS 测试路径无关。`AX_REPOSITORY` 必须是包含本次改造的仓库；直接下载上游 AX main 不会得到这些接口。

```bash
export WORK_ROOT=/srv/agent-platform
export RELEASE_DIR="$WORK_ROOT/release"
export OPS_DIR="$WORK_ROOT/ops"
export KUBE_CONTEXT=agent-prod
export SUBSTRATE_REV=944abe3278b895ccbf5d45555a49dd0f2f6ceae7
export KO_DEFAULTPLATFORMS=linux/amd64
mkdir -p "$RELEASE_DIR" "$OPS_DIR" "$WORK_ROOT/bin"
umask 077
kubectl config use-context "$KUBE_CONTEXT"
test "$(kubectl config current-context)" = "$KUBE_CONTEXT"
```

将已经审核的配套源代码放为 `$WORK_ROOT/kagentOnAx/ax` 和 `$WORK_ROOT/kagentOnAx/kagent`。若通过 Git 获取，先给 `AX_REPOSITORY`、`AX_REV` 赋真实值，再执行：

```bash
: "${AX_REPOSITORY:?提供包含本次实现的 Git 仓库}"
: "${AX_REV:?提供配套 AX 完整提交 SHA}"
git clone "$AX_REPOSITORY" "$WORK_ROOT/kagentOnAx/ax"
git -C "$WORK_ROOT/kagentOnAx/ax" checkout --detach "$AX_REV"
test -f "$WORK_ROOT/kagentOnAx/ax/pkg/apis/v1alpha1/execution.proto"
git -C "$WORK_ROOT/kagentOnAx/ax" rev-parse HEAD > "$RELEASE_DIR/ax.commit"
```

当前尚未发布的工作区版本应作为经过校验的源码制品传递，记录基础 SHA、patch 和文件哈希；单纯 checkout 基础 SHA 会丢失本次修改。

## 2. Linux 工具安装与 AX 编译

### 2.1 基础工具

```bash
sudo apt-get update
sudo apt-get install -y build-essential git curl ca-certificates jq openssl \
  gettext-base python3 python3-venv unzip zstd
python3 -m venv "$WORK_ROOT/tools-venv"
"$WORK_ROOT/tools-venv/bin/pip" install 'PyYAML==6.0.2'
```

安装 Go **1.27.1**、Docker Engine + buildx、与集群配套的 kubectl、Helm、ko。用组织制品镜像/官方发行物并验证 SHA256，不使用未经固定版本的安装脚本。Go 安装例（需要先填官方发行物校验值）：

```bash
: "${GO_ARCHIVE_SHA256:?填写 go1.27.1.linux-amd64.tar.gz 的核验值}"
curl -fL https://go.dev/dl/go1.27.1.linux-amd64.tar.gz -o "$RELEASE_DIR/go.tar.gz"
printf '%s  %s\n' "$GO_ARCHIVE_SHA256" "$RELEASE_DIR/go.tar.gz" | sha256sum -c -
# 使用独立版本目录，避免覆盖主机已有 Go。
sudo mkdir -p /opt/go-1.27.1
sudo tar -xzf "$RELEASE_DIR/go.tar.gz" -C /opt/go-1.27.1 --strip-components=1
export PATH="/opt/go-1.27.1/bin:$WORK_ROOT/bin:$PATH"
go version
docker version
docker buildx version
kubectl version --client
helm version
ko version
```

将工具版本输出保存到发布记录。Docker 在构建机运行；Kubernetes 节点使用 CRI 运行时，不要求安装 Docker Engine。

### 2.2 二进制

```bash
cd "$WORK_ROOT/kagentOnAx/ax"
go mod download
go mod verify
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$WORK_ROOT/bin/ax" ./cmd/ax
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$WORK_ROOT/bin/ax-server" ./cmd/ax-server
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$WORK_ROOT/bin/ax-sandbox-guest" ./cmd/ax-sandbox-guest
sha256sum "$WORK_ROOT/bin/ax" "$WORK_ROOT/bin/ax-server" "$WORK_ROOT/bin/ax-sandbox-guest" \
  > "$RELEASE_DIR/ax-binaries.sha256"
```

普通构建使用已提交生成文件，不需要重新生成 Proto。修改契约时才按 `ax/scripts/generate-managed.sh` 的固定插件版本生成，并确认重复生成没有新差异。不要运行 `go get ...@latest` 改变已锁定的后端。

### 2.3 镜像构建

AX Server 通过 ko 构建；Guest 使用当前 Dockerfile：

```bash
export KO_DOCKER_REPO=registry.example.com/agent-platform/ax
export AX_GUEST_TAG=registry.example.com/agent-platform/ax-guest:ax-release-001
cd "$WORK_ROOT/kagentOnAx/ax"
# 先按仓库要求登录；不要把 registry 密码写入命令行或手册。
ko build --platform=linux/amd64 ./cmd/ax-server > "$RELEASE_DIR/ax-server.image"
docker buildx build --platform linux/amd64 --push \
  -f Dockerfile.guest -t "$AX_GUEST_TAG" --metadata-file "$RELEASE_DIR/guest-build.json" .
export AX_SERVER_IMAGE="$(tail -n 1 "$RELEASE_DIR/ax-server.image")"
export AX_GUEST_IMAGE="${AX_GUEST_TAG%:*}@$(jq -r '."containerimage.digest"' "$RELEASE_DIR/guest-build.json")"
printf '%s\n' "$AX_GUEST_IMAGE" > "$RELEASE_DIR/ax-guest.image"
[[ "$AX_SERVER_IMAGE" == *@sha256:* ]]
[[ "$AX_GUEST_IMAGE" == *@sha256:* ]]
```

仓库 `.ko.yaml` 和部分 Dockerfile 使用浮动基础镜像。生产构建流水线还必须锁定并记录基础镜像 digest、生成 SBOM/扫描结果；固定最终 digest 只保证部署不漂移，并不自动保证重复构建字节一致。不要将上游旧 Guest 或测试夹具镜像填入 `guestImage`。

## 3. 准备真实 Kubernetes

已有生产集群直接执行 3.1。新建自管集群按附录 A 完成 Linux/CRI/kubeadm、HA 控制面、CNI 和 CSI 后再继续；不使用 kind 作为生产路径。

### 3.1 前置检查

```bash
kubectl --context "$KUBE_CONTEXT" get nodes -o wide
kubectl --context "$KUBE_CONTEXT" get storageclass
kubectl --context "$KUBE_CONTEXT" get --raw /apis/certificates.k8s.io/v1beta1 | jq '.resources[].name'
kubectl --context "$KUBE_CONTEXT" get pods -n kube-system
```

必须看到 `clustertrustbundles` 和 `podcertificaterequests`，并确认每台 kubelet 支持相应 projected volume。1.36 的 feature gates：`ClusterTrustBundle`、`ClusterTrustBundleProjection`、`PodCertificateRequest`；API server 还需启用 `certificates.k8s.io/v1beta1`。配置依据锁定版本 `hack/create-kind-cluster.sh` 的能力要求，但这里使用真实集群配置而非运行 kind。

平台网络必须允许：Pod/Service DNS、Pod 到 Kubernetes API、AX 到 Substrate API/router、worker 到对象存储/镜像仓库、MITM 到 controller HTTPS/模型服务、egress 到 credential provider。CNI 必须实际执行 NetworkPolicy；“创建了策略对象”不等于链路隔离生效。

### 3.2 节点条件

gVisor 由 Substrate atelet 获取 SandboxConfig 中的资产运行，不能仅安装 Kubernetes RuntimeClass 代替。节点必须允许 Substrate 所需的特权、hostPath、网络和文件系统操作；在专用 worker 节点及命名空间授权这些例外，业务 controller/UI 保持非特权。

MicroVM 额外检查（在目标节点执行）：

```bash
uname -m
test -c /dev/kvm
ls -l /dev/kvm
lsmod | grep kvm
```

虚拟机宿主需开启嵌套虚拟化；节点镜像/内核、CPU 架构、KVM、virtiofs、快照资产必须形成一套验收矩阵。没有这些条件时先只启用 gVisor，不创建 microvm TaskGroup。

## 4. 安装固定版本 Substrate

### 4.1 使用独立固定 checkout

本工作区 `projects/substrate` 的 HEAD 与 AX 锁定版本不同。生产安装必须以 AX 锁定 commit 为基线，不能直接运行另一个 HEAD 的脚本。

```bash
git clone https://github.com/agent-substrate/substrate.git "$WORK_ROOT/substrate-pinned"
git -C "$WORK_ROOT/substrate-pinned" checkout --detach "$SUBSTRATE_REV"
test "$(git -C "$WORK_ROOT/substrate-pinned" rev-parse HEAD)" = "$SUBSTRATE_REV"
cd "$WORK_ROOT/substrate-pinned"
git diff --exit-code
go mod verify
```

不修改 Substrate 源码/生成物/二进制。站点部署 overlay、凭据和渲染结果放在 `$OPS_DIR`，不要回写源码目录。

### 4.2 PostgreSQL 与对象存储

DBA 先创建仅供 ate-api 使用的数据库与账号，例如 database `atepg`。提供 TLS CA、访问策略、连接数预算、备份/PITR；不要使用安装器附带的演示 PostgreSQL。以下 DSN 从受限文件读取；文件内容中的证书路径指的是**目标 Pod 内**路径。

```bash
export ATE_API_POSTGRES_CONNECTION_STRING="$(cat "$OPS_DIR/substrate-postgres.dsn")"
# DSN 例：postgresql://USER:URL_ENCODED_PASSWORD@db.example.com:5432/atepg?sslmode=verify-full&sslrootcert=/run/postgres-server-ca/server-ca.pem
export ATE_API_POSTGRES_SERVER_CA_FILE="$OPS_DIR/pki/postgres-ca.pem"
export ATE_API_POSTGRES_SCHEMA=public
export ATE_API_POSTGRES_POOL_MAX_CONNS=20
```

对象存储主路径示例为 `gs://YOUR_BUCKET/ax/`。给 worker/atelet 实际使用的云身份授予该前缀所需的读写/删除权限；配置地域、加密和备份，禁止生命周期规则提前删除仍被 PreparedRuntime/Checkpoint 引用的对象。非 GCP Kubernetes 需要平台配置 Workload Identity Federation/受控 ADC 挂载，不能假定 Pod 自带 GCP 身份。S3 等后端必须在固定版本的实际资产读取和快照路径上单独验收，不能仅替换 URI 前缀。

### 4.3 安装控制面和 gVisor

```bash
cd "$WORK_ROOT/substrate-pinned"
export NO_DEV_ENV=1
export KUBECTL_CONTEXT="$KUBE_CONTEXT"
export KO_DOCKER_REPO=registry.example.com/agent-platform/substrate
export EXPECTED_JWT_ISSUER="$(kubectl get --raw /.well-known/openid-configuration | jq -r .issuer)"
test -n "$EXPECTED_JWT_ISSUER"
test "$EXPECTED_JWT_ISSUER" != null
make build-atectl
install -m 0755 bin/kubectl-ate "$WORK_ROOT/bin/kubectl-ate"
./hack/install-ate.sh --deploy-ate-system --rollout-timeout 10m \
  --atenet-dataplane=envoy \
  --experimental-egress-credential-injection \
  --credential-provider-name ate-secret://k8s.io \
  --credential-provider-address credprovider.ate-system.svc:50051
unset ATE_API_POSTGRES_CONNECTION_STRING
```

此安装器会构建/推送并应用固定 checkout 的平台清单、安装 CRD 和证书控制器，并给节点打 Substrate version label；在正式环境执行前须在预生产审核其生成内容和影响范围。不要运行 `--delete-all` 或把开发环境脚本当作生产卸载命令。

这里的 **`ate-secret://k8s.io` 不能使用安装器默认 `ate-secret://kubernetes.io`**：AX 当前生成的 URI 是 `ate-secret://k8s.io/default/<namespace>/<secret>/<key>`，provider 名称不匹配会导致注入失败。该开关会启用 sdsmint MITM，但不会安装 credential provider。

```bash
kubectl get crd workerpools.ate.dev sandboxconfigs.ate.dev
kubectl get sandboxconfig gvisor-default -o yaml
kubectl -n podcertificate-controller-system rollout status deployment/podcertificate-controller --timeout=10m
kubectl -n ate-system get deploy,ds,pods,svc
kubectl get clustertrustbundles.certificates.k8s.io
kubectl -n ate-system get ds -l app=atelet -L ate.dev/substrate-version
kubectl get nodes -L ate.dev/substrate-version
```

扩容新节点时必须使用当前安装的精确版本 label；不匹配的节点不会获得容量。初次安装后记录所有实际镜像 digest、SandboxConfig 资产 hash 和节点 version。

### 4.4 平台认证与 TLS

核对 `ate-api-authentication` ConfigMap 的 JWT issuer 与 Kubernetes token 的 issuer 一致；audience 为 `api.ate-system.svc`。AX 的投射 token 正是该 audience。固定后端的授权能力不足以构成敌对多租户边界：将 ate-api/router 仅开放给平台服务，限制 namespace、Pod、运维入口和可创建 workload 的主体。

固定版本自带 router **HTTPS Service 443**，可直接配置 `atenet-router.ate-system.svc:443`，无需默认增加 TLS 代理。其 SAN 与 CA 必须现场核实；不要把 80 填入 managed router 或关闭 TLS 校验。AX 的旧普通 Task 入口仍保留 8080，不能向不可信网络暴露。

### 4.5 可选 MicroVM

完成 KVM 检查后，在固定 checkout 按脚本生成/上传其配套资产：

```bash
cd "$WORK_ROOT/substrate-pinned"
./hack/install-microvm-deps.sh --help
# 设置脚本要求的 BUCKET_NAME、架构和云身份后执行：
./hack/install-microvm-deps.sh --install
kubectl get sandboxconfig microvm -o yaml
```

该脚本包括下载、镜像/二进制资产组装和上传，不是单纯 apply。必须检查实际生成的 amd64/arm64 asset URL 和 SHA256；不要手写占位校验值。然后构建 `./cmd/ateom-microvm` 的镜像，并在 AX 中加入独立 microvm profile。主流程先验收 gVisor；MicroVM 按同样业务场景另做完整测试。

## 5. 安装并验收 credential provider 和 MITM

### 5.1 必须具备的接口

平台提供并审核独立服务制品，部署其供应方提供的清单。当前仓库没有可指定的已验证 provider 镜像，本文不虚构一个镜像名或启动参数。

| 项目 | 必须满足 |
|---|---|
| gRPC | 固定 Substrate `pkg/proto/credproviderpb/credprovider.proto` 的 `CredentialProvider.FetchSecret` |
| URI | 解析 `ate-secret://k8s.io/default/<namespace>/<secret>/<key>` |
| 请求身份 | 校验调用方 mTLS，仅信任 egress；用 `actor_spiffe_id` 做 Secret 授权，不能把任意请求者提交的字段当可信身份 |
| Secret | 允许授权运行实例访问它的 AX runtime Secret 和绑定的模型 Secret；拒绝跨 namespace/跨实例读取 |
| 服务证书 | SAN `credprovider.ate-system.svc`，链到 egress 使用的 servicedns trust bundle；客户端证书链到 podidentity trust bundle |
| 行为 | 不记录 Secret 内容；拒绝未知 URI、未授权、空凭据；定义轮换/撤销及缓存失效时间 |

执行供应方已审核安装清单后：

```bash
: "${CREDENTIAL_PROVIDER_MANIFEST:?平台提供的已审核 provider 安装清单}"
kubectl --context "$KUBE_CONTEXT" apply -f "$CREDENTIAL_PROVIDER_MANIFEST"
kubectl -n ate-system get svc credprovider
kubectl -n ate-system get endpointslices -l kubernetes.io/service-name=credprovider
kubectl -n ate-system get deploy atenet-egress -o yaml > "$RELEASE_DIR/egress-effective.yaml"
```

检查实际 egress args 的 provider prefix、地址、CA、client cert。只看到 Endpoints 不代表 FetchSecret 正确；在隔离验收 namespace 用已授权和未授权的运行身份各测试一次，记录状态码，不输出 Secret 值。

### 5.2 MITM 的两段信任

1. **Agent → MITM**：AX 挂载 `egress-mitm.ate.dev` TrustBundle，并设置对应 runtime 的 CA 环境；确认 Bundle 存在且投射到实际运行环境。
2. **MITM → kagent HTTPS**：egress 实际 HTTP/TLS 转发组件必须验证 kagent controller 证书。把 controller CA 加入该组件有效的上游信任配置并验证 hostname。只在 Agent 或 AX Pod 放 CA 对这一段无效；不能凭设置 `SSL_CERT_FILE` 猜测 Envoy 已采用它。

生产平台需保存这两段的实际证书链、信任配置和请求证据。使用私有 CA 时，必须按固定版本 egress 数据面的 TLS 配置接入；本手册不把“增加 ConfigMap”写成自动完成信任配置。未完成时在此停止业务放量。

## 6. AX 证书、命名空间与 Redis

### 6.1 命名空间与证书签发

```bash
kubectl create namespace ax-system --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace kagent --dry-run=client -o yaml | kubectl apply -f -
mkdir -p "$OPS_DIR/pki"
```

使用组织 PKI 签发以下证书，CA 私钥不得放在工作区或 Pod 中。以下生成 CSR，**不是自签生产根 CA**：

```bash
openssl req -new -newkey rsa:3072 -nodes \
  -keyout "$OPS_DIR/pki/ax-server.key" -out "$OPS_DIR/pki/ax-server.csr" \
  -subj '/CN=ax-server.ax-system.svc' \
  -addext 'subjectAltName=DNS:ax-server.ax-system.svc,DNS:ax-server.ax-system.svc.cluster.local' \
  -addext 'extendedKeyUsage=serverAuth'
openssl req -new -newkey rsa:3072 -nodes \
  -keyout "$OPS_DIR/pki/kagent-ax-client.key" -out "$OPS_DIR/pki/kagent-ax-client.csr" \
  -subj '/CN=kagent-controller' \
  -addext 'subjectAltName=URI:spiffe://agent.example/kagent/controller' \
  -addext 'extendedKeyUsage=clientAuth'
openssl req -new -newkey rsa:3072 -nodes \
  -keyout "$OPS_DIR/pki/ax-admin.key" -out "$OPS_DIR/pki/ax-admin.csr" \
  -subj '/CN=ax-platform-admin' \
  -addext 'subjectAltName=URI:spiffe://agent.example/platform/operator' \
  -addext 'extendedKeyUsage=clientAuth'
```

CA 必须实际签入 SAN/EKU，不只保留 CSR 的 CN。拿到 `ax-server.crt`、`kagent-ax-client.crt`、`ax-admin.crt`、`ax-server-ca.pem`、`ax-client-ca.pem` 后验证：

```bash
openssl verify -purpose sslserver -CAfile "$OPS_DIR/pki/ax-server-ca.pem" "$OPS_DIR/pki/ax-server.crt"
openssl verify -purpose sslclient -CAfile "$OPS_DIR/pki/ax-client-ca.pem" "$OPS_DIR/pki/kagent-ax-client.crt"
openssl x509 -in "$OPS_DIR/pki/kagent-ax-client.crt" -noout -ext subjectAltName
kubectl -n ax-system create secret tls ax-server-tls \
  --cert="$OPS_DIR/pki/ax-server.crt" --key="$OPS_DIR/pki/ax-server.key" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ax-system create secret generic ax-client-ca \
  --from-file=ca.crt="$OPS_DIR/pki/ax-client-ca.pem" --dry-run=client -o yaml | kubectl apply -f -
```

四类 Harness 当前注入的 runtime URL 使用短主机名 `kagent-controller.kagent`。凭据按 hostname 匹配，`kagent-controller.kagent` 与 `kagent-controller.kagent.svc` 即使 DNS 指向同一 Service 也不能互换。controller 证书必须同时包含短名和 `.svc` SAN。controller HTTPS 证书按 kagent 手册签发；callbackURL 从开始就固定为 `https://kagent-controller.kagent:8083`，后续不能随意改变。

### 6.2 Redis 持久化与认证

建议优先使用已验证的单写入口 Redis 服务，支持本版本客户端的密码认证及命令事务。若采用仓库 PVC 方案，在副本数 **1**、Recreate、AOF always、noeviction 基础上显式配置 StorageClass 和密码。附带示例只是单实例，不是自动故障转移 HA。

```bash
openssl rand -hex 32 > "$OPS_DIR/redis-password"
{
  printf 'appendonly yes\nappendfsync always\ndir /data\nmaxmemory-policy noeviction\n'
  printf 'requirepass %s\n' "$(cat "$OPS_DIR/redis-password")"
} > "$OPS_DIR/redis.conf"
kubectl -n ax-system create secret generic ax-redis-auth \
  --from-file=password="$OPS_DIR/redis-password" --from-file=redis.conf="$OPS_DIR/redis.conf" \
  --dry-run=client -o yaml | kubectl apply -f -
export AX_STORAGE_CLASS=YOUR_DURABLE_STORAGE_CLASS
"$WORK_ROOT/tools-venv/bin/python" - <<'PY'
import os, pathlib, yaml
root = pathlib.Path(os.environ['WORK_ROOT'])
docs = list(yaml.safe_load_all((root/'kagentOnAx/ax/deploy/redis.yaml').read_text()))
for d in docs:
    if d['kind'] == 'PersistentVolumeClaim':
        d['spec']['storageClassName'] = os.environ['AX_STORAGE_CLASS']
    if d['kind'] == 'Deployment':
        pod = d['spec']['template']['spec']
        pod['volumes'].append({'name': 'auth', 'secret': {'secretName': 'ax-redis-auth'}})
        c = pod['containers'][0]
        c['args'] = ['redis-server', '/run/redis-auth/redis.conf']
        c['volumeMounts'].append({'name': 'auth', 'mountPath': '/run/redis-auth', 'readOnly': True})
pathlib.Path(os.environ['OPS_DIR'], 'redis.yaml').write_text(yaml.safe_dump_all(docs, sort_keys=False))
PY
kubectl apply -f "$OPS_DIR/redis.yaml"
kubectl -n ax-system get pvc ax-redis
kubectl -n ax-system rollout status deploy/ax-redis --timeout=10m
```

正式部署前把 `redis:7.4.2-alpine` 解析为组织审核的 digest，配置内存容量、告警、访问隔离及备份；此示例不自动完成这些站点选择。Redis 6379 仅允许 AX Pod/明确的维护作业访问；跨节点加密采用平台已验证方案。禁止公网 LoadBalancer/NodePort。密码和 ledger epoch 是不同的值。

## 7. 配置 AX 并首次启动

### 7.1 Worker 镜像和运行配置

从固定 Substrate checkout 构建 worker 镜像：

```bash
cd "$WORK_ROOT/substrate-pinned"
export KO_DOCKER_REPO=registry.example.com/agent-platform/substrate
./hack/run-tool.sh ko build --platform=linux/amd64 --ldflags="$(make -s ldflags)" ./cmd/ateom-gvisor > "$RELEASE_DIR/gvisor-worker.image"
export GVISOR_WORKER_IMAGE="$(tail -n 1 "$RELEASE_DIR/gvisor-worker.image")"
[[ "$GVISOR_WORKER_IMAGE" == *@sha256:* ]]
```

创建 `$OPS_DIR/managed.json`（以下 jq 会读取前面导出的最终 digest）：

```bash
jq -n --arg worker "$GVISOR_WORKER_IMAGE" --arg guest "$AX_GUEST_IMAGE" '{
  clients: {
    "spiffe://agent.example/kagent/controller": ["kagent"],
    "spiffe://agent.example/platform/operator": ["kagent"]
  },
  poolProfiles: {gvisor: {workerImage: $worker}},
  sandboxConfigs: {gvisor: "gvisor-default"},
  guestImage: $guest,
  callbackURL: "https://kagent-controller.kagent:8083",
  router: {
    endpoint: "atenet-router.ate-system.svc:443",
    authority: "atenet-router.ate-system.svc",
    caFile: "/run/servicedns-ca/trust-bundle.pem",
    tokenFile: "/var/run/secrets/ateapi/token"
  }
}' > "$OPS_DIR/managed.json"
# 仅全新安装生成一次；保存到 Redis 之外的受控备份。
openssl rand -hex 32 | tr -d '\n' > "$OPS_DIR/ledger-epoch"
export ORDINARY_SNAPSHOTS=gs://YOUR_BUCKET/ax/ordinary
kubectl -n ax-system create configmap ax-managed \
  --from-file=managed.json="$OPS_DIR/managed.json" \
  --from-file=ledgerEpoch="$OPS_DIR/ledger-epoch" \
  --from-literal=ordinarySnapshotsBucket="$ORDINARY_SNAPSHOTS" \
  --dry-run=client -o yaml | kubectl apply -f -
```

epoch 生成命令已去除换行；初始化 Job 和正常 Server 均读取同一 ConfigMap key。原值须保存在 Redis 之外，不能在重启时重新生成。

profile 可按 `kagent/kagent-default` 覆盖 `gvisor` 默认配置。非默认 `template` 结构必须来自固定 WorkerPool schema/配置转换结果，经 server dry-run 检查；不要猜测 Kubernetes PodSpec 可直接塞入任意层级。存续中的 profile 不原地改动，避免恢复核验发现不同配置。

### 7.2 生成部署文件与首次初始化 Job

把 `ko://` 改成上一步真实镜像，注入 Redis 密码，再从同一 Pod 配置生成初始化 Job，避免 token、CA、epoch 路径不一致：

```bash
"$WORK_ROOT/tools-venv/bin/python" - <<'PY'
import os, pathlib, copy, yaml
root = pathlib.Path(os.environ['WORK_ROOT'])
ops = pathlib.Path(os.environ['OPS_DIR'])
docs = list(yaml.safe_load_all((root/'kagentOnAx/ax/deploy/ax-server.yaml').read_text()))
deployment = next(d for d in docs if d['kind'] == 'Deployment')
container = deployment['spec']['template']['spec']['containers'][0]
container['image'] = os.environ['AX_SERVER_IMAGE']
container['env'].append({'name': 'REDIS_PASSWORD', 'valueFrom': {'secretKeyRef': {'name': 'ax-redis-auth', 'key': 'password'}}})
pod = copy.deepcopy(deployment['spec']['template'])
pod['metadata']['labels'] = {'app': 'ax-ledger-init'}
pod['spec']['restartPolicy'] = 'Never'
c = pod['spec']['containers'][0]
c['args'] = ['--redis-addr=ax-redis.ax-system.svc.cluster.local:6379',
             '--managed-ledger-epoch=$(AX_MANAGED_LEDGER_EPOCH)', '--initialize-managed-ledger']
c.pop('readinessProbe', None)
c.pop('livenessProbe', None)
job = {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': 'ax-ledger-init', 'namespace': 'ax-system'},
       'spec': {'backoffLimit': 0, 'template': pod}}
ops.joinpath('ax-server.yaml').write_text(yaml.safe_dump_all(docs, sort_keys=False))
ops.joinpath('ax-prerequisites.yaml').write_text(yaml.safe_dump_all([d for d in docs if d['kind'] not in ('Deployment', 'Service')], sort_keys=False))
ops.joinpath('ax-ledger-init.yaml').write_text(yaml.safe_dump(job, sort_keys=False))
PY
kubectl apply -f "$OPS_DIR/ax-prerequisites.yaml"
kubectl apply -f "$OPS_DIR/ax-ledger-init.yaml"
kubectl -n ax-system wait --for=condition=complete job/ax-ledger-init --timeout=120s
kubectl -n ax-system logs job/ax-ledger-init
kubectl apply -f "$OPS_DIR/ax-server.yaml"
kubectl -n ax-system rollout status deployment/ax-server --timeout=10m
```

初始化仅新安装执行一次。遇到已存在账本、epoch 不匹配或 DataLoss 必须停止排查，禁止删除 Redis/PVC 后重试。Job `backoffLimit: 0` 保留失败证据；不要写无限重试循环。

私有仓库还需在 ax-system 和 worker 所在 namespace 配置拉取凭据/节点凭据。AX 部署启动本身不要求 kagent 已运行；运行准备和 callback 验收则要求 kagent 可用。

### 7.3 健康与访问隔离

```bash
kubectl -n ax-system get pods,svc,pvc
kubectl -n ax-system logs deployment/ax-server --tail=100
kubectl auth can-i create workerpools.ate.dev --as=system:serviceaccount:ax-system:ax-server -n kagent
kubectl auth can-i create secrets --as=system:serviceaccount:ax-system:ax-server -n kagent
kubectl -n ax-system port-forward svc/ax-server 18080:8080 18443:8443
```

另一个终端执行 `curl --fail http://127.0.0.1:18080/readyz`。`/healthz` 仅存活，`/readyz` 还校验受管 ledger；二者都不证明真实 Worker/golden/callback 链路已通过。

在放行业务前按矩阵创建并验证 NetworkPolicy：

| 目标 | 允许来源 | 协议 |
|---|---|---|
| AX 8443 | kagent controller、授权维护 Pod | TCP + mTLS |
| AX 8080 | 健康检查/明确授权普通 Task 客户端 | 不对外公开；不要让普通入口成为平台入口 |
| Redis 6379 | AX Server、初始化/恢复 Job | 密码认证及平台链路隔离/加密 |
| Substrate API/router | 平台服务、AX | 平台 TLS/身份校验 |
| provider 50051 | egress | mTLS；拒绝其他主体 |

NetworkPolicy 中 namespaceSelector 和 podSelector 应写在同一个 `from` 项，避免“或”匹配扩大权限；还需允许 kubelet 探针和站点监控来源。先在预生产验证 CNI 的 Service DNAT/hostNetwork 行为，再应用生产隔离策略。

## 8. 创建第一个 TaskGroup

在保留端口转发的另一个终端设置同一工作目录变量：

```bash
export AX_CA_FILE="$OPS_DIR/pki/ax-server-ca.pem"
export AX_CLIENT_CERT_FILE="$OPS_DIR/pki/ax-admin.crt"
export AX_CLIENT_KEY_FILE="$OPS_DIR/pki/ax-admin.key"
export AX_SERVER_NAME=ax-server.ax-system.svc
cat > "$OPS_DIR/group.yaml" <<'YAML'
apiVersion: ax.dev/v1alpha1
kind: TaskGroup
metadata:
  name: kagent-default
  atespace: kagent
spec:
  replicas: 4
  sandboxClass: gvisor
  snapshotLocation: gs://YOUR_BUCKET/kagent
YAML
ax --server 127.0.0.1:18443 --atespace kagent group create \
  --file "$OPS_DIR/group.yaml" --request-id install-kagent-default-001
ax --server 127.0.0.1:18443 --atespace kagent group get --name kagent-default
ax --server 127.0.0.1:18443 --atespace kagent group list
kubectl -n kagent get workerpools.ate.dev,pods
```

先把 `YOUR_BUCKET` 替换为真实桶。TaskGroup 是 AX API 资源，**不是 Kubernetes CRD**，不能 `kubectl apply group.yaml`。replicas 是 worker 容量，不会自动创建四个 Agent/Session。保留返回的 UID、resourceVersion 和原 request-id；创建响应丢失时不能换 request-id 重建。

成功标准：组的 UID 固定、后端容量可用；随后在 kagent 手册中创建 Harness/Agent，确认 PreparedRuntime 和真实 Task 工作。仅组存在还不算业务验收完成。

## 9. 运行、升级与恢复

### 9.1 日常观测

保存组 UID、Task/PreparedRuntime refs、operation ID、错误码、时间及 trace ID；不要保存凭据值。关注 Redis 内存/持久化错误、账本 Ready、Worker 容量、准备失败、未决操作、对象存储错误、证书有效期。认证/配置修改需滚动重启相关进程：AX client CA 配置在启动时加载，不能假设所有证书和配置均自动热更新。

新 worker 节点上线先确认版本 label、atelet 就绪、资产和存储可达。不要靠删除 worker Pod 排障来保留运行内存。节点维护前按固定 Substrate `docs/upgrade.md` 做业务停写/挂起与逐节点验证。

### 9.2 未决操作维护入口

先停止业务 admission，停止所有 AX 执行器并从平台侧确认旧后端请求已排空。仅等待锁过期或把 Deployment scale=0 都不足以证明没有迟到请求。

维护 Job 使用与正常 AX 相同镜像、ServiceAccount、Redis、managed.json、epoch、CA 和 token mounts，只修改 args：

```text
保留 --redis-addr / --substrate-* / --managed-config / --managed-ledger-epoch
添加 --recover-atespace=kagent
```

它输出待恢复记录后退出，不启动 API。从输出中选一条完整 JSON 对象，经检查后作为只读 `operation.json` 挂载到第二个维护 Job，再增加：

```text
--recover-operation-file=/run/recovery/operation.json
--confirm-executors-fenced
```

恢复只在原 UID、意图 fingerprint、后端生命周期/配置/凭据等证据匹配时提交 CAS；Delete 可重试原 UID 删除。资源缺失、部分安装、依赖变化、持久化失败时保留未决状态，需要修复平台或恢复一致备份；没有 force-unlock。恢复成功再让 kagent 重试原业务操作，不能手改 kagent DB 指向新实例。

### 9.3 备份与升级边界

联合备份至少包含：Substrate PostgreSQL、AX Redis/AOF、kagent PostgreSQL、快照对象、PKI/Secret、外置 ledger epoch、profile/manifest 与镜像 digest。记录一致恢复点；不要将较老 Redis 备份与较新后端状态任意拼接。

本次仅支持新安装，不支持旧 kagent 实例接管。升级/回滚不能只回滚镜像而忽略状态格式和 wire；保留上一个完整发布包，在隔离环境验证恢复再恢复入口。不提供生产 `helm uninstall` 或删除 PVC 的快捷“重装”步骤。

## 10. 故障定位与发布验收

| 现象 | 优先检查 | 禁止的捷径 |
|---|---|---|
| Pod Pending | PVC/StorageClass、节点 taint/资源、镜像权限 | 删除持久卷强行重装 |
| 挂载 PodCertificate/TrustBundle 失败 | Kubernetes API、kubelet gates、证书控制器和 signer | 关闭 TLS |
| AX 不 Ready | Redis 地址/密码、guard/epoch、已注册账本丢失 | 重新初始化非空账本 |
| TaskGroup 无 ReadyWorkers | 固定 worker image、节点版本 label、gVisor/KVM 资产 | 直接改 AX 管理的 WorkerPool |
| mTLS 成功但 PermissionDenied | 客户端 URI SAN 与 clients/atespace 映射 | 放行所有 URI |
| callback Unauthenticated | provider prefix、Secret 授权、运行 UID/凭据撤销 | 恢复旧 unsigned Actor 头 |
| TLS 到 controller 失败 | MITM 上游信任链、SAN、controller 证书 | skip-verify |
| RPC 未决/冲突 | 原 operation、后端观察、显式维护流程 | 换 request-id 重建 |

发布签收：实际 gVisor 创建/挂起/恢复；golden 与 DATA/FULL；组放置/删除保护；同名新 UID 防护；MITM 注入/撤销；CLI/SDK/流取消；证书轮换；丢响应/迟到请求；Redis/PG/对象存储故障与联合恢复；MicroVM 如启用则单独全测。测试证据附真实镜像 digest 和集群版本，不把本地夹具结果填写为集群通过。

## 附录 A：从空 Linux 节点建立自管 Kubernetes

这是平台建设步骤，不在已有集群重复执行。参考 [kubeadm HA 官方步骤](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/high-availability/) 建立 API 负载均衡入口、至少三个控制平面节点及独立 worker；生产还需 etcd 备份、时间同步、DNS 和证书管理。

1. 每台节点设置唯一 hostname、固定内网地址和时钟同步；安装发行版支持的 containerd，启用 CRI 和 systemd cgroup。按节点既定 swap 策略配置 kubelet；本文路线关闭 swap 并持久化 `/etc/fstab` 修改，不执行通配删除。
2. 配置内核模块/转发（新节点）：

   ```bash
   sudo modprobe overlay
   sudo modprobe br_netfilter
   printf 'overlay\nbr_netfilter\n' | sudo tee /etc/modules-load.d/kubernetes.conf
   printf 'net.ipv4.ip_forward=1\nnet.bridge.bridge-nf-call-iptables=1\n' \
     | sudo tee /etc/sysctl.d/99-kubernetes.conf
   sudo sysctl --system
   sudo swapoff -a
   ```

3. 按 [kubeadm 安装文档](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/install-kubeadm/) 使用组织镜像中的同一 **1.36.x** patch 安装 kubeadm/kubelet/kubectl，冻结自动升级。这里不用过时的在线 patch 号；由平台发行清单指定 `K8S_VERSION` 和相应 package version。
4. 第一控制面准备 `ClusterConfiguration`。下面片段需与站点 `InitConfiguration`、CRI socket、节点地址合并；CIDR 不得与现有网络重叠：

   ```yaml
   apiVersion: kubeadm.k8s.io/v1beta4
   kind: ClusterConfiguration
   kubernetesVersion: v1.36.REPLACE_PATCH
   controlPlaneEndpoint: kube-api.example.com:6443
   networking:
     podSubnet: 10.244.0.0/16
     serviceSubnet: 10.96.0.0/12
   apiServer:
     extraArgs:
       - name: feature-gates
         value: ClusterTrustBundle=true,ClusterTrustBundleProjection=true,PodCertificateRequest=true
       - name: runtime-config
         value: certificates.k8s.io/v1beta1=true
   controllerManager:
     extraArgs:
       - name: feature-gates
         value: ClusterTrustBundle=true,ClusterTrustBundleProjection=true,PodCertificateRequest=true
   ---
   apiVersion: kubelet.config.k8s.io/v1beta1
   kind: KubeletConfiguration
   cgroupDriver: systemd
   featureGates:
     ClusterTrustBundle: true
     ClusterTrustBundleProjection: true
     PodCertificateRequest: true
   ```

5. 替换所有占位值后执行 `sudo kubeadm init --config kubeadm.yaml --upload-certs`。安全保存生成的 join 命令；其他控制面使用 `--control-plane --certificate-key`，worker 使用普通 join。证书密钥/引导 token 不写入日志或 Git。
6. 安装组织固定版本的 CNI（启用 NetworkPolicy）、CSI 和默认 StorageClass，配置 LoadBalancer/Ingress 实现，等待所有节点 Ready。通过 3.1 的 API discovery 和实际证书投射 Pod 检查后才安装 Substrate。

选择 GKE 时，可使用固定 checkout 的 `tools/setup-gcp` 在专用项目建立所需网络、集群、桶和 IAM；按其 README 填写 `PROJECT_ID`、region、可用版本、节点机器类型和嵌套虚拟化，再执行 `go run ./tools/setup-gcp bootstrap`。该开发辅助工具的默认拓扑不是完整生产 HA 设计，需要平台先审核。参考锁定文件，不使用别的 HEAD 的环境例子。

## 附录 B：依据与交接

- 固定后端源码：`944abe3278b895ccbf5d45555a49dd0f2f6ceae7` 的 `hack/install-ate.sh`、`docs/authentication.md`、`docs/egress-trust-bundle.md`、`tools/setup-gcp/README.md`、`manifests/ate-install/atenet-router.yaml`。
- 当前 AX：`cmd/ax-server/main.go`、`managed.go`、`internal/substrate/managed_platform.go`、`managed.go`、`deploy/`。
- Kubernetes 证书机制：[官方证书与 CSR 文档](https://kubernetes.io/docs/reference/access-authn-authz/certificate-signing-requests/)；实际启用要求以固定平台版本和 API discovery 为准。
- 交给 kagent 管理员：AX 8443 地址/SAN/CA、controller client 证书与授权 atespace、TaskGroup 名称和 UID、callback HTTPS 地址、MITM 信任与 provider 验收记录、镜像 digest 和备份责任人。
