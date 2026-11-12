# iam

A general IM engine built for 10B scale.

## Docs

- Design direction: [`docs/design.md`](docs/design.md)
- Message protocol: [`docs/protocol.md`](docs/protocol.md)
- Storage design: [`docs/storage.md`](docs/storage.md)
- Changelog: [`CHANGELOG.md`](CHANGELOG.md)
- Contributing: [`CONTRIBUTING.md`](CONTRIBUTING.md)

## Stack

- Language: Rust
- Storage component: TiDB SQL with TiKV storage and PD coordination (local native text path)
- Latency target: same-region server core path P99 &lt; 10 ms

## 当前实现与验收范围

当前是可测试的**内存协议内核**，部分满足 [`docs/protocol.md`](docs/protocol.md)，不是可部署 IM 服务。

- `iam-protocol`：FixedHeader、Protobuf、收帧与线格式校验
- `iam-core`：AUTH / RECONNECT / SEND / SYNC / SNAPSHOT / HISTORY 等内存实现
- `iam-loop`：进程内闭环测试驱动（非网关、非客户端 SDK）
- `iam-store-tidb`：两表 SQL 存储及复用 Engine 认证的本机文本入口；支持成功消息写入、去重、查询和分页，不是完整协议的持久 SEND / SYNC

默认协议路径仍使用 `MemoryStore`，原内存行为与套件保留。TiDB 入口默认只读；新消息接受仅限显式声明的全新空库和本进程新建的会话，重启不凭查空恢复写入。使用、建表和真实 TiDB 验证见 [存储实现说明](docs/storage.md#实现入口与验证)。

受当前构建环境限制，TiDB 组件尚未完成编译、回归和真实数据库验证；不能视为已验收。

后续阶段再做：TCP+TLS 网关、完整协议持久化与故障恢复、真实鉴权、客户端落盘与恢复。本阶段不引入网关运行时或产品权限框架。

## 验证

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
