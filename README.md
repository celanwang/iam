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
- Latency target: same-region server core path P99 &lt; 10 ms

## 当前实现与验收范围

当前是可测试的**内存协议内核**，部分满足 [`docs/protocol.md`](docs/protocol.md)，不是可部署 IM 服务。

- `iam-protocol`：FixedHeader、Protobuf、收帧与线格式校验
- `iam-core`：AUTH / RECONNECT / SEND / SYNC / SNAPSHOT / HISTORY 等内存实现
- `iam-loop`：进程内闭环测试驱动（非网关、非客户端 SDK）

后续阶段再做：TCP+TLS 网关、持久化与故障恢复、真实鉴权、客户端落盘与恢复。本阶段不引入网关运行时、数据库或产品权限框架。

## 验证

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
