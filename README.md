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
