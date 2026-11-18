# Contributing

## Branching

- Do not commit feature or docs work directly on `main`.
- Create a focused branch from the latest `main`, then open a PR to merge.
- Prefer short-lived branches with a clear prefix:

| Prefix | Use |
|---|---|
| `docs/` | design notes, ADRs, documentation |
| `feat/` | user-visible or engine features |
| `fix/` | bug fixes |
| `chore/` | tooling, ignore files, non-product chores |
| `ci/` | automated checks |
| `proto/` | protocol drafts and experiments |

Current design work uses `docs/design`. Protocol design used `proto/message`.
Storage design uses `docs/storage`.

## Commits

Follow [Conventional Commits](https://www.conventionalcommits.org/) with these project rules:

1. **One line only** — subject line, no body unless a rare exception is explicitly requested.
2. **English only**.
3. Format: `type(scope): summary`
4. Keep the summary concise and specific.

Common types: `feat`, `fix`, `docs`, `chore`, `test`, `refactor`.

Examples:

```text
docs(design): add overall IM engine design outline
feat(gateway): accept websocket frame handshake
fix(sync): retry gap fill on missing conv_seq
```

Avoid multi-paragraph messages, Chinese commit subjects, and stacking unrelated changes in one commit.

## Pull requests

- Open a PR into `main` for review and merge.
- Keep the PR scoped to one concern when practical.
- Design freezes and architecture changes should reference `docs/design.md`.

## Checks

The [Workspace workflow](.github/workflows/workspace.yml) runs on pull requests
and pushes to `main`. It checks formatting, runs Clippy for all targets and
features, and runs the workspace tests with all features. Clippy and tests use
`Cargo.lock`. To reproduce the checks locally, use the workflow's Rust version.

```sh
rustup toolchain install 1.99.1 --profile minimal --component rustfmt --component clippy
cargo +1.99.1 fmt --all -- --check
cargo +1.99.1 clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo +1.99.1 test --locked --workspace --all-features
```

Real TiDB tests are ignored by default. Use a dedicated loopback TiDB instance
to run the relevant storage, client sync, and TCP + TLS suites locally. The test
harness creates a separate `iam_test_*` database per case.

```sh
export IAM_TIDB_TEST_URL='mysql://root@127.0.0.1:4000/iam_test'
export IAM_TIDB_ALLOW_TEST_WRITES=1
cargo test --locked -p iam-store tidb_ -- --ignored --skip perf:: --skip tidb_reliability_mixed_load --test-threads=1
cargo test --locked -p iam-gateway --test tls -- --ignored --skip capacity:: --test-threads=1
```

See the [local storage validation guide](docs/storage.md#本机控制台与验证) for setup
and the [gateway performance guide](docs/gateway-perf.md) for capacity runs.

## Changelog

- Notable changes belong in [`CHANGELOG.md`](CHANGELOG.md) under `[Unreleased]`.
- Follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) categories:
  `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`.
- Write entries in English, short and concrete; group by category, not by commit.
- Do not list every commit. Skip pure chores unless they affect contributors
  (for example ignore rules or required workflow docs).
- When cutting a release, move `[Unreleased]` items into a dated `## [vX.Y.Z]`
  section that matches the git tag.

## Design source of truth

- Product and architecture direction: [`docs/design.md`](docs/design.md)
- Do not rewrite the design doc casually; extend via ADR or a follow-up docs PR when semantics change.
