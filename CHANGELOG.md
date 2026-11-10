# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once the first tagged release ships.

Version sections should match git tags when releases begin. Until then, keep
user-visible and design-visible work under `[Unreleased]`, and move entries into
a dated version section when cutting a release.

## [Unreleased]

### Added

- Project design direction for a Rust-based general IM engine (`docs/design.md`).
- Basic message protocol design with typed structures, command contracts,
  state transitions, recovery flows, and acceptance scenarios (`docs/protocol.md`).
- Contributor conventions for branching, one-line English commits, and PRs
  (`CONTRIBUTING.md`).
- Protocol core crates: frozen `iam.protocol` IDL, `iam-protocol` frame/codec,
  in-memory `iam-core` AUTH/SEND/PUSH/SYNC semantics, and `iam-loop` closed-loop
  integration tests.
- Protocol wrap-up (in-memory): wire `ERROR` routing, `RECONNECT` with session
  replace, SYNC cold-start `reset`, `SNAPSHOT` / `HISTORY`, account sync stream,
  and expanded closed-loop acceptance tests.



### Changed

- Align protocol and design guidance on ordering, idempotency, synchronization,
  connection recovery, compatibility, and failover boundaries.
- Expand the protocol design with explicit framing, send outcomes,
  synchronization, snapshots, history reads, and default limits.
- Specify stable protocol field numbers, capability negotiation, token lifetimes,
  bounded idempotency, identifier encoding, and deterministic error handling.
