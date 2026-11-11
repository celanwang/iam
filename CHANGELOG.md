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
- Protocol compliance reinforcement: accept-window/close-watermark tests,
  token key-id/purpose checks, SYNC `max_bytes` paging, entrance `RATE_LIMITED`,
  and ERROR matrix / account / HISTORY loop coverage.

### Changed

- Shorten README implementation-scope notes; keep a brief in-memory kernel
  summary and deferred next-stage list.

### Fixed

- Validate nested wire occurrences, scalar ranges, required response fields and
  response invariants before applying data; reject unknown write semantics.
- Route malformed frames and unsupported commands through protocol errors,
  preserve business error routing, and persist invalid-text send rejections.
- Bound frame buffering, count decode fields across the whole frame, generate
  UUIDv7 identifiers with independent random bits, and bound UTF-8 diagnostics.

- Rebuild snapshot and history pages at fixed upper bounds, recheck access,
  validate history contexts, and apply byte budgets to complete paging responses.

- Enforce send-result expiry and closure cleanup consistently, retain terminal
  rejections, and invalidate connections on closing protocol errors.
- Isolate tenant data and device sessions and deliver messages to other sender devices.

### Security

- Encrypt opaque tokens with random nonces and per-engine keys, bind tenant
  identity, and support retained verification keys during rotation.

### Changed

- Document the tested in-memory implementation scope, verification commands,
  and remaining transport, durability, authentication and client obligations.

- Align protocol and design guidance on ordering, idempotency, synchronization,
  connection recovery, compatibility, and failover boundaries.
- Expand the protocol design with explicit framing, send outcomes,
  synchronization, snapshots, history reads, and default limits.
- Specify stable protocol field numbers, capability negotiation, token lifetimes,
  bounded idempotency, identifier encoding, and deterministic error handling.
