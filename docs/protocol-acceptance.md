# Protocol acceptance checklist (in-memory phase)

Tracks [`protocol.md`](./protocol.md) §实现顺序与验收 against the current Rust workspace.
Scope: no middleware, no TCP gateway, no client SDK.

Legend: `done` automated; `partial` logic present / light coverage; `n/a` out of this phase; `open` still incomplete.

| # | Acceptance item | Status | Notes |
|---|-----------------|--------|-------|
| 1 | Framing half/multi-frame, illegal length, memory bounds | done | `iam-protocol` samples |
| 2 | Lost ACK same-key retry → same receipt; different content no new message | done | closed_loop + windows |
| 3 | Serial barrier while unresolved; closed unknown can proceed | done / partial | barrier done; closed-unknown next-send lightly covered |
| 4 | Status miss inside window ≠ final fail; rate limit does not overturn commit | done / partial | NOT_FOUND path done; rate-limit entrance tested separately |
| 5 | Re-auth after lost connection state; pending send + cursors | partial | RECONNECT covered; no durable client queue |
| 6 | Lost first/last push discovered by active SYNC | done | reset→snapshot recover |
| 7 | Edit/recall reorder does not regress | n/a | write APIs out of protocol scope |
| 8 | Client crash mid-persist / duplicate sync pages | n/a | no SDK |
| 9 | Snapshot paging with concurrent changes | partial | snapshot path exists; concurrency edge not stressed |
| 10 | Cursor/page expiry, unknown required events, history rollback | partial | cursor expiry→reset; unknown events n/a |
| 11 | push_ack capability matrix | done | enabled / not enabled ERROR |
| 12 | Token tamper / cross-identity / purpose mix-up | done | tokens + compliance tests |
| 13 | Accept/query window boundaries + close watermark cleanup | done | `iam-core` windows tests |
| 14 | Presence / duplicate fields / unknown branches cross-language | partial | Rust-only vectors |

## Phase verdict

In-memory **compliance reinforcement** for the checklist items that apply without gateway/SDK/storage is largely in place. Remaining `partial`/`open` items need either a later phase (gateway, SDK, durable store) or deeper concurrency fuzzing—not claimed as full `protocol.md` absolute compliance.
