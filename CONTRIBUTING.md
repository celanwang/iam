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
| `proto/` | protocol drafts and experiments |

Current design work uses `docs/design`.

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

## Design source of truth

- Product and architecture direction: [`docs/design.md`](docs/design.md)
- Do not rewrite the design doc casually; extend via ADR or a follow-up docs PR when semantics change.
