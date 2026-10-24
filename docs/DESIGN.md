# Echo Sync — design (retired)

The design record now lives in two files:

- [`docs/SPEC.md`](SPEC.md) — the normative specification, derived from the code. It is the
  contract; a behavior change updates it in the same branch.
- [`docs/DECISIONS.md`](DECISIONS.md) — the dated rulings that produced the current contract,
  numbered as the former §16 of this document.

This file is kept only so that `DESIGN §x` citations in source comments and commit messages
still resolve. They use the retired numbering below; replacing them with SPEC references is a
separate task.

| retired `DESIGN §` | subject | now in |
|---|---|---|
| §1 | one-sentence model | SPEC §1 |
| §2 | goals and non-goals | SPEC §1 |
| §3 | architecture | SPEC §1, §12 |
| §4 | access scopes, grants, `principalFor` | SPEC §4, §8 |
| §5 | `SyncTable`, column kinds, codecs, `dataHash` | SPEC §3 |
| §6 | mutations, ledger, outcomes, retry; pokes and liveness | SPEC §5, §10 |
| §7 | log ordering invariant, `next_position()` | SPEC §6 |
| §8 | pull: head, membership diff, walks, continuation, ack and release | SPEC §7, §9 |
| §9 | client: outbox, staging, install, rebase, leader topology | SPEC §9, §12 |
| §9.1 | tab-side API, `resetReplica`, credentials, version gate | SPEC §9, §10, §12 |
| §10 | queries, store ordering, the single comparator | SPEC §3, §12 |
| §11 | schema evolution, migrations, startup verification | SPEC §13 |
| §12 | transport, presence | SPEC §10 |
| §13 | Effect usage rules | `AGENTS.md` |
| §14 | testing philosophy | SPEC §17, `AGENTS.md` |
| §15 | prior art | dropped |
| §16 | open items and rulings | DECISIONS 1–32 |
