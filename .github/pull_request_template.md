## Architecture (before)

Unchanged vs `main` / first baseline (delete whichever is wrong).

```mermaid
flowchart TB
```

```mermaid
classDiagram
```

## Architecture (after)

```mermaid
flowchart TB
```

```mermaid
classDiagram
```

## User behavior (when X, used to Y, now Z)

- When a caller runs **start**, it used to Y. Now it Z.
- When a caller runs **wait**, it used to Y. Now it Z.
- When a caller runs **cancel** (or drops `ExecutionHandle`), it used to Y. Now it Z.
- When a caller runs **resume**, it used to Y. Now it Z.
- When a caller runs **fail** (executor Failed / TimedOut, policy Accept), it used to Y. Now it Z.
- When a caller runs **retry**, it used to Y. Now it Z.
- When a caller runs **inspect**, it used to Y. Now it Z.

## Phase 2+ review gate

- [ ] Named crash-resume test on a real sqlite file (or this PR does not
      change persist/resume behavior).
- [ ] CI jobs green: `test`, `adversarial`, `coverage`, `stress-resume`
      (`stress-100k` is separate; `chaos-sqlite` is the standing sqlite
      load breaker). No `continue-on-error`.
- [ ] `docs/RESUME_CATALOG.md` row updated (`test:` name, not MISSING).
- [ ] `docs/CHAOS_LOG.md` attack → test name if this PR hunts sqlite load.
