# Migration plans

Versioned plans for the GoaT data import pipeline migration. The newest
version is the source of truth; older versions are preserved verbatim so the
evolution of the plan stays auditable.

## Current

- [v2-current-plan.md](v2-current-plan.md) — Phase 1 complete, Phase 2 in
  progress (cleanup tasks tracked), Phases 3–5 outlined.

## History

- [v1-initial-plan.md](v1-initial-plan.md) — original plan written at project
  kickoff. Defines the five-phase framing, the gap analysis, and the
  network-robustness / logging / conventions reference that v2 still relies on.

## Versioning convention

- The newest plan is always named `vN-current-plan.md`.
- When a new revision lands, the previous `current` file is renamed to
  `vN-<short-descriptive-suffix>.md` (e.g. `v1-initial-plan.md`) and a new
  `v(N+1)-current-plan.md` is added.
- Historical plans are never edited after archival — corrections go into the
  new version's change log.
- Each new plan ends with a `Change log` section summarizing what changed
  versus the previous version.
