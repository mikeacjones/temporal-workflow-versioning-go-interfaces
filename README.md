Example showing using versioning via patching, but keeping separate interface implementations to resolve version.

This becomes a form of workflow versioning, without using different registered names for the workflows.

The structure allows for automating scaffolding.

The just file provides the following commands:

`just bump {name}` <- takes the current version and creates the frozen version file, and bump the current version by 1. Updates the resolver as well. Eg: `just bump processOrder`

`just new {name}` <- scaffolds a new workflow with an immediate v1 marker and resolver

`just retire {name}` <- by default, retires the oldest version and removes it from the resolver

`just retire {name} {version}` <- retires the specified version (and all older versions)

## Progressive rollout demo (TUI)

`tui/` is an interactive demo that shows **why you might let callers pin a
workflow version when starting it, instead of always defaulting to "latest".**

The latest version (`v3`) ships with a deliberately buggy new step:
`SendThankYou` always fails. Because activity retries are unbounded, latest
orders get *stuck* retrying it forever rather than terminally failing — older
pinned versions (`v1`, `v2`) never call that step, so they are immune.

### Run it

```sh
temporal server start-dev      # terminal 1: Temporal dev server (UI at http://localhost:8233)
go run ./worker   # terminal 2: the order-processing worker
go run ./tui       # terminal 3: the demo
```

(Set `TEMPORAL_ADDRESS` if your server isn't on `localhost:7233`.)

### What it demonstrates

The demo models a **live, continuous order stream** — several orders a second,
flowing the whole time. Under the hood every order is quietly pinned to the
known-good stable version (`v2`); callers never specify a version, "start" just
means "process this order". That pinning is exactly what makes a controlled
rollout possible.

- **`[s]` start/stop the stream** — steady-state production traffic on `v2`.
- **`[v]` roll out v3** — progressively shift the *incoming* stream to v3 in
  stages (10 → 25 → 50 → 75 → 100%), holding at each step to watch the v3
  cohort. The moment too many v3 orders are stuck it **auto-halts** and routes
  the entire stream back to `v2`. Only the small canary slice is ever affected;
  the bulk of traffic keeps completing on the safe version.
- **`[n]` naive cutover** — the "always start at latest" mistake: flip 100% of
  the live stream to v3 at once, no gating. Watch every new order get stuck.
- **`[h]`** halts a rollout and reverts the stream to `v2`; **`[r]`** resets;
  **`[q]`** quits.

Tune order duration with `WORK_DURATION` (e.g. `WORK_DURATION=2s go run ./worker`)
to slow orders down and build a larger in-flight population.

### The self-heal payoff

The stuck v3 orders are *retrying*, not failed. Fix `SendThankYou` in
`activities/processOrderActivities.go` (return success instead of an error),
re-run `just worker` to redeploy, and the in-flight v3 executions self-heal on
their next retry and complete — no restart, no data loss. Then press `[v]` to
resume the rollout, which now ramps cleanly to 100%.
