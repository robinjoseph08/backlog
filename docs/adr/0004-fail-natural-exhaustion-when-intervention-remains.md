---
status: proposed
---

# Fail natural exhaustion when intervention remains

`backlog run` will distinguish orderly scheduler shutdown from successful backlog completion. Natural one-shot exhaustion will show every Intervention-required Run in an Attention Required section and exit nonzero when any remain, so automation cannot interpret a stopped scheduler as verified Completion. Signal-triggered Drain will retain its successful exit semantics after its Owned Workers stop because it reports an orderly shutdown, not the outcome of the backlog.
