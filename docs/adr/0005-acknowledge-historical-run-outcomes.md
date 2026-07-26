---
status: proposed
---

# Acknowledge Historical Run outcomes without deleting history

Backlog will let operators acknowledge Historical Runs that did not reach Completion, hiding acknowledged outcomes from default status while preserving their state, diagnostics, logs, sessions, and Follow access. Explicit acknowledgment avoids allowing unseen failures to disappear through automatic expiry, while a bounded recent Completion view prevents successful history from making operational status grow forever.
