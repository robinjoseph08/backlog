---
status: proposed
---

# Acknowledge Historical Run outcomes without deleting history

Backlog will let operators explicitly acknowledge Historical Runs with `failed` or `needs-human` outcomes, while completed Reset outcomes are already handled. Unexpected non-Completion outcomes remain visible until acknowledged; handled outcomes are hidden from default status without deleting history, and full history remains available on demand.

Acknowledgment adds only presentation metadata and does not change lifecycle state, Lease ownership, Candidate eligibility, GitHub state, or retained artifacts and diagnostic access. A bounded recent Completion view prevents successful history from making operational status grow forever, while every Completion with pending cleanup remains visible until cleanup finishes.
