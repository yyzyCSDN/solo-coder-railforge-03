# RailForge Public Benchmark

A dependency-free freight railway operations benchmark with a public smoke suite. Private task prompts, hidden regressions and broken snapshots are deliberately excluded from this package.

## Cross-sector crew connections with lodging guarantee

`railforge.crew.connection` evaluates one connection order against four
resources at once: 值乘/驾驶时长合法性 (`crew.duty`), 接续点容量, the effective
资格 rule (`crew.qualification`), and a concrete 住宿窗口 with a free bed.
`ConnectionLedger` (`crew.connection_ledger`) versions every resource as a
"代次" and commits through a single multi-resource compare-and-swap: a stale
generation on **any** resource rejects the whole order before a single value
changes, so a failed order never leaves a half-written roster.

`CrewConnectionService` (`crew.connection_workflow`) adds the state plane on
top of the hash-chained `EventStream` and audit `Trail`: commits publish the
full effect, rejections publish a rejection event without mutating state, and
`replay_stream`/`verify_replay` rebuild the ledger from events and prove it
matches the live state. Tests: `tests/test_crew_connection.py` (joint
validation, stale-generation atomicity, audit chaining, replay and
concurrent conflict resolution).
