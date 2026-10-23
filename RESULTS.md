# Measured Results — Event-Sourced DQ Rule Governance (run 2026-10-19)

## Environment (report these exact values in Methodology §4.6)
- Single cloud VM: 2 vCPU, 7 GB RAM, Linux 6.18 x86_64
- Python 3.13.16, DuckDB 1.5.6, dbt-core 1.12.6 + dbt-duckdb 1.11.0, Synthea (master build), SHA-256
- Kafka: in-process stand-in (8 partitions keyed by rule id, offsets, at-least-once with 5% injected duplicates). Not a real Kafka cluster.

## Data (corrections vs. Methodology draft)
- Synthea run `-p 10000` produced **11,508 patient records** (10,000 living + deceased); 1,937,028 numeric observations, 207,809 conditions, 238,260 medications.
- 240 rules over 6 templates: T1 impossible 49, T2 implausible 49, T3 diagnosis–sex 15, T4 age–diagnosis 40, T5 lab-unit 47, T6 medication–diagnosis 40. T3 is smaller because only 15 sex-specific conditions occur ≥20 times; the other templates fill to 240.
- Injected labeled errors: 1% → 16,272; 5% → 81,361; 10% → 162,722.
- Environment samples: dev 235, uat 608, prod_test 1,180 patients; prod = full data (5% error file).

## Runs
- Light load: 20 authors, 300 commands/run, idle-rule targeting, 10 reps → 2,699 approved changes.
- Contention (saturation): N ∈ {1, 5, 10, 20} authors, 2,000-command burst, Zipf (s = 1.1) hot rules, 10 reps each.
- Offered-load sweep: 6 levels, 3 reps each. Poll-interval sweep: 4 levels, 3 reps each.
- Baseline: dbt builds actually executed and timed (5 reps per env); scheduler simulated, 10 × 2,000 Poisson approvals.
- 80 event-sourced runs in total.

## M1 Propagation latency (approval → active in prod)
| Condition | n | Median (95% CI) | p95 | p99 |
|---|---|---|---|---|
| **Event-sourced, light load (N=20)** | 2,699 | **0.092 s (0.090–0.094)** | 0.143 s | 0.167 s |
| Baseline on-demand selective (10/h) | 20,000 | 25.5 s | 33.3 s | 47.5 s |
| Baseline on-demand selective (50/h) | 20,000 | 25.8 s | 59.7 s | 81.2 s |
| Baseline scheduled Δ = 5 min | 20,000 | 240.8 s | 375.1 s | 387.4 s |
| Baseline scheduled Δ = 15 min | 20,000 | 542.1 s | 943.8 s | 981.2 s |
| Baseline scheduled Δ = 30 min | 20,000 | 1,000.0 s | 1,801.2 s | 1,871.2 s |

Mann–Whitney U (one-sided, ES < baseline): p < 10⁻³⁰⁰ for every comparison. Cliff's δ = −1.00 for every comparison: every event-sourced latency was lower than every baseline latency. Median speed-up: 277× vs on-demand (10/h), 2,622× vs Δ = 5 min, 10,887× vs Δ = 30 min.

Measured dbt build time (mean of 5): full rebuild 22.1–23.4 s per env, **90.0 s** for all 4; selective (one rule) 6.3–6.4 s per env, **25.5 s** for all 4. On-demand baseline capacity ≈ 1 / 25.5 s = **0.039 changes/s (≈141/h)**.

## M2 Capacity and contention
Offered-load sweep (event-sourced, measured arrival rate):
| Arrival (changes/s) | Median | p95 |
|---|---|---|
| 0.56 | 0.095 s | 0.141 s |
| 1.12 | 0.089 s | 0.134 s |
| 3.55 | 0.101 s | 0.152 s |
| 8.30 | 0.099 s | 0.159 s |
| 17.35 | 0.108 s | 0.204 s |
| 36.68 | 0.240 s | 1.718 s |
Latency stays near 0.1 s up to ~17 changes/s; the knee appears at ~37 changes/s (p95 1.7 s). That is ~900× the on-demand baseline's capacity.

Contention (2,000-command bursts, ~3,000+ commands/s accepted): conflicts per command 4.3% (N=1), 12.7% (N=5), 22.4% (N=10), 31.4% (N=20); every conflict was resolved by retry (0 commands exhausted 20 retries). Under these deliberate bursts, propagation latency rose to a median of 11.9–13.9 s because promotion gates queue behind the backlog; this is an overload test, not normal operation.

## M3 Audit reconstruction
20,100,829 (rule, environment, timestamp) states sampled across 80 runs: **100% matched** the independent oracle. Final projection equalled the oracle in 80/80 runs; 0 version gaps or duplicates. (A first contention run showed 4 mismatches out of 13.2M; traced to the oracle not recording a seeded bootstrap version — fixed in the oracle only, contention block re-run: 11,083,883 / 11,083,883 matched.)

## M4 Tamper detection (200 trials per type, 20-author log)
| Attack | Chain only | Anchor every 100 | every 50 | every 10 | every event |
|---|---|---|---|---|---|
| Modify field | 100% | 100% | 100% | 100% | 100% |
| Modify + rehash event | 100% | 99.5% | 99.5% | 100% | 100% |
| Delete event | 100% | 100% | 99% | 100% | 100% |
| Reorder adjacent | 100% | 100% | 100% | 100% | 100% |
| Insert forged event | 100% | 100% | 100% | 100% | 100% |
| Insider rewrites suffix (rehashes all later events) | 0% | 89% | 95.5% | 99% | 100% |
| Truncate tail | 0% | 23% | 67% | 90.5% | 100% |
The < 100% cells come from the newest events after the last anchor (no successor or anchor yet covers them).

## M5 Policy enforcement
Self-approval 1,897/1,897 blocked; high-risk approval without client-confirmation reference 705/705 blocked; author attempting direct promotion 987/987 blocked (100%). Duplicate deliveries: 17,574 delivered, 17,574 absorbed (idempotent).

## M6 Rollback latency (rollback command → restored in all envs)
Light load: median 0.051 s, p95 0.098 s (n = 301). Load sweep: median 0.055 s. Under contention bursts: median 2.45 s.

## M7 Control check
dbt-materialized violation sets (baseline) vs. runtime-rendered rules (event-sourced): **0 of 240 rules differ** → the governance layer does not change what rules detect.
Rule detection vs injected ground truth (5% file): recall 1.000 for T1–T5, 0.998 for T6; precision 1.00 (T1, T6), 0.99 (T3, T4), 0.61 (T5), 0.48 (T2). Low T2/T5 precision is expected: implausible-range bounds are the clean 0.1–99.9 percentiles and the unit rule flags legitimate minority units — the "possible but implausible" category Wang et al. warn needs validation. Not a governance result.

## Figures
- fig3_latency_cdf — CDF of propagation latency (log x), ES vs all baselines
- fig4_throughput — ES median/p95 latency vs offered load, with baseline capacity line


## Review-v2 experiments (run 2026-10-19/20)

### Replication (10 light-load runs, logs kept)
- ES median 0.0846 s (95% CI 0.0824–0.0866), p95 0.132, p99 0.148, n = 2692. Versus the original run: Cliff's δ = 0.14 (small; run-to-run variation).
- Reconstruction 2,108,097/2,108,097; chain valid in all runs.

### Baseline B2 — mutable parameter table + audit triggers (SQLite 3.45.1, same handler policy code, same seeds)
- Latency median 0.0091 s, p95 0.0157, p99 0.0203 (n = 2696). B2 is faster than ES by 75 ms (Cliff's δ = 1.00). **The sub-second latency comes from run-time parameter binding, not from event sourcing.**
- Point-in-time reconstruction from the intact audit table: 2,129,261/2,129,261.
- Tamper detection, using the strongest structural check (contiguous keys + key counter + monotonic timestamps), 200 trials each: {"modify_field": 0.0, "modify_and_rehash": 0.0, "delete_event": 100.0, "reorder_adjacent": 51.0, "insert_forged": 100.0, "rewrite_suffix": 0.0, "truncate_tail": 0.0} (%).
- Policy: 104/104 injected violations blocked (same handler code).

### Workload-mix sensitivity (3 runs each)
- baseline: mix {'update': 0.4, 'relax': 0.3, 'create': 0.15, 'rollback': 0.1, 'retire': 0.05}, median 0.0846 s, p95 0.132 s, high-risk share 45.2%, reconstruction 100%, verifier violations 0
- create_heavy: mix {'update': 0.2, 'relax': 0.15, 'create': 0.55, 'rollback': 0.05, 'retire': 0.05}, median 0.0829 s, p95 0.131 s, high-risk share 22.1%, reconstruction 100%, verifier violations 0
- relax_heavy: mix {'update': 0.1, 'relax': 0.7, 'create': 0.1, 'rollback': 0.05, 'retire': 0.05}, median 0.0855 s, p95 0.132 s, high-risk share 78.8%, reconstruction 100%, verifier violations 0
- rollback_heavy: mix {'update': 0.3, 'relax': 0.2, 'create': 0.1, 'rollback': 0.35, 'retire': 0.05}, median 0.0841 s, p95 0.137 s, high-risk share 44.6%, reconstruction 100%, verifier violations 0
- tighten_heavy: mix {'update': 0.7, 'relax': 0.1, 'create': 0.1, 'rollback': 0.05, 'retire': 0.05}, median 0.0844 s, p95 0.135 s, high-risk share 27.3%, reconstruction 100%, verifier violations 0

### Independent verifier (invariants.py)
- 22 retained ES logs, 46,255 events: violations {'C1': 0, 'C2': 0, 'C3': 0, 'C4': 0, 'C5': 0, 'C6': 0, 'C7': 0, 'C8': 0, 'C9': 0, 'C10': 0}.
- Mutation self-test (40 per type): {"C1_duplicate_event": 1.0, "C2_edit_without_rehash": 1.0, "C3_self_approval": 1.0, "C3_author_promotes": 1.0, "C4_no_justification": 1.0, "C4_risk_mislabelled": 1.0, "C5_skip_environment": 1.0, "C5_promote_unapproved": 1.0, "C6_lost_update": 1.0, "C7_event_not_applied": 1.0, "C8_patient_identifier": 1.0, "C10_projection_drift": 1.0}.

All manuscript numbers are generated from `paper/numbers.json` by `analysis_v2.py`.

## Review-v3 experiments (run 2026-10-22; numbers in paper3/numbers_v3.json)

### Baseline B3 — B2 + hash-chained audit rows + anchors (10 light-load runs, same seeds)
- Latency median 0.0114 s (95% CI 0.0113–0.0116), p95 0.020, p99 0.025; B2 0.0091 s; ES replication 0.085 s
- Per-run medians: ES slower than B2 by 75 ms and than B3 by 73 ms (Mann–Whitney p < 0.001, Cliff's δ = 1); B3 − B2 = 2.3 ms
- Reconstruction 2,130,149 / 2,130,149 sampled states; final state correct in every run
- Tamper (200 trials/attack): chain only detects edits/deletes/reorders/inserts (100%) but 0% suffix rewrite and truncation;
  anchor every 50 rows: rewrite 99.5%, truncation 94.5%; anchor every row: 100% for all seven attacks
- Policy: 103 of 103 injected violations blocked

### Impact gate, 5% injected faulty (low-risk-rated) changes (10 runs)
- 126 / 131 faulty changes blocked (96.2%): 108 in dev, 12 in uat, 6 in prod_test; 5 reached production
- 38 / 2,566 ordinary changes blocked (1.5%); median latency of ordinary changes 0.094 s (vs 0.085 s without gate)
- Verifier: 0 violations in the 10 gate logs
- Diagnostics (gate_diag.py, 3 seeds, every decision logged): the 2 misses were T3 sex-specific rules with <= 15 in-scope
  records in every test env (could not exceed the 5-record minimum); of 10 ordinary changes blocked, 5 were new rules
  (no baseline) and 5 were range narrowings on skewed distributions that flagged 258–601 vs 11–54 records

### Ablations (3 light-load runs each; independent verifier)
- No command-layer policy: 15 self-approvals + 13 unjustified approvals accepted -> verifier C3 = 15, C4 = 13;
  6 author promotions targeted a superseded version and wrote no event; final state correct 3/3
- No deduplication: C7 flagged in 3/3 runs; 412 duplicate promotion events; final state still correct 3/3
- Random partitions: C1 4,974, C4 307, C5 4,517, C6 4,138 violating events; C10 in 3/3 runs; reconstruction 66%;
  30 projector write conflicts; median latency 0.260 s; final state wrong in 3/3 runs

### Verification cost (32 ES logs, 67,261 events)
- Chain + anchor check 12 µs/event; replay 1.0 µs/event; full verifier 80 µs/event; hash on append 53 µs/event;
  B3 chain check 9.0 µs/row (41,682 rows)

### Approval lead-time model (log-normal approval wait, σ = 1)
- Median 15 min: ES 15.0 min, B1 every 30 min 33.8 min (40% of mean lead time is propagation)
- Median 1 day: all designs ≈ 1,441–1,457 min; propagation < 1% of lead time
