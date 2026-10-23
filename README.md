# Event-Sourced Governance of Data Quality Rules — Simulation and Evaluation

Code, raw results, and analysis scripts for the paper *Event-Sourced Governance of Data Quality
Rules for Real-Time, Auditable Healthcare Data Platforms* (manuscript under review).

The study asks whether event sourcing can make changes to data quality (DQ) rule parameters both
fast and verifiable. Every rule-lifecycle action (create, update, approve, promote, roll back,
retire) is an immutable, SHA-256 hash-chained event in a log partitioned by rule; separation of
duties and risk-tiered approval are enforced before an event is written; idempotent projections
drive gated promotion dev → uat → prod_test → prod; and the log head is anchored outside the
platform. The design is compared with three baselines on synthetic healthcare data:

| Design | Description |
|---|---|
| **ES** | Event-sourced design (this work) |
| **B1** | Rules as dbt models, deployed by on-demand or scheduled rebuilds (builds actually executed and timed) |
| **B2** | Mutable parameter table with a trigger-written audit table (SQLite) |
| **B3** | B2 plus a hash-chained audit table with anchors (ledger-table style) |

All data are synthetic (Synthea). No patient data or employer data are used.

## What is real and what is a stand-in

This is a **single-process simulation**, not a production deployment.

| Component | In this study | Production equivalent |
|---|---|---|
| Patient data | Synthea, 10,000 Massachusetts patients, 3 years of history, seed 42 | EHR / claims feeds |
| Analytical store | DuckDB files, one per environment | Snowflake |
| Rule builds (B1) | dbt-core + dbt-duckdb, actually executed and timed | dbt on Snowflake |
| Event log | In-process partitioned log (8 partitions keyed by rule id, offsets, at-least-once delivery with 5% injected duplicates, persisted as JSONL) | Apache Kafka |
| Hash chain | SHA-256 per partition, external anchors every *k* events | Same, with anchors in a separate trust domain |
| B1 waiting time | Discrete-event simulation of a scheduler (5/15/30 min) using the measured dbt build times | CI/CD scheduler |

## Repository layout

```
dqsim/        all Python code (run every command from the repository root)
results/      summary results and logs used in the paper (raw run directories are not included)
RESULTS.md    measured results in plain text
```

| File | Purpose | Paper |
|---|---|---|
| `dqsim/prepare_data.py` | Synthea → DuckDB, knowledge tables, labeled error injection (1/5/10%), environment samples | §2.4 |
| `dqsim/rules.py` | Six rule templates rendered to SQL; parameter validation; risk tiering | §2.1, eq. (1) |
| `dqsim/es_system.py` | Partitioned hash-chained log, command handler (roles, separation of duties, risk-tiered approval, optimistic concurrency, ordered promotion), idempotent projector, optional impact gate | §2.2, Alg. 1–2 |
| `dqsim/workload.py` | Concurrent authors/approvers, command mix, injected policy violations and faulty changes | §2.4 |
| `dqsim/baseline.py` | B1: dbt project generation, timed builds, scheduled / on-demand deployment simulation | §2.4, §3.1 |
| `dqsim/b2_system.py`, `dqsim/b2_audit.py` | B2 and its reconstruction / tamper experiments | §3.1, §3.3 |
| `dqsim/b3_system.py`, `dqsim/b3_audit.py` | B3 (hash-chained audit + anchors) and its chain verifier / tamper experiments | §3.1, §3.3 |
| `dqsim/audit.py` | Point-in-time reconstruction against an oracle; tamper experiments on the event log | §3.3, Table 5 |
| `dqsim/invariants.py` | Independent verifier of constraints C1–C10 over raw logs, plus a mutation self-test | Alg. 3, §3.3, §3.5 |
| `dqsim/detect.py` | Detection quality against injected ground truth; dbt vs run-time binding control check | §3.7 |
| `dqsim/run_all.py` | Original experiments: latency, load sweep, contention, polling, dbt baseline | §3.1–3.2 |
| `dqsim/run_new.py` | Replication runs, B2, workload-mix sensitivity, verifier + mutation test | §3.1, §3.3, §3.6 |
| `dqsim/run_v3.py` | B3, impact gate with faulty changes, ablations, verification cost | §3.1, §3.3–3.5 |
| `dqsim/gate_diag.py` | Re-runs gate seeds with every gate decision logged | §3.4 |
| `dqsim/analysis.py`, `analysis_v2.py`, `analysis_v3.py` | Statistics (bootstrap CIs, Mann–Whitney U, Cliff's δ, per-run medians), lead-time model, figures, `numbers*.json` | §3, Tables 4–9, Figs. 3–4 |

## Reproducing the results

Requirements: Python 3.13, Java 17+ (for Synthea), about 6 GB RAM. The full pipeline takes several
hours on a 2-core machine.

```bash
pip install -r requirements.txt

# 1. Synthetic patients (~45 min on 2 cores)
curl -L -o synthea.jar https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar
java -Xmx5g -jar synthea.jar -p 10000 -s 42 -cs 42 --exporter.years_of_history=3 \
     --exporter.csv.export=true --exporter.fhir.export=false \
     --exporter.hospital.fhir.export=false --exporter.practitioner.fhir.export=false \
     --exporter.baseDirectory=./out Massachusetts

# 2. Rule library (240 rules, 6 templates), error injection, environment samples
python dqsim/prepare_data.py out/csv data

# 3. Original experiments (ES latency/load/contention/polling + dbt baseline B1)
python dqsim/run_all.py data results 10
python dqsim/detect.py data results
python dqsim/analysis.py results

# 4. Replication runs, baseline B2, workload mixes, independent verifier
python dqsim/run_new.py data results
python dqsim/analysis_v2.py data results results          # -> results/numbers.json

# 5. Baseline B3, impact gate, ablations, verification cost (~75 min)
python dqsim/run_v3.py data results
python dqsim/gate_diag.py data results 3
python dqsim/analysis_v3.py results results/numbers.json results   # -> results/numbers_v3.json, Figs. 3-4
```

Every number reported in the paper is in `results/numbers.json` and `results/numbers_v3.json`.
Thread scheduling makes individual latencies vary slightly between runs; medians and all
correctness results (reconstruction, tamper detection, policy enforcement, verifier outcomes)
should reproduce.

## Running on real Kafka / Snowflake

Replace `PartitionedLog.append/fetch` in `es_system.py` with a Kafka producer (key = rule id,
`acks=all`, idempotent producer) and one consumer per partition; replace the DuckDB connections in
`Projector` with Snowflake connections (projection tables `active_rules`, `events_applied`); and
point the dbt profile at Snowflake. The metric definitions in `workload.py` stay the same.

## License

MIT — see `LICENSE`.
