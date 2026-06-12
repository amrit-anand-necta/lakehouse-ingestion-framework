# Lakehouse Ingestion Framework

A **config-driven, multi-source ingestion framework** for Delta Lake, built with PySpark.
Adding a new source table requires **one config entry — zero code changes**.

## Why

In most ETL codebases, every new source table means a new pipeline script. At 50+ tables
that becomes unmaintainable: bug fixes need 50 PRs, logic drifts between copies, and
nobody knows which pipeline failed last night without digging through logs.

This framework solves that with three ideas:

1. **Metadata-driven** — every source table is a row in a config table (source type,
   path/credentials, load strategy, watermark). One engine reads the config and does the work.
2. **Dispatcher pattern** — readers are pluggable. `read_source()` routes to the right
   reader by `source_type`. A new source type = one new reader module + one dispatch case.
3. **Observability first** — every run writes start/end records to an audit table
   (row counts, rejects, errors, duration) and emits structured logs at every checkpoint.

## Architecture

```
                       ┌──────────────────┐
  config table ──────► │   orchestrator    │  loops active configs
  (Delta)              └────────┬─────────┘
                                │ per config_id
                       ┌────────▼─────────┐
                       │     pipeline      │  read → validate → write → audit
                       └────────┬─────────┘
              ┌─────────────────┼──────────────────┐
      ┌───────▼──────┐  ┌───────▼───────┐  ┌───────▼──────┐
      │   readers     │  │  validation   │  │   writers     │
      │ csv/json/     │  │ null-PK,      │  │ append /      │
      │ parquet/jdbc/ │  │ dedupe,       │  │ overwrite /   │
      │ rest API      │  │ schema drift  │  │ Delta MERGE   │
      └──────────────┘  └───────────────┘  └───────┬──────┘
                                                    │
                              audit table ◄─────────┴────► bronze Delta tables
```

## Load strategies

| `load_type`   | Mechanism                                   | Use case                          |
|---------------|---------------------------------------------|-----------------------------------|
| `full`        | overwrite + overwriteSchema                  | small dimension/reference tables  |
| `append`      | append                                       | immutable event data              |
| `incremental` | Delta MERGE on primary key + watermark       | transactional data with updates   |

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/bootstrap.py        # creates config + audit tables, sample data
python main.py                     # runs all active pipelines
```

## Project layout

```
src/ingestion_framework/
├── session.py        # SparkSession factory (Delta-enabled)
├── logging_setup.py  # structured logging
├── config.py         # config table access
├── readers/          # one module per source type + dispatcher
├── validation.py     # quality gates (soft-fail philosophy)
├── writers/          # append / overwrite / MERGE
├── audit.py          # audit table writer
├── pipeline.py       # per-table ingestion lifecycle
└── orchestrator.py   # fan-out over active configs
```

## Status

- [x] Project scaffold
- [ ] Spark session factory + logging
- [ ] Config table + bootstrap
- [ ] File readers (CSV / JSON / Parquet)
- [ ] JDBC reader (filter pushdown, parallel reads)
- [ ] REST API reader
- [ ] Validation gates
- [ ] Delta writers (full / append / MERGE)
- [ ] Audit framework
- [ ] Pipeline + orchestrator
- [ ] Tests
