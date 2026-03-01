# Microbenchmark Harness

Repeatable in-repo benchmark for `pg_zerialize` serialization hot paths.

## Quick Start

```bash
make bench
```

Default connection/runtime settings:
- `PGHOST=127.0.0.1`
- `PGPORT=5432`
- `PGUSER=postgres`
- `PGPASSWORD=postgres`
- `PGDATABASE=postgres`
- `RUNS=5`
- `WARMUP=1`

You can override any of them:

```bash
RUNS=8 WARMUP=2 PGDATABASE=postgres make bench
```

## What It Measures

`bench/microbench.sql` prepares four datasets and reports averaged wall-clock timing per query:
- `narrow_*`: 3-column schema
- `wide_*`: mixed-type wide schema
- `arrays_msgpack`: array-heavy schema
- `numeric_msgpack`: numeric-heavy schema

Results are emitted between:
- `BENCH_RESULTS_BEGIN`
- `BENCH_RESULTS_END`

Format per line:

`label|avg_ms|min_ms|max_ms|runs|warmup|timestamp`

## Output Artifacts

- Timestamped run log: `results/microbench_YYYYmmdd_HHMMSS.out`
- Latest symlink: `results/microbench_latest.out`
