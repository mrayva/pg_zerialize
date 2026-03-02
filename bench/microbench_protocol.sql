\set ON_ERROR_STOP on
\if :{?runs}
\else
\set runs 5
\endif
\if :{?warmup}
\else
\set warmup 1
\endif

SET client_min_messages TO warning;
SET jit = off;
SET max_parallel_workers_per_gather = 0;

CREATE TEMP TABLE bench_results(
  label text PRIMARY KEY,
  avg_ms double precision,
  min_ms double precision,
  max_ms double precision,
  runs int,
  warmup int,
  measured_at timestamptz
);

CREATE OR REPLACE FUNCTION pg_temp.run_bench(p_label text, sql_text text, warmup_n int, run_n int)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  i int;
  t0 timestamptz;
  elapsed_ms double precision;
  v_sum bigint;
  total_ms double precision := 0;
  min_ms double precision := 1e300;
  max_ms double precision := 0;
BEGIN
  FOR i IN 1..warmup_n LOOP
    EXECUTE sql_text INTO v_sum;
  END LOOP;

  FOR i IN 1..run_n LOOP
    t0 := clock_timestamp();
    EXECUTE sql_text INTO v_sum;
    elapsed_ms := EXTRACT(epoch FROM (clock_timestamp() - t0)) * 1000.0;
    total_ms := total_ms + elapsed_ms;
    IF elapsed_ms < min_ms THEN min_ms := elapsed_ms; END IF;
    IF elapsed_ms > max_ms THEN max_ms := elapsed_ms; END IF;
  END LOOP;

  INSERT INTO bench_results(label, avg_ms, min_ms, max_ms, runs, warmup, measured_at)
  VALUES (p_label, total_ms / run_n, min_ms, max_ms, run_n, warmup_n, clock_timestamp())
  ON CONFLICT (label) DO UPDATE
  SET avg_ms = EXCLUDED.avg_ms,
      min_ms = EXCLUDED.min_ms,
      max_ms = EXCLUDED.max_ms,
      runs = EXCLUDED.runs,
      warmup = EXCLUDED.warmup,
      measured_at = EXCLUDED.measured_at;
END;
$$;

\if :{?run_msgpack}
SELECT pg_temp.run_bench('narrow_msgpack', 'SELECT sum(octet_length(row_to_msgpack(t.*))) FROM bench_narrow t', :warmup, :runs);
SELECT pg_temp.run_bench('wide_msgpack', 'SELECT sum(octet_length(row_to_msgpack(t.*))) FROM bench_wide t', :warmup, :runs);
SELECT pg_temp.run_bench('arrays_msgpack', 'SELECT sum(octet_length(row_to_msgpack(t.*))) FROM bench_arrays t', :warmup, :runs);
SELECT pg_temp.run_bench('numeric_msgpack', 'SELECT sum(octet_length(row_to_msgpack(t.*))) FROM bench_numeric t', :warmup, :runs);
\endif

\if :{?run_cbor}
SELECT pg_temp.run_bench('narrow_cbor', 'SELECT sum(octet_length(row_to_cbor(t.*))) FROM bench_narrow t', :warmup, :runs);
SELECT pg_temp.run_bench('wide_cbor', 'SELECT sum(octet_length(row_to_cbor(t.*))) FROM bench_wide t', :warmup, :runs);
\endif

\if :{?run_zera}
SELECT pg_temp.run_bench('narrow_zera', 'SELECT sum(octet_length(row_to_zera(t.*))) FROM bench_narrow t', :warmup, :runs);
SELECT pg_temp.run_bench('wide_zera', 'SELECT sum(octet_length(row_to_zera(t.*))) FROM bench_wide t', :warmup, :runs);
\endif

\if :{?run_flex}
SELECT pg_temp.run_bench('narrow_flex', 'SELECT sum(octet_length(row_to_flexbuffers(t.*))) FROM bench_narrow t', :warmup, :runs);
SELECT pg_temp.run_bench('wide_flex', 'SELECT sum(octet_length(row_to_flexbuffers(t.*))) FROM bench_wide t', :warmup, :runs);
\endif

\pset format unaligned
\pset tuples_only on
SELECT label || '|' ||
       round(avg_ms::numeric, 3)::text || '|' ||
       round(min_ms::numeric, 3)::text || '|' ||
       round(max_ms::numeric, 3)::text || '|' ||
       runs::text || '|' ||
       warmup::text || '|' ||
       to_char(measured_at, 'YYYY-MM-DD"T"HH24:MI:SSOF')
FROM bench_results
ORDER BY label;
