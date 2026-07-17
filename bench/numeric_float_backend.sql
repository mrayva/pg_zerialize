\set ON_ERROR_STOP on
\if :{?runs}
\else
\set runs 10
\endif
\if :{?warmup}
\else
\set warmup 3
\endif

SET client_min_messages TO warning;
SET jit = off;
SET max_parallel_workers_per_gather = 0;

CREATE EXTENSION IF NOT EXISTS pg_zerialize;

DROP TABLE IF EXISTS bench_numeric_float;
CREATE TABLE bench_numeric_float AS
SELECT
  i::int AS id,
  CASE WHEN i % 4 = 0 THEN i::numeric ELSE i::numeric / 3.0 END AS n0,
  CASE WHEN i % 6 = 0 THEN (i::bigint * 1000000)::numeric ELSE i::numeric / 7.0 END AS n1,
  i::numeric / 11.0 AS n2,
  (i::numeric / 13.0) + 0.123456 AS n3
FROM generate_series(1, 220000) gs(i);
ANALYZE bench_numeric_float;

-- Force module load before setting its custom GUC.
SELECT octet_length(row_to_msgpack(t.*)) > 0
FROM bench_numeric_float t
LIMIT 1;

CREATE TEMP TABLE bench_results(
  backend text PRIMARY KEY,
  avg_ms double precision,
  min_ms double precision,
  max_ms double precision
);

CREATE OR REPLACE FUNCTION pg_temp.run_numeric_bench(
  p_backend text, warmup_n int, run_n int)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  i int;
  t0 timestamptz;
  elapsed_ms double precision;
  result_size bigint;
  total_ms double precision := 0;
  min_ms double precision := 1e300;
  max_ms double precision := 0;
BEGIN
  PERFORM set_config('pg_zerialize.numeric_float_backend', p_backend, false);

  FOR i IN 1..warmup_n LOOP
    SELECT sum(octet_length(row_to_msgpack(t.*)))
      INTO result_size FROM bench_numeric_float t;
  END LOOP;

  FOR i IN 1..run_n LOOP
    t0 := clock_timestamp();
    SELECT sum(octet_length(row_to_msgpack(t.*)))
      INTO result_size FROM bench_numeric_float t;
    elapsed_ms := EXTRACT(epoch FROM (clock_timestamp() - t0)) * 1000.0;
    total_ms := total_ms + elapsed_ms;
    min_ms := LEAST(min_ms, elapsed_ms);
    max_ms := GREATEST(max_ms, elapsed_ms);
  END LOOP;

  INSERT INTO bench_results VALUES
    (p_backend, total_ms / run_n, min_ms, max_ms);
END;
$$;

SELECT pg_temp.run_numeric_bench('postgres', :warmup, :runs);
SELECT pg_temp.run_numeric_bench('fast_float', :warmup, :runs);

\pset format unaligned
\pset tuples_only on
SELECT backend || '|' ||
       round(avg_ms::numeric, 3)::text || '|' ||
       round(min_ms::numeric, 3)::text || '|' ||
       round(max_ms::numeric, 3)::text || '|' ||
       CASE WHEN backend = 'postgres' THEN '0.000'
            ELSE round((100.0 * (avg_ms - first_value(avg_ms) OVER (ORDER BY backend DESC)) /
                        first_value(avg_ms) OVER (ORDER BY backend DESC))::numeric, 3)::text
       END
FROM bench_results
ORDER BY backend DESC;
