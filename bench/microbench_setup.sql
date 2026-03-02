\set ON_ERROR_STOP on

SET client_min_messages TO warning;
SET jit = off;
SET max_parallel_workers_per_gather = 0;

CREATE EXTENSION IF NOT EXISTS pg_zerialize;

DROP TABLE IF EXISTS bench_narrow;
CREATE TABLE bench_narrow AS
SELECT
  i::int AS id,
  (i % 2 = 0) AS active,
  ('name_' || i::text)::text AS name
FROM generate_series(1, 250000) gs(i);
ANALYZE bench_narrow;

DROP TABLE IF EXISTS bench_wide;
CREATE TABLE bench_wide AS
SELECT
  i::int AS i0,
  (i + 1)::int AS i1,
  (i + 2)::int AS i2,
  (i + 3)::int AS i3,
  (i + 4)::int AS i4,
  (i + 5)::int AS i5,
  (i + 6)::int AS i6,
  (i + 7)::int AS i7,
  (i + 8)::int AS i8,
  (i + 9)::int AS i9,
  (i % 2 = 0) AS b0,
  (i % 3 = 0) AS b1,
  (i % 5 = 0) AS b2,
  (i % 7 = 0) AS b3,
  repeat('a', 8) || i::text AS t0,
  repeat('b', 8) || i::text AS t1,
  repeat('c', 8) || i::text AS t2,
  repeat('d', 8) || i::text AS t3,
  (i::numeric / 3.0)::numeric AS n0,
  (i::numeric / 7.0)::numeric AS n1,
  (DATE '2020-01-01' + (i % 3650))::date AS d0,
  (TIMESTAMP '2020-01-01 00:00:00' + ((i % 100000) * INTERVAL '1 second'))::timestamp AS ts0,
  jsonb_build_object('i', i, 'k', repeat('z', 8)) AS j0,
  decode(repeat(md5(i::text), 2), 'hex')::bytea AS ba0
FROM generate_series(1, 120000) gs(i);
ANALYZE bench_wide;

DROP TABLE IF EXISTS bench_arrays;
CREATE TABLE bench_arrays AS
SELECT
  i::int AS id,
  ARRAY[i, i + 1, i + 2, i + 3, i + 4, i + 5]::int[] AS a_i0,
  ARRAY[(i % 2)=0, (i % 3)=0, (i % 5)=0, (i % 7)=0, (i % 11)=0, (i % 13)=0]::bool[] AS a_b0,
  ARRAY[(i::float8)/3, (i::float8)/5, (i::float8)/7, (i::float8)/11]::float8[] AS a_f0,
  ARRAY[('s' || i::text), ('t' || i::text), ('u' || i::text)]::text[] AS a_t0
FROM generate_series(1, 180000) gs(i);
ANALYZE bench_arrays;

DROP TABLE IF EXISTS bench_numeric;
CREATE TABLE bench_numeric AS
SELECT
  i::int AS id,
  CASE WHEN i % 4 = 0 THEN (i::numeric) ELSE (i::numeric / 3.0) END AS n0,
  CASE WHEN i % 6 = 0 THEN ((i::bigint * 1000000)::numeric) ELSE (i::numeric / 7.0) END AS n1,
  (i::numeric / 11.0)::numeric AS n2,
  ((i::numeric / 13.0) + 0.123456)::numeric AS n3
FROM generate_series(1, 220000) gs(i);
ANALYZE bench_numeric;
