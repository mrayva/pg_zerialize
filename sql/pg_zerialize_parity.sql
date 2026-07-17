SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

CREATE TYPE pgz_parity_narrow AS (
    id int,
    name text,
    active bool
);

CREATE TYPE pgz_parity_wide AS (
    i0 int,
    i1 int,
    i2 int,
    i3 int,
    t0 text,
    t1 text,
    b0 bool,
    n0 numeric,
    d0 date,
    ts0 timestamp,
    j0 jsonb,
    ba0 bytea,
    a0 int[]
);

CREATE TYPE pgz_parity_interval AS (
    value interval,
    values interval[]
);

CREATE TYPE pgz_parity_fixed_arrays AS (
    smallints smallint[],
    ints integer[],
    bigints bigint[],
    bools boolean[],
    reals real[],
    floats double precision[],
    uuids uuid[],
    names name[],
    chars "char"[],
    dates date[],
    timestamps timestamp[],
    intervals interval[]
);

SELECT row_to_msgpack(ROW(1, 'alice', true)::pgz_parity_narrow)
       = row_to_msgpack_slow(ROW(1, 'alice', true)::pgz_parity_narrow) AS narrow_equal;

SELECT row_to_msgpack(ROW(2, NULL::text, NULL::bool)::pgz_parity_narrow)
       = row_to_msgpack_slow(ROW(2, NULL::text, NULL::bool)::pgz_parity_narrow) AS narrow_null_equal;

SELECT row_to_msgpack(
           ROW(10, 11, 12, 13,
               't0', NULL::text,
               true,
               1234.567::numeric,
               DATE '2025-01-15',
               TIMESTAMP '2025-01-15 10:30:00',
               '{"k":1}'::jsonb,
               decode('DEADBEEF','hex')::bytea,
               ARRAY[1,2,NULL,4]::int[]
           )::pgz_parity_wide)
       = row_to_msgpack_slow(
           ROW(10, 11, 12, 13,
               't0', NULL::text,
               true,
               1234.567::numeric,
               DATE '2025-01-15',
               TIMESTAMP '2025-01-15 10:30:00',
               '{"k":1}'::jsonb,
               decode('DEADBEEF','hex')::bytea,
               ARRAY[1,2,NULL,4]::int[]
           )::pgz_parity_wide) AS wide_equal;

SELECT rows_to_msgpack(ARRAY[
           ROW(1, 'a', true)::pgz_parity_narrow,
           ROW(2, NULL::text, false)::pgz_parity_narrow,
           NULL::pgz_parity_narrow
       ])
       = rows_to_msgpack_slow(ARRAY[
           ROW(1, 'a', true)::pgz_parity_narrow,
           ROW(2, NULL::text, false)::pgz_parity_narrow,
           NULL::pgz_parity_narrow
       ]) AS batch_equal;

DO $$
DECLARE
    style text;
    value interval;
BEGIN
    FOREACH style IN ARRAY ARRAY['postgres', 'postgres_verbose', 'sql_standard', 'iso_8601'] LOOP
        PERFORM set_config('intervalstyle', style, false);
        FOREACH value IN ARRAY ARRAY[
            '0'::interval,
            '1 mon -2 days 03:04:05.000006'::interval,
            'infinity'::interval,
            '-infinity'::interval
        ] LOOP
            IF row_to_msgpack(ROW(value, ARRAY[value, NULL, value])::pgz_parity_interval)
               <> row_to_msgpack_slow(ROW(value, ARRAY[value, NULL, value])::pgz_parity_interval) THEN
                RAISE EXCEPTION 'interval fast/slow mismatch for style % and value %', style, value;
            END IF;
        END LOOP;
    END LOOP;

    IF row_to_msgpack(ROW(
           ARRAY['-32768'::smallint, 0, 32767],
           ARRAY[-2147483647, 0, 2147483647],
           ARRAY['-9223372036854775808'::bigint, 0, '9223372036854775807'::bigint],
           ARRAY[false, true], ARRAY['-Infinity'::real, 0.0, 'Infinity'::real],
           ARRAY['-Infinity'::float8, 0.0, 'Infinity'::float8],
           ARRAY['00000000-0000-0000-0000-000000000000'::uuid,
                 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid],
           ARRAY['first'::name, 'second'::name], ARRAY['A'::"char", 'Z'::"char"],
           ARRAY['2000-01-01'::date, '9999-12-31'::date],
           ARRAY['2000-01-01'::timestamp, '2099-12-31 23:59:59.999999'::timestamp],
           ARRAY['-infinity'::interval, '0'::interval, 'infinity'::interval]
       )::pgz_parity_fixed_arrays)
       <> row_to_msgpack_slow(ROW(
           ARRAY['-32768'::smallint, 0, 32767],
           ARRAY[-2147483647, 0, 2147483647],
           ARRAY['-9223372036854775808'::bigint, 0, '9223372036854775807'::bigint],
           ARRAY[false, true], ARRAY['-Infinity'::real, 0.0, 'Infinity'::real],
           ARRAY['-Infinity'::float8, 0.0, 'Infinity'::float8],
           ARRAY['00000000-0000-0000-0000-000000000000'::uuid,
                 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid],
           ARRAY['first'::name, 'second'::name], ARRAY['A'::"char", 'Z'::"char"],
           ARRAY['2000-01-01'::date, '9999-12-31'::date],
           ARRAY['2000-01-01'::timestamp, '2099-12-31 23:59:59.999999'::timestamp],
           ARRAY['-infinity'::interval, '0'::interval, 'infinity'::interval]
       )::pgz_parity_fixed_arrays) THEN
        RAISE EXCEPTION 'fixed-width array fast/slow mismatch';
    END IF;
END
$$;

RESET intervalstyle;

DROP TYPE pgz_parity_interval;
DROP TYPE pgz_parity_fixed_arrays;
DROP TYPE pgz_parity_wide;
DROP TYPE pgz_parity_narrow;
DROP EXTENSION pg_zerialize;
