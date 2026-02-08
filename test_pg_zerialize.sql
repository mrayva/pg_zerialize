-- Comprehensive test suite for pg_zerialize.cpp
-- Tests all code paths: type conversion, formats, batch, caching, edge cases.
--
-- Run: sudo -u postgres psql -d postgres -f test_pg_zerialize.sql
--
-- On failure the script stops immediately (ON_ERROR_STOP).
-- Every query that returns data acts as an implicit "no crash" assertion.
-- Explicit assertions use DO blocks that RAISE EXCEPTION on mismatch.

\set ON_ERROR_STOP on
\timing off
\pset tuples_only on
\pset format unaligned

DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

-- ============================================================
-- Helper: assert that a bytea value has a specific length
-- ============================================================
CREATE OR REPLACE FUNCTION assert_size(actual int, expected int, label text) RETURNS void AS $$
BEGIN
    IF actual <> expected THEN
        RAISE EXCEPTION '% FAILED: expected % bytes, got %', label, expected, actual;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Helper: assert that a bytea value is non-null and non-empty
-- ============================================================
CREATE OR REPLACE FUNCTION assert_nonempty(val bytea, label text) RETURNS void AS $$
BEGIN
    IF val IS NULL THEN
        RAISE EXCEPTION '% FAILED: result is NULL', label;
    END IF;
    IF octet_length(val) = 0 THEN
        RAISE EXCEPTION '% FAILED: result is empty', label;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 1. SINGLE-RECORD FUNCTIONS: basic smoke test (all 4 formats)
-- ============================================================
DO $$
DECLARE
    r record;
    b bytea;
BEGIN
    -- row_to_msgpack
    SELECT row_to_msgpack(ROW(1, 'hello', true)) INTO b;
    PERFORM assert_nonempty(b, 'row_to_msgpack basic');

    -- row_to_cbor
    SELECT row_to_cbor(ROW(1, 'hello', true)) INTO b;
    PERFORM assert_nonempty(b, 'row_to_cbor basic');

    -- row_to_zera
    SELECT row_to_zera(ROW(1, 'hello', true)) INTO b;
    PERFORM assert_nonempty(b, 'row_to_zera basic');

    -- row_to_flexbuffers
    SELECT row_to_flexbuffers(ROW(1, 'hello', true)) INTO b;
    PERFORM assert_nonempty(b, 'row_to_flexbuffers basic');

    RAISE NOTICE 'PASS: 1. basic smoke test (all 4 formats)';
END $$;

-- ============================================================
-- 2. INTEGER TYPES: INT2, INT4, INT8
-- ============================================================
DO $$
DECLARE b bytea; s1 int; s2 int; s3 int;
BEGIN
    SELECT row_to_msgpack(ROW(42::smallint))   INTO b; s1 := octet_length(b);
    SELECT row_to_msgpack(ROW(42::int))        INTO b; s2 := octet_length(b);
    SELECT row_to_msgpack(ROW(42::bigint))     INTO b; s3 := octet_length(b);

    -- All should produce non-empty output
    IF s1 = 0 OR s2 = 0 OR s3 = 0 THEN
        RAISE EXCEPTION 'integer types FAILED: got zero-length output';
    END IF;

    -- INT4 and INT8 go through separate DatumGetInt32/64 paths
    -- but with value 42 the msgpack encoding may or may not differ
    RAISE NOTICE 'PASS: 2. integer types (INT2=% INT4=% INT8=% bytes)', s1, s2, s3;
END $$;

-- ============================================================
-- 3. FLOAT TYPES: FLOAT4, FLOAT8
-- ============================================================
DO $$
DECLARE b bytea; s1 int; s2 int;
BEGIN
    SELECT row_to_msgpack(ROW(3.14::float4)) INTO b; s1 := octet_length(b);
    SELECT row_to_msgpack(ROW(3.14::float8)) INTO b; s2 := octet_length(b);

    IF s1 = 0 OR s2 = 0 THEN
        RAISE EXCEPTION 'float types FAILED';
    END IF;
    RAISE NOTICE 'PASS: 3. float types (FLOAT4=% FLOAT8=% bytes)', s1, s2;
END $$;

-- ============================================================
-- 4. BOOLEAN TYPE
-- ============================================================
DO $$
DECLARE bt bytea; bf bytea;
BEGIN
    SELECT row_to_msgpack(ROW(true))  INTO bt;
    SELECT row_to_msgpack(ROW(false)) INTO bf;

    PERFORM assert_nonempty(bt, 'bool true');
    PERFORM assert_nonempty(bf, 'bool false');

    -- true and false should serialize to the same size
    PERFORM assert_size(octet_length(bt), octet_length(bf), 'bool size symmetry');

    RAISE NOTICE 'PASS: 4. boolean type (% bytes)', octet_length(bt);
END $$;

-- ============================================================
-- 5. TEXT TYPES: TEXT, VARCHAR, BPCHAR (CHAR)
-- ============================================================
DO $$
DECLARE s1 int; s2 int; s3 int; b bytea;
BEGIN
    SELECT row_to_msgpack(ROW('hello'::text))       INTO b; s1 := octet_length(b);
    SELECT row_to_msgpack(ROW('hello'::varchar))    INTO b; s2 := octet_length(b);
    SELECT row_to_msgpack(ROW('hello'::char(10)))   INTO b; s3 := octet_length(b);

    IF s1 = 0 OR s2 = 0 OR s3 = 0 THEN
        RAISE EXCEPTION 'text types FAILED';
    END IF;
    -- TEXT and VARCHAR with same content should match
    PERFORM assert_size(s1, s2, 'text vs varchar size');

    RAISE NOTICE 'PASS: 5. text types (TEXT=% VARCHAR=% CHAR(10)=% bytes)', s1, s2, s3;
END $$;

-- ============================================================
-- 6. NUMERIC TYPE (converted to double)
-- ============================================================
DO $$
DECLARE b bytea; s int;
BEGIN
    SELECT row_to_msgpack(ROW(123.456::numeric)) INTO b;
    PERFORM assert_nonempty(b, 'numeric basic');

    SELECT row_to_msgpack(ROW(99999999.99::numeric(12,2))) INTO b;
    PERFORM assert_nonempty(b, 'numeric large');

    SELECT row_to_msgpack(ROW(0::numeric)) INTO b;
    PERFORM assert_nonempty(b, 'numeric zero');

    RAISE NOTICE 'PASS: 6. NUMERIC type';
END $$;

-- ============================================================
-- 7. NULL VALUES
-- ============================================================
DO $$
DECLARE b bytea; s_null int; s_val int;
BEGIN
    -- Single NULL field
    SELECT row_to_msgpack(ROW(NULL::int)) INTO b;
    PERFORM assert_nonempty(b, 'null int');
    s_null := octet_length(b);

    SELECT row_to_msgpack(ROW(42::int)) INTO b;
    s_val := octet_length(b);

    -- NULL encoding should be smaller or equal to value encoding
    IF s_null > s_val THEN
        RAISE EXCEPTION 'null encoding (%) larger than value encoding (%)', s_null, s_val;
    END IF;

    -- Multiple NULLs
    SELECT row_to_msgpack(ROW(NULL::int, NULL::text, NULL::bool)) INTO b;
    PERFORM assert_nonempty(b, 'multiple nulls');

    -- Mix of NULL and non-NULL
    SELECT row_to_msgpack(ROW(1, NULL::text, true, NULL::float8)) INTO b;
    PERFORM assert_nonempty(b, 'mixed nulls');

    RAISE NOTICE 'PASS: 7. NULL values (null=% val=% bytes)', s_null, s_val;
END $$;

-- ============================================================
-- 8. ARRAY TYPES: int[], text[], bool[], float8[]
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT row_to_msgpack(ROW(ARRAY[1,2,3])) INTO b;
    PERFORM assert_nonempty(b, 'int array');

    SELECT row_to_msgpack(ROW(ARRAY['a','b','c'])) INTO b;
    PERFORM assert_nonempty(b, 'text array');

    SELECT row_to_msgpack(ROW(ARRAY[true,false,true])) INTO b;
    PERFORM assert_nonempty(b, 'bool array');

    SELECT row_to_msgpack(ROW(ARRAY[1.1, 2.2, 3.3]::float8[])) INTO b;
    PERFORM assert_nonempty(b, 'float8 array');

    RAISE NOTICE 'PASS: 8. array types (int[], text[], bool[], float8[])';
END $$;

-- ============================================================
-- 9. EMPTY ARRAYS
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT row_to_msgpack(ROW(ARRAY[]::int[])) INTO b;
    PERFORM assert_nonempty(b, 'empty int array');

    SELECT row_to_msgpack(ROW(ARRAY[]::text[])) INTO b;
    PERFORM assert_nonempty(b, 'empty text array');

    RAISE NOTICE 'PASS: 9. empty arrays';
END $$;

-- ============================================================
-- 10. NULL INSIDE ARRAYS
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT row_to_msgpack(ROW(ARRAY[1, NULL, 3]::int[])) INTO b;
    PERFORM assert_nonempty(b, 'int array with null');

    SELECT row_to_msgpack(ROW(ARRAY['a', NULL, 'c']::text[])) INTO b;
    PERFORM assert_nonempty(b, 'text array with null');

    RAISE NOTICE 'PASS: 10. NULL inside arrays';
END $$;

-- ============================================================
-- 11. FALLBACK TYPE (unsupported -> text representation)
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    -- DATE is not directly supported; falls through to text
    SELECT row_to_msgpack(ROW('2025-01-15'::date)) INTO b;
    PERFORM assert_nonempty(b, 'date fallback');

    -- TIMESTAMP also falls through
    SELECT row_to_msgpack(ROW('2025-01-15 10:30:00'::timestamp)) INTO b;
    PERFORM assert_nonempty(b, 'timestamp fallback');

    -- UUID
    SELECT row_to_msgpack(ROW('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid)) INTO b;
    PERFORM assert_nonempty(b, 'uuid fallback');

    -- JSONB (falls through to text)
    SELECT row_to_msgpack(ROW('{"key":"val"}'::jsonb)) INTO b;
    PERFORM assert_nonempty(b, 'jsonb fallback');

    RAISE NOTICE 'PASS: 11. fallback types (date, timestamp, uuid, jsonb)';
END $$;

-- ============================================================
-- 12. COMPOSITE / NAMED TYPE
-- ============================================================
CREATE TYPE test_person AS (name text, age int, active bool);

DO $$
DECLARE b bytea;
BEGIN
    SELECT row_to_msgpack(ROW('Alice', 30, true)::test_person) INTO b;
    PERFORM assert_nonempty(b, 'named composite');

    RAISE NOTICE 'PASS: 12. named composite type';
END $$;

-- ============================================================
-- 13. TABLE DATA (real HeapTuple path)
-- ============================================================
CREATE TEMP TABLE test_users (
    id serial PRIMARY KEY,
    name text NOT NULL,
    age int,
    score numeric(10,2),
    active bool DEFAULT true,
    tags text[]
);

INSERT INTO test_users (name, age, score, active, tags) VALUES
    ('Alice', 30, 95.50, true,  ARRAY['admin','user']),
    ('Bob',   25, 87.30, true,  ARRAY['user']),
    ('Carol', 35, 92.80, false, ARRAY['user','mod']),
    ('Dave',  28, NULL,  NULL,  NULL);

DO $$
DECLARE
    cnt int;
    total_bytes int;
BEGIN
    SELECT count(*), sum(octet_length(row_to_msgpack(test_users.*)))
      INTO cnt, total_bytes
      FROM test_users;

    IF cnt <> 4 THEN
        RAISE EXCEPTION 'table test FAILED: expected 4 rows, got %', cnt;
    END IF;
    IF total_bytes IS NULL OR total_bytes = 0 THEN
        RAISE EXCEPTION 'table test FAILED: zero total bytes';
    END IF;

    RAISE NOTICE 'PASS: 13. table data (% rows, % total bytes)', cnt, total_bytes;
END $$;

-- ============================================================
-- 14. DETERMINISTIC OUTPUT (same input -> same output)
-- ============================================================
DO $$
DECLARE b1 bytea; b2 bytea;
BEGIN
    SELECT row_to_msgpack(ROW(42, 'test', true)) INTO b1;
    SELECT row_to_msgpack(ROW(42, 'test', true)) INTO b2;

    IF b1 <> b2 THEN
        RAISE EXCEPTION 'deterministic output FAILED';
    END IF;

    RAISE NOTICE 'PASS: 14. deterministic output';
END $$;

-- ============================================================
-- 15. FORMAT SIZE ORDERING
--     CBOR <= MsgPack < FlexBuffers < ZERA (generally)
-- ============================================================
DO $$
DECLARE sm int; sc int; sf int; sz int;
BEGIN
    SELECT octet_length(row_to_msgpack(test_users.*))      INTO sm FROM test_users WHERE id = 1;
    SELECT octet_length(row_to_cbor(test_users.*))         INTO sc FROM test_users WHERE id = 1;
    SELECT octet_length(row_to_flexbuffers(test_users.*))  INTO sf FROM test_users WHERE id = 1;
    SELECT octet_length(row_to_zera(test_users.*))         INTO sz FROM test_users WHERE id = 1;

    -- FlexBuffers and ZERA should be strictly larger than MsgPack/CBOR
    IF sf <= sm THEN
        RAISE EXCEPTION 'format ordering FAILED: FlexBuffers (%) <= MsgPack (%)', sf, sm;
    END IF;
    IF sz <= sm THEN
        RAISE EXCEPTION 'format ordering FAILED: ZERA (%) <= MsgPack (%)', sz, sm;
    END IF;

    RAISE NOTICE 'PASS: 15. format size ordering (MP=% CBOR=% Flex=% ZERA=%)', sm, sc, sf, sz;
END $$;

-- ============================================================
-- 16. BATCH PROCESSING: basic smoke test (all 4 formats)
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT rows_to_msgpack(array_agg(test_users.*))      INTO b FROM test_users;
    PERFORM assert_nonempty(b, 'rows_to_msgpack');

    SELECT rows_to_cbor(array_agg(test_users.*))         INTO b FROM test_users;
    PERFORM assert_nonempty(b, 'rows_to_cbor');

    SELECT rows_to_zera(array_agg(test_users.*))         INTO b FROM test_users;
    PERFORM assert_nonempty(b, 'rows_to_zera');

    SELECT rows_to_flexbuffers(array_agg(test_users.*))  INTO b FROM test_users;
    PERFORM assert_nonempty(b, 'rows_to_flexbuffers');

    RAISE NOTICE 'PASS: 16. batch processing smoke test';
END $$;

-- ============================================================
-- 17. BATCH: empty array
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT rows_to_msgpack(ARRAY[]::test_users[]) INTO b;
    PERFORM assert_nonempty(b, 'batch empty');

    RAISE NOTICE 'PASS: 17. batch empty array';
END $$;

-- ============================================================
-- 18. BATCH: NULL records in array
-- ============================================================
DO $$
DECLARE b bytea; s int;
BEGIN
    SELECT rows_to_msgpack(ARRAY[
        (SELECT test_users FROM test_users WHERE id = 1),
        NULL::test_users,
        (SELECT test_users FROM test_users WHERE id = 2)
    ]) INTO b;
    PERFORM assert_nonempty(b, 'batch with null records');
    s := octet_length(b);

    RAISE NOTICE 'PASS: 18. batch with NULL records (% bytes)', s;
END $$;

-- ============================================================
-- 19. BATCH: single record array
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT rows_to_msgpack(ARRAY[
        (SELECT test_users FROM test_users WHERE id = 1)
    ]) INTO b;
    PERFORM assert_nonempty(b, 'batch single record');

    RAISE NOTICE 'PASS: 19. batch single record';
END $$;

-- ============================================================
-- 20. BATCH vs INDIVIDUAL SIZE CONSISTENCY
-- ============================================================
DO $$
DECLARE
    individual_sum int;
    batch_size int;
BEGIN
    SELECT sum(octet_length(row_to_msgpack(test_users.*)))
      INTO individual_sum FROM test_users;

    SELECT octet_length(rows_to_msgpack(array_agg(test_users.*)))
      INTO batch_size FROM test_users;

    -- Batch wraps records in an outer array, so it should be
    -- close to the sum of individual sizes (with small overhead)
    IF abs(batch_size - individual_sum) > individual_sum * 0.1 THEN
        RAISE EXCEPTION 'batch vs individual FAILED: individual=% batch=%', individual_sum, batch_size;
    END IF;

    RAISE NOTICE 'PASS: 20. batch vs individual size (indiv=% batch=%)', individual_sum, batch_size;
END $$;

-- ============================================================
-- 21. SCHEMA CACHING: repeated calls on same type
-- ============================================================
DO $$
DECLARE b1 bytea; b2 bytea; b3 bytea;
BEGIN
    -- First call: cache miss (lookup_rowtype_tupdesc + BlessTupleDesc + cache)
    SELECT row_to_msgpack(test_users.*) INTO b1 FROM test_users WHERE id = 1;

    -- Second call: cache hit
    SELECT row_to_msgpack(test_users.*) INTO b2 FROM test_users WHERE id = 1;

    -- Third call: cache hit
    SELECT row_to_msgpack(test_users.*) INTO b3 FROM test_users WHERE id = 1;

    -- All should produce identical output (same data, cached tupdesc)
    IF b1 <> b2 OR b2 <> b3 THEN
        RAISE EXCEPTION 'schema caching FAILED: outputs differ';
    END IF;

    RAISE NOTICE 'PASS: 21. schema caching (3 identical results)';
END $$;

-- ============================================================
-- 22. SCHEMA CACHING: different types don't collide
-- ============================================================
CREATE TYPE test_point AS (x float8, y float8);

DO $$
DECLARE bp bytea; bu bytea;
BEGIN
    SELECT row_to_msgpack(ROW(1.0, 2.0)::test_point) INTO bp;
    SELECT row_to_msgpack(test_users.*)               INTO bu FROM test_users WHERE id = 1;

    -- Sizes must differ (different schemas)
    IF octet_length(bp) = octet_length(bu) THEN
        -- Could be coincidence; at least verify content differs
        IF bp = bu THEN
            RAISE EXCEPTION 'schema caching collision FAILED: identical output for different types';
        END IF;
    END IF;

    RAISE NOTICE 'PASS: 22. schema caching (different types don''t collide)';
END $$;

-- ============================================================
-- 23. WIDE RECORD (20 columns) — exercises pre-allocation
-- ============================================================
DO $$
DECLARE b bytea; s int;
BEGIN
    SELECT row_to_msgpack(ROW(
        1,2,3,4,5,
        'a','b','c','d','e',
        1.1,2.2,3.3,4.4,5.5,
        true,false,true,false,true
    )) INTO b;
    s := octet_length(b);
    IF s = 0 THEN
        RAISE EXCEPTION 'wide record FAILED';
    END IF;

    RAISE NOTICE 'PASS: 23. wide record (20 columns, % bytes)', s;
END $$;

-- ============================================================
-- 24. SINGLE-COLUMN RECORD
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT row_to_msgpack(ROW(42)) INTO b;
    PERFORM assert_nonempty(b, 'single column');

    RAISE NOTICE 'PASS: 24. single-column record';
END $$;

-- ============================================================
-- 25. LARGE TEXT VALUES
-- ============================================================
DO $$
DECLARE b bytea; big text;
BEGIN
    big := repeat('x', 10000);
    SELECT row_to_msgpack(ROW(big)) INTO b;
    -- Must be at least 10000 bytes (text content + overhead)
    IF octet_length(b) < 10000 THEN
        RAISE EXCEPTION 'large text FAILED: only % bytes', octet_length(b);
    END IF;

    RAISE NOTICE 'PASS: 25. large text (10k chars -> % bytes)', octet_length(b);
END $$;

-- ============================================================
-- 26. BOUNDARY INTEGER VALUES
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    -- INT2 boundaries
    SELECT row_to_msgpack(ROW((-32768)::smallint)) INTO b;
    PERFORM assert_nonempty(b, 'INT2 min');
    SELECT row_to_msgpack(ROW(32767::smallint)) INTO b;
    PERFORM assert_nonempty(b, 'INT2 max');

    -- INT4 boundaries
    SELECT row_to_msgpack(ROW((-2147483648)::int)) INTO b;
    PERFORM assert_nonempty(b, 'INT4 min');
    SELECT row_to_msgpack(ROW(2147483647::int)) INTO b;
    PERFORM assert_nonempty(b, 'INT4 max');

    -- INT8 boundaries
    SELECT row_to_msgpack(ROW((-9223372036854775808)::bigint)) INTO b;
    PERFORM assert_nonempty(b, 'INT8 min');
    SELECT row_to_msgpack(ROW(9223372036854775807::bigint)) INTO b;
    PERFORM assert_nonempty(b, 'INT8 max');

    -- Zero
    SELECT row_to_msgpack(ROW(0::int)) INTO b;
    PERFORM assert_nonempty(b, 'int zero');

    RAISE NOTICE 'PASS: 26. boundary integer values';
END $$;

-- ============================================================
-- 27. SPECIAL FLOAT VALUES: NaN, Infinity, -Infinity, 0.0
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT row_to_msgpack(ROW('NaN'::float8))       INTO b;
    PERFORM assert_nonempty(b, 'float NaN');

    SELECT row_to_msgpack(ROW('Infinity'::float8))  INTO b;
    PERFORM assert_nonempty(b, 'float Inf');

    SELECT row_to_msgpack(ROW('-Infinity'::float8)) INTO b;
    PERFORM assert_nonempty(b, 'float -Inf');

    SELECT row_to_msgpack(ROW(0.0::float8))         INTO b;
    PERFORM assert_nonempty(b, 'float zero');

    RAISE NOTICE 'PASS: 27. special float values (NaN, Inf, -Inf, 0.0)';
END $$;

-- ============================================================
-- 28. EMPTY TEXT
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT row_to_msgpack(ROW(''::text)) INTO b;
    PERFORM assert_nonempty(b, 'empty text');

    RAISE NOTICE 'PASS: 28. empty text';
END $$;

-- ============================================================
-- 29. MANY-ROW BATCH (100 records)
-- ============================================================
CREATE TEMP TABLE test_bulk (id int, val text);
INSERT INTO test_bulk SELECT i, 'row' || i FROM generate_series(1, 100) i;

DO $$
DECLARE b bytea; s int;
BEGIN
    SELECT rows_to_msgpack(array_agg(test_bulk.*)) INTO b FROM test_bulk;
    s := octet_length(b);

    IF s < 500 THEN
        RAISE EXCEPTION 'bulk batch FAILED: only % bytes for 100 rows', s;
    END IF;

    RAISE NOTICE 'PASS: 29. 100-row batch (% bytes)', s;
END $$;

-- ============================================================
-- 30. ALL FORMATS ON BATCH PRODUCE DIFFERENT OUTPUT
-- ============================================================
DO $$
DECLARE bm bytea; bc bytea; bz bytea; bf bytea;
BEGIN
    SELECT rows_to_msgpack(array_agg(test_users.*))      INTO bm FROM test_users;
    SELECT rows_to_cbor(array_agg(test_users.*))         INTO bc FROM test_users;
    SELECT rows_to_zera(array_agg(test_users.*))         INTO bz FROM test_users;
    SELECT rows_to_flexbuffers(array_agg(test_users.*))  INTO bf FROM test_users;

    -- Each format should produce different binary output
    IF bm = bc OR bm = bz OR bm = bf THEN
        RAISE EXCEPTION 'batch format distinction FAILED: two formats produced identical output';
    END IF;

    RAISE NOTICE 'PASS: 30. all batch formats produce distinct output';
END $$;

-- ============================================================
-- 31. STRICT ATTRIBUTE: NULL input -> NULL output
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT row_to_msgpack(NULL::test_person) INTO b;
    IF b IS NOT NULL THEN
        RAISE EXCEPTION 'STRICT null FAILED: expected NULL, got non-null';
    END IF;

    RAISE NOTICE 'PASS: 31. STRICT attribute (NULL record -> NULL output)';
END $$;

-- ============================================================
-- 32. NESTED ARRAYS (array of arrays falls back to text)
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    -- 2D array should fall through to text representation
    SELECT row_to_msgpack(ROW(ARRAY[[1,2],[3,4]])) INTO b;
    PERFORM assert_nonempty(b, '2D array fallback');

    RAISE NOTICE 'PASS: 32. 2D array (text fallback)';
END $$;

-- ============================================================
-- 33. MIXED TYPE RECORD (all supported types in one row)
-- ============================================================
CREATE TEMP TABLE test_all_types (
    c_int2 smallint,
    c_int4 int,
    c_int8 bigint,
    c_float4 float4,
    c_float8 float8,
    c_numeric numeric(12,4),
    c_bool bool,
    c_text text,
    c_varchar varchar(50),
    c_inta int[],
    c_texta text[],
    c_boola bool[]
);

INSERT INTO test_all_types VALUES (
    1, 2, 3, 1.5, 2.5, 100.1234, true,
    'hello', 'world',
    ARRAY[10,20], ARRAY['a','b'], ARRAY[true,false]
);

DO $$
DECLARE bm int; bc int; bz int; bf int;
BEGIN
    SELECT octet_length(row_to_msgpack(test_all_types.*))     INTO bm FROM test_all_types;
    SELECT octet_length(row_to_cbor(test_all_types.*))        INTO bc FROM test_all_types;
    SELECT octet_length(row_to_zera(test_all_types.*))        INTO bz FROM test_all_types;
    SELECT octet_length(row_to_flexbuffers(test_all_types.*)) INTO bf FROM test_all_types;

    IF bm = 0 OR bc = 0 OR bz = 0 OR bf = 0 THEN
        RAISE EXCEPTION 'all-types FAILED: zero-length output';
    END IF;

    RAISE NOTICE 'PASS: 33. mixed type record (MP=% CBOR=% ZERA=% Flex=%)', bm, bc, bz, bf;
END $$;

-- ============================================================
-- 34. BATCH ON ALL-TYPES TABLE
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT rows_to_msgpack(array_agg(test_all_types.*)) INTO b FROM test_all_types;
    PERFORM assert_nonempty(b, 'batch all types');

    RAISE NOTICE 'PASS: 34. batch on mixed-type table';
END $$;

-- ============================================================
-- 35. UNICODE TEXT
-- ============================================================
DO $$
DECLARE b bytea;
BEGIN
    SELECT row_to_msgpack(ROW('こんにちは世界'::text)) INTO b;
    PERFORM assert_nonempty(b, 'unicode Japanese');

    SELECT row_to_msgpack(ROW('Привет мир'::text)) INTO b;
    PERFORM assert_nonempty(b, 'unicode Russian');

    SELECT row_to_msgpack(ROW('🚀🎉💡'::text)) INTO b;
    PERFORM assert_nonempty(b, 'unicode emoji');

    RAISE NOTICE 'PASS: 35. unicode text (Japanese, Russian, emoji)';
END $$;

-- ============================================================
-- Cleanup
-- ============================================================
DROP FUNCTION assert_size(int, int, text);
DROP FUNCTION assert_nonempty(bytea, text);
DROP TYPE test_person;
DROP TYPE test_point;
DROP TABLE test_users;
DROP TABLE test_bulk;
DROP TABLE test_all_types;

\echo ''
\echo '========================================'
\echo '  ALL 35 TESTS PASSED'
\echo '========================================'
\echo ''
