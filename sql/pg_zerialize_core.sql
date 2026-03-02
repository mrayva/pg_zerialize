SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

CREATE TYPE pgz_core AS (
    a int,
    b text,
    c bool
);

SELECT row_to_msgpack(ROW(1, 'x', true)::pgz_core) IS NOT NULL AS msgpack_ok;
SELECT row_to_msgpack_slow(ROW(1, 'x', true)::pgz_core) IS NOT NULL AS msgpack_slow_ok;
SELECT row_to_cbor(ROW(1, 'x', true)::pgz_core) IS NOT NULL AS cbor_ok;
SELECT row_to_zera(ROW(1, 'x', true)::pgz_core) IS NOT NULL AS zera_ok;
SELECT row_to_flexbuffers(ROW(1, 'x', true)::pgz_core) IS NOT NULL AS flex_ok;

SELECT rows_to_msgpack(ARRAY[
    ROW(1, 'x', true)::pgz_core,
    ROW(2, 'y', false)::pgz_core
]) IS NOT NULL AS rows_msgpack_ok;

SELECT rows_to_msgpack_slow(ARRAY[
    ROW(1, 'x', true)::pgz_core,
    ROW(2, 'y', false)::pgz_core
]) IS NOT NULL AS rows_msgpack_slow_ok;

SELECT rows_to_cbor(ARRAY[
    ROW(1, 'x', true)::pgz_core,
    ROW(2, 'y', false)::pgz_core
]) IS NOT NULL AS rows_cbor_ok;

SELECT rows_to_zera(ARRAY[
    ROW(1, 'x', true)::pgz_core,
    ROW(2, 'y', false)::pgz_core
]) IS NOT NULL AS rows_zera_ok;

SELECT rows_to_flexbuffers(ARRAY[
    ROW(1, 'x', true)::pgz_core,
    ROW(2, 'y', false)::pgz_core
]) IS NOT NULL AS rows_flex_ok;

SELECT rows_to_msgpack(ARRAY[1,2,3]);

DROP TYPE pgz_core;
DROP EXTENSION pg_zerialize;
