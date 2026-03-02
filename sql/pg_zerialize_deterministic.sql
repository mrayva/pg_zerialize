SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

CREATE TYPE pgz_det AS (
    id int,
    name text,
    active bool,
    n0 numeric,
    a0 int[]
);

SELECT row_to_msgpack(ROW(7, 'det', true, 42.5::numeric, ARRAY[1,2,3])::pgz_det)
       = row_to_msgpack(ROW(7, 'det', true, 42.5::numeric, ARRAY[1,2,3])::pgz_det) AS msgpack_stable;

SELECT row_to_cbor(ROW(7, 'det', true, 42.5::numeric, ARRAY[1,2,3])::pgz_det)
       = row_to_cbor(ROW(7, 'det', true, 42.5::numeric, ARRAY[1,2,3])::pgz_det) AS cbor_stable;

SELECT row_to_zera(ROW(7, 'det', true, 42.5::numeric, ARRAY[1,2,3])::pgz_det)
       = row_to_zera(ROW(7, 'det', true, 42.5::numeric, ARRAY[1,2,3])::pgz_det) AS zera_stable;

SELECT row_to_flexbuffers(ROW(7, 'det', true, 42.5::numeric, ARRAY[1,2,3])::pgz_det)
       = row_to_flexbuffers(ROW(7, 'det', true, 42.5::numeric, ARRAY[1,2,3])::pgz_det) AS flex_stable;

SELECT encode(row_to_msgpack(ROW(1, 'a', true, 1::numeric, ARRAY[1])::pgz_det), 'hex')
     = encode(row_to_msgpack_slow(ROW(1, 'a', true, 1::numeric, ARRAY[1])::pgz_det), 'hex') AS msgpack_hex_parity;

DROP TYPE pgz_det;
DROP EXTENSION pg_zerialize;
