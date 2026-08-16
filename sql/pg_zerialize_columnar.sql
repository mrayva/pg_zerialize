SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

CREATE TYPE pgz_col_row AS (
    id int,
    name text,
    active bool,
    price double precision,
    tag text
);

CREATE TYPE pgz_col_nested AS (
    id int,
    detail pgz_col_row
);

-- Basic correctness, one composite row per format, including a NULL
-- field within a row and a wholly-NULL row.

SELECT msgpack_to_jsonb(rows_to_msgpack_columnar(ARRAY[
           ROW(1, 'a', true, 1.5, 'x')::pgz_col_row,
           ROW(2, 'b', false, 2.5, NULL)::pgz_col_row,
           NULL::pgz_col_row
       ])) = '{"id":[1,2,null],"name":["a","b",null],"active":[true,false,null],"price":[1.5,2.5,null],"tag":["x",null,null]}'::jsonb
       AS msgpack_columnar_ok;

SELECT cbor_to_jsonb(rows_to_cbor_columnar(ARRAY[
           ROW(1, 'a', true, 1.5, 'x')::pgz_col_row,
           ROW(2, 'b', false, 2.5, NULL)::pgz_col_row,
           NULL::pgz_col_row
       ])) = '{"id":[1,2,null],"name":["a","b",null],"active":[true,false,null],"price":[1.5,2.5,null],"tag":["x",null,null]}'::jsonb
       AS cbor_columnar_ok;

SELECT zera_to_jsonb(rows_to_zera_columnar(ARRAY[
           ROW(1, 'a', true, 1.5, 'x')::pgz_col_row,
           ROW(2, 'b', false, 2.5, NULL)::pgz_col_row,
           NULL::pgz_col_row
       ])) = '{"id":[1,2,null],"name":["a","b",null],"active":[true,false,null],"price":[1.5,2.5,null],"tag":["x",null,null]}'::jsonb
       AS zera_columnar_ok;

SELECT flexbuffers_to_jsonb(rows_to_flexbuffers_columnar(ARRAY[
           ROW(1, 'a', true, 1.5, 'x')::pgz_col_row,
           ROW(2, 'b', false, 2.5, NULL)::pgz_col_row,
           NULL::pgz_col_row
       ])) = '{"id":[1,2,null],"name":["a","b",null],"active":[true,false,null],"price":[1.5,2.5,null],"tag":["x",null,null]}'::jsonb
       AS flexbuffers_columnar_ok;

SELECT ion_to_jsonb(rows_to_ion_columnar(ARRAY[
           ROW(1, 'a', true, 1.5, 'x')::pgz_col_row,
           ROW(2, 'b', false, 2.5, NULL)::pgz_col_row,
           NULL::pgz_col_row
       ])) = '{"id":[1,2,null],"name":["a","b",null],"active":[true,false,null],"price":[1.5,2.5,null],"tag":["x",null,null]}'::jsonb
       AS ion_columnar_ok;

SELECT bson_to_jsonb(rows_to_bson_columnar(ARRAY[
           ROW(1, 'a', true, 1.5, 'x')::pgz_col_row,
           ROW(2, 'b', false, 2.5, NULL)::pgz_col_row,
           NULL::pgz_col_row
       ])) = '{"id":[1,2,null],"name":["a","b",null],"active":[true,false,null],"price":[1.5,2.5,null],"tag":["x",null,null]}'::jsonb
       AS bson_columnar_ok;

SELECT beve_to_jsonb(rows_to_beve_columnar(ARRAY[
           ROW(1, 'a', true, 1.5, 'x')::pgz_col_row,
           ROW(2, 'b', false, 2.5, NULL)::pgz_col_row,
           NULL::pgz_col_row
       ])) = '{"id":[1,2,null],"name":["a","b",null],"active":[true,false,null],"price":[1.5,2.5,null],"tag":["x",null,null]}'::jsonb
       AS beve_columnar_ok;

-- Structural comparison against the jsonb-based columnar construction
-- (jsonb_build_object/jsonb_agg), the approach nats_publish_from_sql.py's
-- --batch-size uses, to prove the native C++ path agrees with it.

SELECT msgpack_to_jsonb(rows_to_msgpack_columnar(ARRAY[
           ROW(1, 'a', true, 1.5, 'x')::pgz_col_row,
           ROW(2, 'b', false, 2.5, NULL)::pgz_col_row
       ])) = jsonb_build_object(
           'id', jsonb_build_array(1, 2),
           'name', jsonb_build_array('a', 'b'),
           'active', jsonb_build_array(true, false),
           'price', jsonb_build_array(1.5, 2.5),
           'tag', jsonb_build_array('x', NULL)
       ) AS msgpack_columnar_matches_jsonb_construction;

-- Empty array input -> empty object.

SELECT msgpack_to_jsonb(rows_to_msgpack_columnar(ARRAY[]::pgz_col_row[])) = '{}'::jsonb AS msgpack_columnar_empty;
SELECT bson_to_jsonb(rows_to_bson_columnar(ARRAY[]::pgz_col_row[])) = '{}'::jsonb AS bson_columnar_empty;

-- All-NULL-rows input: schema can't be determined -> error.

SELECT rows_to_msgpack_columnar(ARRAY[NULL::pgz_col_row, NULL::pgz_col_row]);

-- Nested composite column -> rejected (out of scope for the fast columnar path).

SELECT rows_to_msgpack_columnar(ARRAY[
    ROW(1, ROW(1,'a',true,1.5,'x')::pgz_col_row)::pgz_col_nested
]);

DROP TYPE pgz_col_nested;
DROP TYPE pgz_col_row;
DROP EXTENSION pg_zerialize;
