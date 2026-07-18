SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;
BEGIN;

CREATE TYPE pg_temp.pgz_flex_leaf AS (
    label text,
    payload bytea
);

CREATE TYPE pg_temp.pgz_flex_holder AS (
    id bigint,
    active boolean,
    score double precision,
    special double precision,
    title text,
    matrix integer[],
    child pg_temp.pgz_flex_leaf,
    children pg_temp.pgz_flex_leaf[]
);

CREATE TEMP TABLE pgz_flex_values AS
SELECT ROW(
    9223372036854775807::bigint,
    true,
    1.25::double precision,
    'Infinity'::double precision,
    E'quoted " text \\ newline\n \u03a9',
    ARRAY[[1, NULL], [3, 4]],
    ROW('root', decode('00ff10', 'hex'))::pg_temp.pgz_flex_leaf,
    ARRAY[
        ROW('first', decode('deadbeef', 'hex'))::pg_temp.pgz_flex_leaf,
        NULL::pg_temp.pgz_flex_leaf
    ]
)::pg_temp.pgz_flex_holder AS value;

SELECT flexbuffers_to_jsonb(row_to_flexbuffers(value)) =
       '{
          "id":9223372036854775807,
          "active":true,
          "score":1.25,
          "special":"Infinity",
          "title":"quoted \" text \\ newline\n \u03a9",
          "matrix":[[1,null],[3,4]],
          "child":{"label":"root","payload":["~b","AP8Q","base64"]},
          "children":[
            {"label":"first","payload":["~b","3q2+7w==","base64"]},
            null
          ]
        }'::jsonb AS nested_row_semantics
FROM pgz_flex_values;

SELECT flexbuffers_to_jsonb(rows_to_flexbuffers(
           ARRAY[value, NULL::pg_temp.pgz_flex_holder]
       )) =
       jsonb_build_array(flexbuffers_to_jsonb(row_to_flexbuffers(value)), NULL)
       AS batch_semantics
FROM pgz_flex_values;

SELECT flexbuffers_to_jsonb(NULL::bytea) IS NULL AS strict_null;

-- Fixtures emitted by the upstream FlatBuffers C++ builder exercise valid
-- root and typed-vector forms that pg_zerialize's generic writer does not emit.
SELECT flexbuffers_to_jsonb(decode('03fd0007032c01', 'hex')) =
           '[-3,0,7]'::jsonb AS typed_int_vector,
       flexbuffers_to_jsonb(decode('0000c03f000010c0000040400c5601', 'hex')) =
           '[1.5,-2.25,3]'::jsonb AS fixed_float_vector,
       flexbuffers_to_jsonb(decode('ffffffffffffffff0b08', 'hex')) =
           '18446744073709551615'::jsonb AS unsigned_root;

CREATE FUNCTION pg_temp.pgz_flex_is_invalid(value bytea)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM flexbuffers_to_jsonb(value);
    RETURN false;
EXCEPTION
    WHEN invalid_binary_representation THEN RETURN true;
END
$$;

WITH encoded AS (
    SELECT row_to_flexbuffers(value) AS payload
    FROM pgz_flex_values
)
SELECT pg_temp.pgz_flex_is_invalid(''::bytea) AS empty_rejected,
       pg_temp.pgz_flex_is_invalid(
           substring(payload FOR octet_length(payload) - 1)
       ) AS truncated_rejected,
       pg_temp.pgz_flex_is_invalid(
           set_byte(payload, octet_length(payload) - 1, 3)
       ) AS invalid_width_rejected,
       pg_temp.pgz_flex_is_invalid(
           set_byte(payload, octet_length(payload) - 2, 255)
       ) AS invalid_type_rejected
FROM encoded;

ROLLBACK;
DROP EXTENSION pg_zerialize;
