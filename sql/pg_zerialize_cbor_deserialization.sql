SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;
BEGIN;

CREATE TYPE pg_temp.pgz_cbor_leaf AS (
    label text,
    payload bytea
);

CREATE TYPE pg_temp.pgz_cbor_holder AS (
    id bigint,
    active boolean,
    score double precision,
    special double precision,
    title text,
    matrix integer[],
    child pg_temp.pgz_cbor_leaf,
    children pg_temp.pgz_cbor_leaf[]
);

CREATE TEMP TABLE pgz_cbor_values AS
SELECT ROW(
    9223372036854775807::bigint,
    true,
    1.25::double precision,
    'Infinity'::double precision,
    E'quoted " text \\ newline\n \u03a9',
    ARRAY[[1, NULL], [3, 4]],
    ROW('root', decode('00ff10', 'hex'))::pg_temp.pgz_cbor_leaf,
    ARRAY[
        ROW('first', decode('deadbeef', 'hex'))::pg_temp.pgz_cbor_leaf,
        NULL::pg_temp.pgz_cbor_leaf
    ]
)::pg_temp.pgz_cbor_holder AS value;

SELECT cbor_to_jsonb(row_to_cbor(value)) =
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
FROM pgz_cbor_values;

SELECT cbor_to_jsonb(rows_to_cbor(
           ARRAY[value, NULL::pg_temp.pgz_cbor_holder]
       )) =
       jsonb_build_array(cbor_to_jsonb(row_to_cbor(value)), NULL)
       AS batch_semantics
FROM pgz_cbor_values;

SELECT cbor_to_jsonb(decode('1bffffffffffffffff', 'hex')) =
           '18446744073709551615'::jsonb AS unsigned_max,
       cbor_to_jsonb(decode('3bffffffffffffffff', 'hex')) =
           '-18446744073709551616'::jsonb AS negative_max,
       cbor_to_jsonb(decode('f93e00', 'hex')) = '1.5'::jsonb AS float16_value,
       cbor_to_jsonb(decode('fa3fc00000', 'hex')) = '1.5'::jsonb AS float32_value,
       cbor_to_jsonb(decode('fb7ff0000000000000', 'hex')) =
           '"Infinity"'::jsonb AS infinity_value;

SELECT cbor_to_jsonb(decode('7f626865636c6c6fff', 'hex')) =
           '"hello"'::jsonb AS chunked_text,
       cbor_to_jsonb(decode('5f4200ff4110ff', 'hex')) =
           '["~b","AP8Q","base64"]'::jsonb AS chunked_bytes,
       cbor_to_jsonb(decode('9f01626869f5ff', 'hex')) =
           '[1,"hi",true]'::jsonb AS indefinite_array,
       cbor_to_jsonb(decode('bf616202616101ff', 'hex')) =
           '{"a":1,"b":2}'::jsonb AS indefinite_map;

SELECT cbor_to_jsonb(NULL::bytea) IS NULL AS strict_null;

CREATE FUNCTION pg_temp.pgz_cbor_is_invalid(value bytea)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM cbor_to_jsonb(value);
    RETURN false;
EXCEPTION
    WHEN invalid_binary_representation THEN RETURN true;
END
$$;

SELECT pg_temp.pgz_cbor_is_invalid(decode('', 'hex')) AS empty_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('1a0000', 'hex')) AS truncated_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('f6f6', 'hex')) AS trailing_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('c001', 'hex')) AS tag_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('a10102', 'hex')) AS nonstring_key_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('a2616101616102', 'hex')) AS duplicate_key_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('ff', 'hex')) AS break_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('f7', 'hex')) AS undefined_rejected;

SELECT pg_temp.pgz_cbor_is_invalid(decode('f800', 'hex')) AS simple_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('7f4100ff', 'hex')) AS wrong_chunk_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('7f7fff', 'hex')) AS nested_chunk_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('fc', 'hex')) AS reserved_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('1f', 'hex')) AS indefinite_integer_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('6100', 'hex')) AS nul_string_rejected,
       pg_temp.pgz_cbor_is_invalid(decode('61ff', 'hex')) AS encoding_rejected;

ROLLBACK;
DROP EXTENSION pg_zerialize;
