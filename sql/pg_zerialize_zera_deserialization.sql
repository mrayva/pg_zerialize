SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;
BEGIN;

CREATE TYPE pg_temp.pgz_zera_leaf AS (
    label text,
    payload bytea
);

CREATE TYPE pg_temp.pgz_zera_holder AS (
    id bigint,
    active boolean,
    score double precision,
    special double precision,
    title text,
    matrix integer[],
    child pg_temp.pgz_zera_leaf,
    children pg_temp.pgz_zera_leaf[]
);

CREATE TEMP TABLE pgz_zera_values AS
SELECT ROW(
    9223372036854775807::bigint,
    true,
    1.25::double precision,
    'Infinity'::double precision,
    E'quoted " text \\ newline\n \u03a9 and a long arena string',
    ARRAY[[1, NULL], [3, 4]],
    ROW('root', decode('00ff10', 'hex'))::pg_temp.pgz_zera_leaf,
    ARRAY[
        ROW('first', decode('deadbeef', 'hex'))::pg_temp.pgz_zera_leaf,
        NULL::pg_temp.pgz_zera_leaf
    ]
)::pg_temp.pgz_zera_holder AS value;

SELECT zera_to_jsonb(row_to_zera(value)) =
       '{
          "id":9223372036854775807,
          "active":true,
          "score":1.25,
          "special":"Infinity",
          "title":"quoted \" text \\ newline\n \u03a9 and a long arena string",
          "matrix":[[1,null],[3,4]],
          "child":{"label":"root","payload":["~b","AP8Q","base64"]},
          "children":[
            {"label":"first","payload":["~b","3q2+7w==","base64"]},
            null
          ]
        }'::jsonb AS nested_row_semantics
FROM pgz_zera_values;

SELECT zera_to_jsonb(rows_to_zera(
           ARRAY[value, NULL::pg_temp.pgz_zera_holder]
       )) =
       jsonb_build_array(zera_to_jsonb(row_to_zera(value)), NULL)
       AS batch_semantics
FROM pgz_zera_values;

-- Fixtures emitted by the upstream ZERA writer, independent of this extension.
SELECT zera_to_jsonb(decode(
           '5a454e560100010000000000100000003000000008000000ffffffffffffffff00000000000000000000000000000000',
           'hex')) = '18446744073709551615'::jsonb AS unsigned_max,
       zera_to_jsonb(decode(
           '5a454e56010001000c0000001c000000300000000100000003000000000000000700020000000000030000000000000000ff10',
           'hex')) = '["~b","AP8Q","base64"]'::jsonb AS blob_value,
       zera_to_jsonb(decode(
           '5a454e56010001001400000024000000400000000100000002000000070000000000000000000000050000000000000000000000000000000000000000000000',
           'hex')) = '[7]'::jsonb AS array_value;

SELECT zera_to_jsonb(NULL::bytea) IS NULL AS strict_null;

CREATE FUNCTION pg_temp.pgz_zera_is_invalid(value bytea)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM zera_to_jsonb(value);
    RETURN false;
EXCEPTION
    WHEN invalid_binary_representation THEN RETURN true;
END
$$;

CREATE TEMP TABLE pgz_zera_fixtures AS
SELECT decode(
           '5a454e560100010000000000100000003000000008000000ffffffffffffffff00000000000000000000000000000000',
           'hex') AS scalar,
       decode(
           '5a454e56010001000c0000001c000000300000000100000003000000000000000700020000000000030000000000000000ff10',
           'hex') AS blob;

SELECT pg_temp.pgz_zera_is_invalid(''::bytea) AS empty_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(scalar, 0, 0)) AS magic_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(scalar, 4, 2)) AS version_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(scalar, 6, 0)) AS flags_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(scalar, 12, 15)) AS envelope_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(scalar, 8, 1)) AS root_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(scalar, 16, 49)) AS arena_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(scalar, 36, 1)) AS padding_rejected
FROM pgz_zera_fixtures;

SELECT pg_temp.pgz_zera_is_invalid(set_byte(scalar, 20, 255)) AS tag_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(scalar, 21, 2)) AS value_flags_rejected,
       pg_temp.pgz_zera_is_invalid(
           set_byte(set_byte(scalar, 20, 1), 22, 2)) AS boolean_rejected,
       pg_temp.pgz_zera_is_invalid(
           set_byte(set_byte(set_byte(scalar, 20, 4), 21, 1), 22, 13))
           AS inline_string_rejected,
       pg_temp.pgz_zera_is_invalid(
           set_byte(set_byte(scalar, 20, 4), 21, 0)) AS arena_string_rejected,
       pg_temp.pgz_zera_is_invalid(
           set_byte(set_byte(scalar, 20, 7), 22, 1)) AS typed_array_rejected
FROM pgz_zera_fixtures;

SELECT pg_temp.pgz_zera_is_invalid(set_byte(blob, 20, 2)) AS rank_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(blob, 24, 4)) AS shape_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(blob, 34, 1)) AS dtype_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(blob, 28, 255)) AS blob_span_rejected
FROM pgz_zera_fixtures;

SELECT pg_temp.pgz_zera_is_invalid(decode(
           '5a454e56010001002e0000003e0000006000000002000000010000006102000000010000000000000000000000010000006102000000020000000000000000000000060000000000000000000000000000000000000000000000000000000000',
           'hex')) AS duplicate_key_rejected,
       pg_temp.pgz_zera_is_invalid(set_byte(decode(
           '5a454e56010001002e0000003e0000006000000002000000010000006102000000010000000000000000000000010000006102000000020000000000000000000000060000000000000000000000000000000000000000000000000000000000',
           'hex'), 26, 1)) AS reserved_field_rejected,
       pg_temp.pgz_zera_is_invalid(decode(
           '5a454e56010001001400000024000000400000000100000005000000000000000000000000000000050000000000000000000000000000000000000000000000',
           'hex')) AS cycle_rejected;

ROLLBACK;
DROP EXTENSION pg_zerialize;
