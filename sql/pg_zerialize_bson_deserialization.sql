SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;
BEGIN;

-- Mirrors pg_zerialize_cbor_deserialization.sql for BSON, minus anything
-- that would need a bare-array-root document (no rows_to_bson exists -- see
-- pg_zerialize.cpp's forward-declaration comment and BSON.md). BSON also
-- goes through zerialize's Deserializer/Reader interface (reader_value_to_json
-- in pg_zerialize.cpp) rather than a hand-rolled wire-byte parser.

CREATE TYPE pg_temp.pgz_bson_leaf AS (
    label text,
    payload bytea
);

CREATE TYPE pg_temp.pgz_bson_holder AS (
    id bigint,
    active boolean,
    score double precision,
    special double precision,
    title text,
    matrix integer[],
    child pg_temp.pgz_bson_leaf,
    children pg_temp.pgz_bson_leaf[]
);

CREATE TEMP TABLE pgz_bson_values AS
SELECT ROW(
    9223372036854775807::bigint,
    true,
    1.25::double precision,
    'Infinity'::double precision,
    E'quoted " text \\ newline\n Ω',
    ARRAY[[1, NULL], [3, 4]],
    ROW('root', decode('00ff10', 'hex'))::pg_temp.pgz_bson_leaf,
    ARRAY[
        ROW('first', decode('deadbeef', 'hex'))::pg_temp.pgz_bson_leaf,
        NULL::pg_temp.pgz_bson_leaf
    ]
)::pg_temp.pgz_bson_holder AS value;

-- Nested arrays here are all below the root (inside the document, or inside
-- a nested document within it), so they keep a real BSON type tag from
-- their parent element and round-trip correctly -- see the dedicated
-- bare-root-array test further down for the one case that doesn't.
SELECT bson_to_jsonb(row_to_bson(value)) =
       '{
          "id":9223372036854775807,
          "active":true,
          "score":1.25,
          "special":"Infinity",
          "title":"quoted \" text \\ newline\n Ω",
          "matrix":[[1,null],[3,4]],
          "child":{"label":"root","payload":["~b","AP8Q","base64"]},
          "children":[
            {"label":"first","payload":["~b","3q2+7w==","base64"]},
            null
          ]
        }'::jsonb AS nested_row_semantics
FROM pgz_bson_values;

SELECT bson_to_jsonb(NULL::bytea) IS NULL AS strict_null;

-- The one case BSON's wire format cannot support: a bare array at the
-- document root is indistinguishable from a document (no parent element
-- header exists at the root to carry a type tag), so it decodes as an
-- object with stringified numeric keys instead of an array. This is
-- expected, documented behavior (see BSON.md and README's Current
-- Limitations), not a bug -- confirming it here pins the contract down.
SELECT bson_to_jsonb(bson_from_jsonb(jsonb_build_array(10, 20, 30))) =
       '{"0":10,"1":20,"2":30}'::jsonb AS bare_array_root_decodes_as_object;

-- Arrays nested anywhere below the root, by contrast, round-trip normally
-- (already exercised above via "matrix" and "children"; here via
-- bson_from_jsonb, which recurses through nested jsonb -- bson_build_object
-- treats a raw jsonb-typed variadic argument as opaque, same as CBOR).
SELECT bson_to_jsonb(bson_from_jsonb(
           jsonb_build_object('arr', jsonb_build_array(1,2,3))
       )) = jsonb_build_object('arr', jsonb_build_array(1,2,3)) AS nested_array_roundtrips;

CREATE FUNCTION pg_temp.pgz_bson_is_invalid(value bytea)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM bson_to_jsonb(value);
    RETURN false;
EXCEPTION
    WHEN invalid_binary_representation THEN RETURN true;
END
$$;

SELECT encode(bson_from_jsonb('{"a":1}'::jsonb), 'hex') AS bson_a1_hex \gset

SELECT pg_temp.pgz_bson_is_invalid(decode('', 'hex')) AS empty_rejected,
       pg_temp.pgz_bson_is_invalid(decode('0102', 'hex')) AS truncated_len_rejected,
       pg_temp.pgz_bson_is_invalid(
           decode(substring(:'bson_a1_hex' from 1 for length(:'bson_a1_hex') - 2), 'hex')
       ) AS truncated_body_rejected,
       pg_temp.pgz_bson_is_invalid(decode(:'bson_a1_hex' || 'ff', 'hex')) AS trailing_byte_rejected;

ROLLBACK;
DROP EXTENSION pg_zerialize;
