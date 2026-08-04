SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;
BEGIN;

CREATE TYPE pg_temp.pgz_array_item AS (
    id integer,
    label text
);

CREATE TYPE pg_temp.pgz_array_holder AS (
    int_grid integer[],
    text_cube text[],
    item_grid pg_temp.pgz_array_item[],
    lower_bound_grid integer[]
);

CREATE TEMP TABLE pgz_array_values AS
SELECT ROW(
    ARRAY[[1, NULL], [3, 4]],
    ARRAY[[['a', 'b'], ['c', NULL]], [['d', 'e'], ['f', 'g']]],
    ARRAY[
        [ROW(1, 'one')::pg_temp.pgz_array_item, NULL::pg_temp.pgz_array_item],
        [ROW(2, 'two')::pg_temp.pgz_array_item,
         ROW(3, NULL)::pg_temp.pgz_array_item]
    ],
    '[0:1][5:6]={{10,20},{30,40}}'::integer[]
)::pg_temp.pgz_array_holder AS value;

SELECT row_to_msgpack(value) = row_to_msgpack_slow(value) AS msgpack_parity
FROM pgz_array_values;

SELECT octet_length(row_to_msgpack(value)) > 0 AS msgpack_nested_arrays,
       octet_length(row_to_flexbuffers(value)) > 0 AS flex_nested_arrays,
       octet_length(row_to_cbor(value)) > 0 AS cbor_nested_arrays,
       octet_length(row_to_zera(value)) > 0 AS zera_nested_arrays
FROM pgz_array_values;

-- CBOR/ZERA/Flex now also have a direct multidimensional-array fast writer
-- (previously only MessagePack's fast path handled ndim > 1; the others fell
-- back to the generic dynamic tree for any multidimensional array column).
-- There's no _slow() helper for these three, so cross-check their fast-path
-- output against MessagePack's, decoded through each protocol's own JSONB
-- decoder. Covers a 2-D int array with nulls, a 3-D text cube with nulls, a
-- 2-D array of composites with nulls (multidim + recursive-composite
-- together), and a non-default-lower-bound 2-D array.
SELECT (cbor_to_jsonb(row_to_cbor(value)) = msgpack_to_jsonb(row_to_msgpack(value))) AS cbor_matches_msgpack,
       (zera_to_jsonb(row_to_zera(value)) = msgpack_to_jsonb(row_to_msgpack(value))) AS zera_matches_msgpack,
       (flexbuffers_to_jsonb(row_to_flexbuffers(value)) = msgpack_to_jsonb(row_to_msgpack(value))) AS flex_matches_msgpack
FROM pgz_array_values;

-- The batch API's outer array remains a one-dimensional row collection.
DO $$
BEGIN
    PERFORM rows_to_msgpack(ARRAY[
        [ROW(1, ARRAY[[1, 2], [3, 4]])::record],
        [ROW(2, ARRAY[[5, 6], [7, 8]])::record]
    ]);
    RAISE EXCEPTION 'multidimensional batch input was accepted';
EXCEPTION
    WHEN feature_not_supported THEN NULL;
END
$$;

ROLLBACK;
DROP EXTENSION pg_zerialize;
