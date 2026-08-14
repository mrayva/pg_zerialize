SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;
BEGIN;

-- X_populate_recordset(base anyelement, data bytea) is the reverse of
-- rows_to_X: decode a binary array of documents into a set of typed
-- composites, mirroring jsonb_populate_recordset's anyelement/base-row
-- polymorphism (same base row supplies the per-document fallback).

CREATE TYPE pg_temp.prs_inner AS (a int, b text);
CREATE TYPE pg_temp.prs_row AS (id int, name text, tags text[], detail pg_temp.prs_inner);

CREATE TEMP TABLE prs_values AS
SELECT ARRAY[
    ROW(1, 'first', ARRAY['x', 'y'], ROW(1, 'one')::pg_temp.prs_inner)::pg_temp.prs_row,
    ROW(2, 'second', ARRAY['z'], ROW(2, 'two')::pg_temp.prs_inner)::pg_temp.prs_row,
    ROW(3, 'third', ARRAY[]::text[], ROW(3, 'three')::pg_temp.prs_inner)::pg_temp.prs_row
] AS rows;

-- Full batch round trip: rows -> rows_to_X -> X_populate_recordset must
-- reproduce the original set, in order, for all four protocols.
SELECT (SELECT array_agg(r) FROM msgpack_populate_recordset(NULL::pg_temp.prs_row, rows_to_msgpack(rows)) r) = rows
       AS msgpack_roundtrip,
       (SELECT array_agg(r) FROM cbor_populate_recordset(NULL::pg_temp.prs_row, rows_to_cbor(rows)) r) = rows
       AS cbor_roundtrip,
       (SELECT array_agg(r) FROM zera_populate_recordset(NULL::pg_temp.prs_row, rows_to_zera(rows)) r) = rows
       AS zera_roundtrip,
       (SELECT array_agg(r) FROM flexbuffers_populate_recordset(NULL::pg_temp.prs_row, rows_to_flexbuffers(rows)) r) = rows
       AS flex_roundtrip,
       (SELECT array_agg(r) FROM ion_populate_recordset(NULL::pg_temp.prs_row, rows_to_ion(rows)) r) = rows
       AS ion_roundtrip,
       (SELECT array_agg(r) FROM beve_populate_recordset(NULL::pg_temp.prs_row, rows_to_beve(rows)) r) = rows
       AS beve_roundtrip
FROM prs_values;

-- Row count and field access work through a normal FROM clause too.
SELECT count(*) = 3 AS three_rows, array_agg(id ORDER BY id) = ARRAY[1, 2, 3] AS ids_in_order
FROM prs_values, msgpack_populate_recordset(NULL::pg_temp.prs_row, rows_to_msgpack(rows));

-- Each document supplies fallback via the SAME base row (not per-document).
CREATE TYPE pg_temp.prs_partial AS (f1 int, f2 text, f3 boolean);
SELECT array_agg(r ORDER BY r.f1) = ARRAY[
           ROW(1, 'base', true)::pg_temp.prs_partial,
           ROW(2, 'base', false)::pg_temp.prs_partial
       ]
FROM msgpack_populate_recordset(
    ROW(0, 'base', true)::pg_temp.prs_partial,
    msgpack_from_jsonb(jsonb_build_array(
        jsonb_build_object('f1', 1),
        jsonb_build_object('f1', 2, 'f3', false)
    ))
) AS r;

-- data = NULL yields an empty set (there is no singular "unchanged row" for
-- a set-returning function, unlike X_populate_record).
SELECT count(*) = 0 AS null_data_is_empty
FROM msgpack_populate_recordset(NULL::pg_temp.prs_partial, NULL::bytea);

-- An empty document array yields an empty set (not an error).
SELECT count(*) = 0 AS empty_array_is_empty
FROM msgpack_populate_recordset(NULL::pg_temp.prs_partial, msgpack_from_jsonb('[]'::jsonb));

-- A non-array root document is rejected.
CREATE FUNCTION pg_temp.pgz_non_array_root_rejected()
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM r FROM msgpack_populate_recordset(
        NULL::pg_temp.prs_partial,
        msgpack_build_object('f1', 1)
    ) r;
    RETURN false;
EXCEPTION
    WHEN invalid_text_representation THEN RETURN true;
END
$$;
SELECT pg_temp.pgz_non_array_root_rejected() AS non_array_root_rejected;

-- A non-object array element is rejected.
CREATE FUNCTION pg_temp.pgz_non_object_element_rejected()
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM r FROM msgpack_populate_recordset(
        NULL::pg_temp.prs_partial,
        msgpack_from_jsonb(jsonb_build_array(jsonb_build_object('f1', 1), 42))
    ) r;
    RETURN false;
EXCEPTION
    WHEN invalid_text_representation THEN RETURN true;
END
$$;
SELECT pg_temp.pgz_non_object_element_rejected() AS non_object_element_rejected;

ROLLBACK;
DROP EXTENSION pg_zerialize;
