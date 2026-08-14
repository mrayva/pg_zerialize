SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;
BEGIN;

-- X_populate_record(base anyelement, data bytea) is the reverse of
-- row_to_X: decode a binary document straight into a typed composite,
-- mirroring jsonb_populate_record's anyelement/base-row polymorphism.

CREATE TYPE pg_temp.pr_inner AS (a int, b text);

CREATE TYPE pg_temp.pr_wide AS (
    a_int2 smallint,
    a_int4 integer,
    a_int8 bigint,
    a_float4 real,
    a_float8 double precision,
    a_bool boolean,
    a_text text,
    a_json json,
    a_uuid uuid,
    a_name name,
    a_char "char",
    a_inet inet,
    a_cidr cidr,
    a_interval interval,
    a_numeric numeric,
    a_date date,
    a_timestamp timestamp,
    a_timestamptz timestamptz,
    a_jsonb jsonb,
    a_bytea bytea,
    a_composite pg_temp.pr_inner,
    a_array int[],
    a_composite_array pg_temp.pr_inner[]
);

CREATE TEMP TABLE pr_values AS
SELECT ROW(
    (-32768)::smallint,
    (-2147483648)::integer,
    '-9223372036854775808'::bigint,
    'Infinity'::real,
    'Infinity'::float8,
    true,
    'hello world',
    '{"a":1}'::json,
    'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid,
    'a_name'::name,
    'A'::"char",
    '192.168.1.1'::inet,
    '10.0.0.0/8'::cidr,
    '1 mon -2 days 03:04:05.000006'::interval,
    1234.5678::numeric,
    '2000-01-01'::date,
    '2000-01-01 00:00:00'::timestamp,
    '2000-01-01 00:00:00+00'::timestamptz,
    '{"k":1}'::jsonb,
    decode('DEADBEEF', 'hex'),
    ROW(1, 'x')::pg_temp.pr_inner,
    ARRAY[1, 2, 3],
    ARRAY[ROW(1, 'x')::pg_temp.pr_inner, ROW(2, 'y')::pg_temp.pr_inner]
)::pg_temp.pr_wide AS value;

-- Full round trip: row -> binary -> decoded composite -> re-encoded jsonb
-- must match the original re-encoded the same way. (pr_wide carries a plain
-- `json` column, which has no default equality operator, so parity is
-- checked via re-serialization rather than a direct composite "=".)
SELECT (msgpack_to_jsonb(row_to_msgpack(msgpack_populate_record(NULL::pg_temp.pr_wide, row_to_msgpack(value)))) =
        msgpack_to_jsonb(row_to_msgpack(value))) AS msgpack_roundtrip,
       (msgpack_to_jsonb(row_to_msgpack(cbor_populate_record(NULL::pg_temp.pr_wide, row_to_cbor(value)))) =
        msgpack_to_jsonb(row_to_msgpack(value))) AS cbor_roundtrip,
       (msgpack_to_jsonb(row_to_msgpack(zera_populate_record(NULL::pg_temp.pr_wide, row_to_zera(value)))) =
        msgpack_to_jsonb(row_to_msgpack(value))) AS zera_roundtrip,
       (msgpack_to_jsonb(row_to_msgpack(flexbuffers_populate_record(NULL::pg_temp.pr_wide, row_to_flexbuffers(value)))) =
        msgpack_to_jsonb(row_to_msgpack(value))) AS flex_roundtrip,
       (msgpack_to_jsonb(row_to_msgpack(ion_populate_record(NULL::pg_temp.pr_wide, row_to_ion(value)))) =
        msgpack_to_jsonb(row_to_msgpack(value))) AS ion_roundtrip,
       (msgpack_to_jsonb(row_to_msgpack(bson_populate_record(NULL::pg_temp.pr_wide, row_to_bson(value)))) =
        msgpack_to_jsonb(row_to_msgpack(value))) AS bson_roundtrip,
       (msgpack_to_jsonb(row_to_msgpack(beve_populate_record(NULL::pg_temp.pr_wide, row_to_beve(value)))) =
        msgpack_to_jsonb(row_to_msgpack(value))) AS beve_roundtrip
FROM pr_values;

-- base = NULL, data supplied via a plain JSON bridge (msgpack_from_jsonb),
-- exercising the composite/array paths independent of row_to_X. `a_json`
-- must be passed as text ('{}', not '{}'::json) since jsonb_build_object
-- embeds json/jsonb-typed arguments as structure, not text, and a scalar
-- `json` column only accepts a JSON string here (matching what row_to_X
-- itself always emits for a json-typed column); a_jsonb/a_bytea are left
-- NULL since populating them via this schema-unaware bridge would require
-- hand-authoring pg_zerialize's own "~b" tag, which pr_binary below covers
-- via the real row_to_X path instead.
WITH bridged AS (
    SELECT msgpack_populate_record(NULL::pg_temp.pr_wide, msgpack_from_jsonb(jsonb_build_object(
               'a_int2', 1, 'a_int4', 2, 'a_int8', 3, 'a_float4', 1.5, 'a_float8', 2.5,
               'a_bool', true, 'a_text', 'hi', 'a_json', '{}',
               'a_uuid', '00000000-0000-0000-0000-000000000000',
               'a_name', 'n', 'a_char', 'Z', 'a_inet', '::1', 'a_cidr', '::/0',
               'a_interval', '0', 'a_numeric', 42, 'a_date', '2020-01-01',
               'a_timestamp', '2020-01-01 00:00:00', 'a_timestamptz', '2020-01-01 00:00:00+00',
               'a_composite', jsonb_build_object('a', 9, 'b', 'nine'),
               'a_array', jsonb_build_array(4, 5, 6),
               'a_composite_array', jsonb_build_array(
                   jsonb_build_object('a', 1, 'b', 'one'),
                   jsonb_build_object('a', 2, 'b', 'two'))
           ))) AS row
)
SELECT (row).a_json::text = '{}' AS bridged_json,
       (row).a_composite = ROW(9, 'nine')::pg_temp.pr_inner AS bridged_composite,
       (row).a_array = ARRAY[4, 5, 6] AS bridged_array,
       (row).a_composite_array = ARRAY[ROW(1, 'one')::pg_temp.pr_inner, ROW(2, 'two')::pg_temp.pr_inner]
       AS bridged_composite_array
FROM bridged;

-- Missing keys keep base's value; explicit JSON null clears the column.
CREATE TYPE pg_temp.pr_partial AS (f1 int, f2 text, f3 boolean);

SELECT msgpack_populate_record(
           ROW(1, 'original', true)::pg_temp.pr_partial,
           msgpack_from_jsonb(jsonb_build_object('f2', 'updated', 'f3', NULL))
       ) = ROW(1, 'updated', NULL)::pg_temp.pr_partial AS base_fallback_and_null;

-- base = NULL::type with data = NULL yields an all-NULL composite of that type.
SELECT msgpack_populate_record(NULL::pg_temp.pr_partial, NULL::bytea) =
       ROW(NULL, NULL, NULL)::pg_temp.pr_partial AS null_data_type_only;

-- base row with data = NULL returns base unchanged.
SELECT msgpack_populate_record(ROW(1, 'kept', false)::pg_temp.pr_partial, NULL::bytea) =
       ROW(1, 'kept', false)::pg_temp.pr_partial AS null_data_keeps_base;

-- 2-D array decode (matches try_serialize's multidimensional fast path).
CREATE TYPE pg_temp.pr_md AS (grid int[][]);
SELECT msgpack_populate_record(NULL::pg_temp.pr_md, row_to_msgpack(
           ROW(ARRAY[[1,2,3],[4,5,6]])::pg_temp.pr_md
       )) = ROW(ARRAY[[1,2,3],[4,5,6]])::pg_temp.pr_md AS md_array_roundtrip;

-- Empty array decodes back to an empty (not NULL) array.
SELECT (msgpack_populate_record(NULL::pg_temp.pr_partial, msgpack_from_jsonb(
            jsonb_build_object('f1', 1)
        ))).f1 = 1 AS scalar_only_document;
CREATE TYPE pg_temp.pr_arr AS (xs int[]);
SELECT (msgpack_populate_record(NULL::pg_temp.pr_arr, row_to_msgpack(ROW(ARRAY[]::int[])::pg_temp.pr_arr))).xs =
       ARRAY[]::int[] AS empty_array_roundtrip;

-- tagged_decimal numeric mode: exact decimal payload round-trips through
-- the "~n" tag rather than the lossy float64 default.
SET pg_zerialize.numeric_encoding = 'tagged_decimal';
CREATE TYPE pg_temp.pr_numeric AS (n numeric);
SELECT msgpack_populate_record(NULL::pg_temp.pr_numeric, row_to_msgpack(
           ROW('123456789012345678901234567890.123456789'::numeric)::pg_temp.pr_numeric
       )) = ROW('123456789012345678901234567890.123456789'::numeric)::pg_temp.pr_numeric
       AS tagged_decimal_roundtrip;
RESET pg_zerialize.numeric_encoding;

-- Domain constraints are enforced transparently via the domain's own input function.
CREATE DOMAIN pg_temp.pr_positive AS int CHECK (VALUE > 0);
CREATE TYPE pg_temp.pr_domain AS (n pg_temp.pr_positive);
SELECT msgpack_populate_record(NULL::pg_temp.pr_domain, msgpack_from_jsonb(
           jsonb_build_object('n', 5)
       )) = ROW(5)::pg_temp.pr_domain AS domain_accepts_valid;

CREATE FUNCTION pg_temp.pgz_domain_violation_rejected()
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM msgpack_populate_record(NULL::pg_temp.pr_domain, msgpack_from_jsonb(
        jsonb_build_object('n', -1)
    ));
    RETURN false;
EXCEPTION
    WHEN check_violation THEN RETURN true;
END
$$;
SELECT pg_temp.pgz_domain_violation_rejected() AS domain_rejects_invalid;

-- All four protocols agree on a bytea/jsonb tag round trip.
CREATE TYPE pg_temp.pr_binary AS (raw bytea, doc jsonb);
CREATE TEMP TABLE pr_binary_values AS
SELECT ROW(decode('00FF10AB', 'hex'), '{"nested":[1,2,3]}'::jsonb)::pg_temp.pr_binary AS value;

SELECT (msgpack_populate_record(NULL::pg_temp.pr_binary, row_to_msgpack(value)) = value) AS msgpack_binary_ok,
       (cbor_populate_record(NULL::pg_temp.pr_binary, row_to_cbor(value)) = value) AS cbor_binary_ok,
       (zera_populate_record(NULL::pg_temp.pr_binary, row_to_zera(value)) = value) AS zera_binary_ok,
       (flexbuffers_populate_record(NULL::pg_temp.pr_binary, row_to_flexbuffers(value)) = value) AS flex_binary_ok,
       (ion_populate_record(NULL::pg_temp.pr_binary, row_to_ion(value)) = value) AS ion_binary_ok,
       (bson_populate_record(NULL::pg_temp.pr_binary, row_to_bson(value)) = value) AS bson_binary_ok,
       (beve_populate_record(NULL::pg_temp.pr_binary, row_to_beve(value)) = value) AS beve_binary_ok
FROM pr_binary_values;

-- A malformed document (wrong shape for a scalar column) is rejected cleanly.
CREATE FUNCTION pg_temp.pgz_bad_shape_rejected()
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM msgpack_populate_record(NULL::pg_temp.pr_partial, msgpack_from_jsonb(
        jsonb_build_object('f1', jsonb_build_array(1, 2, 3, 4))
    ));
    RETURN false;
EXCEPTION
    WHEN invalid_text_representation THEN RETURN true;
END
$$;
SELECT pg_temp.pgz_bad_shape_rejected() AS bad_shape_rejected;

ROLLBACK;
DROP EXTENSION pg_zerialize;
