SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;
BEGIN;

CREATE TYPE pg_temp.pgz_nested_coordinates AS (
    latitude double precision,
    longitude double precision
);

CREATE TYPE pg_temp.pgz_nested_address AS (
    city text,
    postal_code text,
    coordinates pg_temp.pgz_nested_coordinates
);

CREATE TYPE pg_temp.pgz_nested_customer AS (
    id integer,
    name text,
    shipping pg_temp.pgz_nested_address,
    previous_addresses pg_temp.pgz_nested_address[],
    billing pg_temp.pgz_nested_address
);

CREATE TEMP TABLE pgz_nested_values AS
SELECT ROW(
    7,
    'Ada',
    ROW(
        'London',
        'SW1A',
        ROW(51.501, -0.142)::pg_temp.pgz_nested_coordinates
    )::pg_temp.pgz_nested_address,
    ARRAY[
        ROW(
            'Paris',
            '75001',
            ROW(48.861, 2.335)::pg_temp.pgz_nested_coordinates
        )::pg_temp.pgz_nested_address,
        NULL::pg_temp.pgz_nested_address
    ],
    NULL::pg_temp.pgz_nested_address
)::pg_temp.pgz_nested_customer AS value;

-- MessagePack's generic path is the reference representation for composites.
SELECT row_to_msgpack(value) = row_to_msgpack_slow(value) AS msgpack_nested_parity
FROM pgz_nested_values;

-- Every protocol must accept the same recursively nested value.
SELECT octet_length(row_to_msgpack(value)) > 0 AS msgpack_nested,
       octet_length(row_to_cbor(value)) > 0 AS cbor_nested,
       octet_length(row_to_zera(value)) > 0 AS zera_nested,
       octet_length(row_to_flexbuffers(value)) > 0 AS flex_nested,
       octet_length(row_to_ion(value)) > 0 AS ion_nested,
       octet_length(row_to_bson(value)) > 0 AS bson_nested,
       octet_length(row_to_beve(value)) > 0 AS beve_nested
FROM pgz_nested_values;

-- Batch conversion preserves nested composites and null outer records.
-- No rows_to_bson: BSON's wire format can't distinguish a bare root array
-- from a root document (see BSON.md), so there is no batch entry point.
SELECT octet_length(rows_to_msgpack(ARRAY[value, NULL::pg_temp.pgz_nested_customer])) > 0 AS msgpack_batch_nested,
       octet_length(rows_to_cbor(ARRAY[value, NULL::pg_temp.pgz_nested_customer])) > 0 AS cbor_batch_nested,
       octet_length(rows_to_zera(ARRAY[value, NULL::pg_temp.pgz_nested_customer])) > 0 AS zera_batch_nested,
       octet_length(rows_to_flexbuffers(ARRAY[value, NULL::pg_temp.pgz_nested_customer])) > 0 AS flex_batch_nested,
       octet_length(rows_to_ion(ARRAY[value, NULL::pg_temp.pgz_nested_customer])) > 0 AS ion_batch_nested,
       octet_length(rows_to_beve(ARRAY[value, NULL::pg_temp.pgz_nested_customer])) > 0 AS beve_batch_nested
FROM pgz_nested_values;

-- CBOR/ZERA/Flex/Ion/BSON/BEVE now recurse through their own cached writer
-- plans for composite columns and composite arrays too (previously only
-- MessagePack did; the others fell back to the generic dynamic tree for any
-- schema with a composite-typed column). Cross-check their recursive
-- fast-path output against MessagePack's, decoded through each protocol's
-- own JSONB decoder, since only MessagePack exposes a _slow() helper for
-- direct byte parity.
SELECT (cbor_to_jsonb(row_to_cbor(value)) = msgpack_to_jsonb(row_to_msgpack(value))) AS cbor_nested_matches_msgpack,
       (zera_to_jsonb(row_to_zera(value)) = msgpack_to_jsonb(row_to_msgpack(value))) AS zera_nested_matches_msgpack,
       (flexbuffers_to_jsonb(row_to_flexbuffers(value)) = msgpack_to_jsonb(row_to_msgpack(value))) AS flex_nested_matches_msgpack,
       (ion_to_jsonb(row_to_ion(value)) = msgpack_to_jsonb(row_to_msgpack(value))) AS ion_nested_matches_msgpack,
       (bson_to_jsonb(row_to_bson(value)) = msgpack_to_jsonb(row_to_msgpack(value))) AS bson_nested_matches_msgpack,
       (beve_to_jsonb(row_to_beve(value)) = msgpack_to_jsonb(row_to_msgpack(value))) AS beve_nested_matches_msgpack
FROM pgz_nested_values;

SELECT (cbor_to_jsonb(rows_to_cbor(ARRAY[value, NULL::pg_temp.pgz_nested_customer]))
        = msgpack_to_jsonb(rows_to_msgpack(ARRAY[value, NULL::pg_temp.pgz_nested_customer]))) AS cbor_batch_matches_msgpack,
       (zera_to_jsonb(rows_to_zera(ARRAY[value, NULL::pg_temp.pgz_nested_customer]))
        = msgpack_to_jsonb(rows_to_msgpack(ARRAY[value, NULL::pg_temp.pgz_nested_customer]))) AS zera_batch_matches_msgpack,
       (flexbuffers_to_jsonb(rows_to_flexbuffers(ARRAY[value, NULL::pg_temp.pgz_nested_customer]))
        = msgpack_to_jsonb(rows_to_msgpack(ARRAY[value, NULL::pg_temp.pgz_nested_customer]))) AS flex_batch_matches_msgpack,
       (ion_to_jsonb(rows_to_ion(ARRAY[value, NULL::pg_temp.pgz_nested_customer]))
        = msgpack_to_jsonb(rows_to_msgpack(ARRAY[value, NULL::pg_temp.pgz_nested_customer]))) AS ion_batch_matches_msgpack,
       (beve_to_jsonb(rows_to_beve(ARRAY[value, NULL::pg_temp.pgz_nested_customer]))
        = msgpack_to_jsonb(rows_to_msgpack(ARRAY[value, NULL::pg_temp.pgz_nested_customer]))) AS beve_batch_matches_msgpack
FROM pgz_nested_values;

-- A composite column whose own nested type contains an unsupported column
-- (point has no protocol writer) cannot use any protocol's recursive fast
-- path; every protocol must fall back to the generic dynamic tree rather
-- than erroring.
CREATE TYPE pg_temp.pgz_nested_unsupported_inner AS (id integer, location point);
CREATE TYPE pg_temp.pgz_nested_unsupported_outer AS (
    label text,
    inner_value pg_temp.pgz_nested_unsupported_inner
);

SELECT (cbor_to_jsonb(row_to_cbor(v))
        = msgpack_to_jsonb(row_to_msgpack(v))) AS cbor_fallback_matches_msgpack,
       (zera_to_jsonb(row_to_zera(v))
        = msgpack_to_jsonb(row_to_msgpack(v))) AS zera_fallback_matches_msgpack,
       (flexbuffers_to_jsonb(row_to_flexbuffers(v))
        = msgpack_to_jsonb(row_to_msgpack(v))) AS flex_fallback_matches_msgpack,
       (ion_to_jsonb(row_to_ion(v))
        = msgpack_to_jsonb(row_to_msgpack(v))) AS ion_fallback_matches_msgpack,
       (bson_to_jsonb(row_to_bson(v))
        = msgpack_to_jsonb(row_to_msgpack(v))) AS bson_fallback_matches_msgpack,
       (beve_to_jsonb(row_to_beve(v))
        = msgpack_to_jsonb(row_to_msgpack(v))) AS beve_fallback_matches_msgpack
FROM (SELECT ROW(
          'unsupported-nested',
          ROW(1, '(1,2)'::point)::pg_temp.pgz_nested_unsupported_inner
      )::pg_temp.pgz_nested_unsupported_outer AS v) s;

-- Composite values supplied to SQL builders are nested maps, not text.
SELECT msgpack_build_array(
           ROW('Rome', '00100', NULL)::pg_temp.pgz_nested_address
       ) IS NOT NULL AS builder_composite;

-- Nested type DDL invalidates cached outer and inner descriptors.
SELECT row_to_msgpack(value) IS NOT NULL AS cache_warmed
FROM pgz_nested_values;
ALTER TYPE pg_temp.pgz_nested_address ADD ATTRIBUTE country text;
SELECT row_to_msgpack(
    ROW(
        8,
        'Grace',
        ROW('Arlington', '22201', NULL, 'US')::pg_temp.pgz_nested_address,
        ARRAY[]::pg_temp.pgz_nested_address[],
        NULL::pg_temp.pgz_nested_address
    )::pg_temp.pgz_nested_customer
) = row_to_msgpack_slow(
    ROW(
        8,
        'Grace',
        ROW('Arlington', '22201', NULL, 'US')::pg_temp.pgz_nested_address,
        ARRAY[]::pg_temp.pgz_nested_address[],
        NULL::pg_temp.pgz_nested_address
    )::pg_temp.pgz_nested_customer
) AS nested_ddl_parity;

-- Dropped attributes are omitted from recursively serialized maps.
ALTER TYPE pg_temp.pgz_nested_address DROP ATTRIBUTE postal_code;
SELECT row_to_msgpack(
    ROW(
        9,
        'Katherine',
        ROW('Hampton', NULL, 'US')::pg_temp.pgz_nested_address,
        ARRAY[]::pg_temp.pgz_nested_address[],
        NULL::pg_temp.pgz_nested_address
    )::pg_temp.pgz_nested_customer
) = row_to_msgpack_slow(
    ROW(
        9,
        'Katherine',
        ROW('Hampton', NULL, 'US')::pg_temp.pgz_nested_address,
        ARRAY[]::pg_temp.pgz_nested_address[],
        NULL::pg_temp.pgz_nested_address
    )::pg_temp.pgz_nested_customer
) AS dropped_nested_attribute_parity;

ROLLBACK;
DROP EXTENSION pg_zerialize;
