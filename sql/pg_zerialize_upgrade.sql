SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;

CREATE EXTENSION pg_zerialize VERSION '1.1';
SELECT extversion = '1.1' AS starts_at_1_1
FROM pg_extension
WHERE extname = 'pg_zerialize';

ALTER EXTENSION pg_zerialize UPDATE TO '1.2';
SELECT extversion = '1.2' AS upgraded_to_1_2
FROM pg_extension
WHERE extname = 'pg_zerialize';

SELECT to_regprocedure('msgpack_from_jsonb(jsonb)') IS NOT NULL AS nested_api_present;
SELECT to_regprocedure('msgpack_build_object("any")') IS NOT NULL AS object_builder_present;
SELECT row_to_msgpack(ROW(1, 'upgrade-ok')) IS NOT NULL AS serialization_works;

ALTER EXTENSION pg_zerialize UPDATE TO '1.3';
SELECT extversion = '1.3' AS upgraded_to_1_3
FROM pg_extension
WHERE extname = 'pg_zerialize';
SELECT to_regprocedure('msgpack_to_jsonb(bytea)') IS NOT NULL AS decoder_present;
SELECT msgpack_to_jsonb(msgpack_from_jsonb('{"upgrade":true}'::jsonb)) =
       '{"upgrade":true}'::jsonb AS decoder_works;

ALTER EXTENSION pg_zerialize UPDATE TO '1.4';
SELECT extversion = '1.4' AS upgraded_to_1_4
FROM pg_extension
WHERE extname = 'pg_zerialize';
SELECT to_regprocedure('flexbuffers_to_jsonb(bytea)') IS NOT NULL AS flex_decoder_present;
SELECT flexbuffers_to_jsonb(row_to_flexbuffers(ROW(1, 'upgrade-ok'))) =
       '{"f1":1,"f2":"upgrade-ok"}'::jsonb AS flex_decoder_works;

ALTER EXTENSION pg_zerialize UPDATE TO '1.5';
SELECT extversion = '1.5' AS upgraded_to_1_5
FROM pg_extension
WHERE extname = 'pg_zerialize';
SELECT to_regprocedure('cbor_to_jsonb(bytea)') IS NOT NULL AS cbor_decoder_present;
SELECT cbor_to_jsonb(row_to_cbor(ROW(1, 'upgrade-ok'))) =
       '{"f1":1,"f2":"upgrade-ok"}'::jsonb AS cbor_decoder_works;

DROP EXTENSION pg_zerialize;
