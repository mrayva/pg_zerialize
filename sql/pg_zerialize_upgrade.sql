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

ALTER EXTENSION pg_zerialize UPDATE TO '1.6';
SELECT extversion = '1.6' AS upgraded_to_1_6
FROM pg_extension
WHERE extname = 'pg_zerialize';
SELECT to_regprocedure('zera_to_jsonb(bytea)') IS NOT NULL AS zera_decoder_present;
SELECT zera_to_jsonb(row_to_zera(ROW(1, 'upgrade-ok'))) =
       '{"f1":1,"f2":"upgrade-ok"}'::jsonb AS zera_decoder_works;

ALTER EXTENSION pg_zerialize UPDATE TO '1.7';
SELECT extversion = '1.7' AS upgraded_to_1_7
FROM pg_extension
WHERE extname = 'pg_zerialize';
SELECT to_regprocedure('cbor_from_jsonb(jsonb)') IS NOT NULL AS cbor_builder_api_present,
       to_regprocedure('zera_build_object("any")') IS NOT NULL AS zera_builder_api_present,
       to_regprocedure('flexbuffers_agg(anyelement)') IS NOT NULL AS flex_builder_api_present;
SELECT cbor_to_jsonb(cbor_from_jsonb('{"upgrade":true}'::jsonb)) =
       '{"upgrade":true}'::jsonb AS cbor_builder_works;
SELECT zera_to_jsonb(zera_from_jsonb('{"upgrade":true}'::jsonb)) =
       '{"upgrade":true}'::jsonb AS zera_builder_works;
SELECT flexbuffers_to_jsonb(flexbuffers_from_jsonb('{"upgrade":true}'::jsonb)) =
       '{"upgrade":true}'::jsonb AS flex_builder_works;

ALTER EXTENSION pg_zerialize UPDATE TO '1.8';
SELECT extversion = '1.8' AS upgraded_to_1_8
FROM pg_extension
WHERE extname = 'pg_zerialize';
SELECT to_regprocedure('msgpack_populate_record(anyelement,bytea)') IS NOT NULL AS msgpack_populate_record_present,
       to_regprocedure('cbor_populate_record(anyelement,bytea)') IS NOT NULL AS cbor_populate_record_present,
       to_regprocedure('zera_populate_record(anyelement,bytea)') IS NOT NULL AS zera_populate_record_present,
       to_regprocedure('flexbuffers_populate_record(anyelement,bytea)') IS NOT NULL AS flex_populate_record_present;
CREATE TYPE pg_temp.upgrade_row AS (f1 int, f2 text);
SELECT msgpack_populate_record(NULL::pg_temp.upgrade_row, row_to_msgpack(ROW(1, 'upgrade-ok')::pg_temp.upgrade_row)) =
       ROW(1, 'upgrade-ok')::pg_temp.upgrade_row AS populate_record_works;

ALTER EXTENSION pg_zerialize UPDATE TO '1.9';
SELECT extversion = '1.9' AS upgraded_to_1_9
FROM pg_extension
WHERE extname = 'pg_zerialize';
SELECT to_regprocedure('msgpack_populate_recordset(anyelement,bytea)') IS NOT NULL AS msgpack_populate_recordset_present,
       to_regprocedure('cbor_populate_recordset(anyelement,bytea)') IS NOT NULL AS cbor_populate_recordset_present,
       to_regprocedure('zera_populate_recordset(anyelement,bytea)') IS NOT NULL AS zera_populate_recordset_present,
       to_regprocedure('flexbuffers_populate_recordset(anyelement,bytea)') IS NOT NULL AS flex_populate_recordset_present;
SELECT array_agg(r ORDER BY r.f1) = ARRAY[ROW(1, 'a')::pg_temp.upgrade_row, ROW(2, 'b')::pg_temp.upgrade_row]
FROM msgpack_populate_recordset(
    NULL::pg_temp.upgrade_row,
    rows_to_msgpack(ARRAY[ROW(1, 'a')::pg_temp.upgrade_row, ROW(2, 'b')::pg_temp.upgrade_row])
) AS r;
DROP TYPE pg_temp.upgrade_row;

DROP EXTENSION pg_zerialize;
