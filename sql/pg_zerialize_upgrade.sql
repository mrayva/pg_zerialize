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

DROP EXTENSION pg_zerialize;
