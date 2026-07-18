SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

SELECT current_setting('pg_zerialize.numeric_encoding') = 'float64'
       AS default_is_compatible;

SET pg_zerialize.numeric_encoding = 'tagged_decimal';

CREATE TYPE pg_temp.pgz_numeric_policy AS (
    scaled numeric,
    huge numeric,
    special numeric,
    values numeric[]
);

CREATE TEMP TABLE pgz_numeric_policy_values AS
SELECT ROW(
    '1.2300'::numeric,
    '123456789012345678901234567890.12345678901234567890'::numeric,
    'Infinity'::numeric,
    ARRAY['-0.000'::numeric, 'NaN'::numeric, '-Infinity'::numeric]
)::pg_temp.pgz_numeric_policy AS value;

CREATE TEMP TABLE pgz_numeric_expected AS
SELECT '{
  "scaled":["~n","1.2300","decimal"],
  "huge":["~n","123456789012345678901234567890.12345678901234567890","decimal"],
  "special":["~n","Infinity","decimal"],
  "values":[
    ["~n","0.000","decimal"],
    ["~n","NaN","decimal"],
    ["~n","-Infinity","decimal"]
  ]
}'::jsonb AS value;

SELECT msgpack_to_jsonb(row_to_msgpack(v.value)) = e.value AS msgpack_exact,
       flexbuffers_to_jsonb(row_to_flexbuffers(v.value)) = e.value AS flex_exact,
       cbor_to_jsonb(row_to_cbor(v.value)) = e.value AS cbor_exact,
       zera_to_jsonb(row_to_zera(v.value)) = e.value AS zera_exact
FROM pgz_numeric_policy_values v CROSS JOIN pgz_numeric_expected e;

SELECT msgpack_to_jsonb(rows_to_msgpack(ARRAY[v.value])) =
           jsonb_build_array(e.value) AS msgpack_array_exact,
       flexbuffers_to_jsonb(rows_to_flexbuffers(ARRAY[v.value])) =
           jsonb_build_array(e.value) AS flex_array_exact,
       cbor_to_jsonb(rows_to_cbor(ARRAY[v.value])) =
           jsonb_build_array(e.value) AS cbor_array_exact,
       zera_to_jsonb(rows_to_zera(ARRAY[v.value])) =
           jsonb_build_array(e.value) AS zera_array_exact
FROM pgz_numeric_policy_values v CROSS JOIN pgz_numeric_expected e;

SELECT msgpack_to_jsonb(msgpack_build_array(
           '1.2300'::numeric,
           '123456789012345678901234567890.12345678901234567890'::numeric
       )) = '[
         ["~n","1.2300","decimal"],
         ["~n","123456789012345678901234567890.12345678901234567890","decimal"]
       ]'::jsonb AS builder_exact;

SELECT msgpack_to_jsonb(msgpack_from_jsonb(
           '{"value":123456789012345678901234567890.12345678901234567890}'::jsonb
       )) = '{
         "value":["~n","123456789012345678901234567890.12345678901234567890","decimal"]
       }'::jsonb AS jsonb_bridge_exact;

SET pg_zerialize.numeric_encoding = 'float64';

SELECT msgpack_to_jsonb(row_to_msgpack(ROW(1::numeric, 1.25::numeric))) =
       '{"f1":1,"f2":1.25}'::jsonb AS default_semantics_unchanged;

DROP EXTENSION pg_zerialize;
