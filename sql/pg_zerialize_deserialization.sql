SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;
BEGIN;

SELECT msgpack_to_jsonb(msgpack_from_jsonb(
           '{"object":{"enabled":true,"nothing":null},"array":[1,-2,3.5,"text"]}'::jsonb
       )) =
       '{"object":{"enabled":true,"nothing":null},"array":[1,-2,3.5,"text"]}'::jsonb
       AS nested_roundtrip;

SELECT msgpack_to_jsonb(decode('c0', 'hex')) = 'null'::jsonb AS scalar_null,
       msgpack_to_jsonb(decode('c3', 'hex')) = 'true'::jsonb AS scalar_bool,
       msgpack_to_jsonb(decode('d1ff7f', 'hex')) = '-129'::jsonb AS signed_integer,
       msgpack_to_jsonb(decode('cfffffffffffffffff', 'hex')) =
           '18446744073709551615'::jsonb AS unsigned_integer;

SELECT msgpack_to_jsonb(decode('cb7ff0000000000000', 'hex')) =
           '"Infinity"'::jsonb AS positive_infinity,
       msgpack_to_jsonb(decode('cbfff0000000000000', 'hex')) =
           '"-Infinity"'::jsonb AS negative_infinity,
       msgpack_to_jsonb(decode('cb7ff8000000000000', 'hex')) =
           '"NaN"'::jsonb AS nan_value;

SELECT msgpack_to_jsonb(decode('c40300ff10', 'hex')) =
       '["~b","AP8Q","base64"]'::jsonb AS binary_tag;

SELECT msgpack_to_jsonb(decode('ca3fc00000', 'hex')) = '1.5'::jsonb AS float32_value,
       msgpack_to_jsonb(decode('da0003616263', 'hex')) = '"abc"'::jsonb AS string16_value,
       msgpack_to_jsonb(decode('db00000003616263', 'hex')) = '"abc"'::jsonb AS string32_value,
       msgpack_to_jsonb(decode('c5000300ff10', 'hex')) =
           '["~b","AP8Q","base64"]'::jsonb AS binary16_value,
       msgpack_to_jsonb(decode('c60000000300ff10', 'hex')) =
           '["~b","AP8Q","base64"]'::jsonb AS binary32_value;

SELECT msgpack_to_jsonb(decode('dc0002c2c3', 'hex')) =
           '[false,true]'::jsonb AS array16_value,
       msgpack_to_jsonb(decode('dd00000002c2c3', 'hex')) =
           '[false,true]'::jsonb AS array32_value,
       msgpack_to_jsonb(decode('de0001a16101', 'hex')) =
           '{"a":1}'::jsonb AS map16_value,
       msgpack_to_jsonb(decode('df00000001a16101', 'hex')) =
           '{"a":1}'::jsonb AS map32_value;

SELECT msgpack_to_jsonb(msgpack_build_object(
           'quoted', E'a"b\\c\n',
           'nested', ARRAY[[1, NULL], [3, 4]]
       )) =
       '{"quoted":"a\"b\\c\n","nested":[[1,null],[3,4]]}'::jsonb
       AS escaped_and_multidimensional;

SELECT msgpack_to_jsonb(NULL::bytea) IS NULL AS strict_null;

CREATE FUNCTION pg_temp.pgz_msgpack_is_invalid(value bytea)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM msgpack_to_jsonb(value);
    RETURN false;
EXCEPTION
    WHEN invalid_binary_representation THEN RETURN true;
END
$$;

SELECT pg_temp.pgz_msgpack_is_invalid(decode('', 'hex')) AS empty_rejected,
       pg_temp.pgz_msgpack_is_invalid(decode('d90361', 'hex')) AS truncated_rejected,
       pg_temp.pgz_msgpack_is_invalid(decode('c0c0', 'hex')) AS trailing_rejected,
       pg_temp.pgz_msgpack_is_invalid(decode('d40000', 'hex')) AS extension_rejected,
       pg_temp.pgz_msgpack_is_invalid(decode('8101c0', 'hex')) AS nonstring_key_rejected,
       pg_temp.pgz_msgpack_is_invalid(decode('82a16101a16102', 'hex')) AS duplicate_key_rejected,
       pg_temp.pgz_msgpack_is_invalid(decode('a100', 'hex')) AS nul_string_rejected,
       pg_temp.pgz_msgpack_is_invalid(decode('a1ff', 'hex')) AS encoding_rejected,
       pg_temp.pgz_msgpack_is_invalid(decode('c1', 'hex')) AS reserved_rejected;

ROLLBACK;
DROP EXTENSION pg_zerialize;
