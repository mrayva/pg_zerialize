-- pg_zerialize extension upgrade from 1.2 to 1.3.

CREATE OR REPLACE FUNCTION msgpack_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'msgpack_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION msgpack_to_jsonb(bytea) IS
'Decode one MessagePack value to jsonb; binary values use a tagged base64 array';
