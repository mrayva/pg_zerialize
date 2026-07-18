-- pg_zerialize extension upgrade from 1.4 to 1.5.

CREATE OR REPLACE FUNCTION cbor_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'cbor_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION cbor_to_jsonb(bytea) IS
'Strictly decode one CBOR value to jsonb; byte strings use a tagged base64 array';
