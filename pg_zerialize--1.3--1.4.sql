-- pg_zerialize extension upgrade from 1.3 to 1.4.

CREATE OR REPLACE FUNCTION flexbuffers_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'flexbuffers_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION flexbuffers_to_jsonb(bytea) IS
'Decode one verified FlexBuffer value to jsonb; blobs use a tagged base64 array';
