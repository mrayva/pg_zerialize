-- pg_zerialize extension upgrade from 1.5 to 1.6.

CREATE OR REPLACE FUNCTION zera_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'zera_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION zera_to_jsonb(bytea) IS
'Validate and decode one ZERA v1 document to jsonb; U8 typed arrays use base64';
