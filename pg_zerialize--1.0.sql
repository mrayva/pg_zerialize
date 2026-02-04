-- pg_zerialize extension SQL definitions

-- Function to convert a row to FlexBuffers format
CREATE OR REPLACE FUNCTION row_to_flexbuffers(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_flexbuffers'
LANGUAGE C IMMUTABLE STRICT;

COMMENT ON FUNCTION row_to_flexbuffers(record) IS
'Convert a PostgreSQL row/record to FlexBuffers binary format';

-- Function to convert a row to MessagePack format
CREATE OR REPLACE FUNCTION row_to_msgpack(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_msgpack'
LANGUAGE C IMMUTABLE STRICT;

COMMENT ON FUNCTION row_to_msgpack(record) IS
'Convert a PostgreSQL row/record to MessagePack binary format';

-- Function to convert a row to CBOR format
CREATE OR REPLACE FUNCTION row_to_cbor(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_cbor'
LANGUAGE C IMMUTABLE STRICT;

COMMENT ON FUNCTION row_to_cbor(record) IS
'Convert a PostgreSQL row/record to CBOR binary format';

-- Function to convert a row to ZERA format
CREATE OR REPLACE FUNCTION row_to_zera(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_zera'
LANGUAGE C IMMUTABLE STRICT;

COMMENT ON FUNCTION row_to_zera(record) IS
'Convert a PostgreSQL row/record to ZERA binary format (zerialize native protocol)';
