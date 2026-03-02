-- pg_zerialize extension SQL definitions

-- Function to convert a row to FlexBuffers format
CREATE OR REPLACE FUNCTION row_to_flexbuffers(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_flexbuffers'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION row_to_flexbuffers(record) IS
'Convert a PostgreSQL row/record to FlexBuffers binary format';

-- Function to convert a row to MessagePack format
CREATE OR REPLACE FUNCTION row_to_msgpack(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_msgpack'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION row_to_msgpack(record) IS
'Convert a PostgreSQL row/record to MessagePack binary format';

-- Test helper: force generic (slow) MessagePack path for parity validation
CREATE OR REPLACE FUNCTION row_to_msgpack_slow(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_msgpack_slow'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION row_to_msgpack_slow(record) IS
'Convert a PostgreSQL row/record to MessagePack using generic slow path (test/parity helper)';

-- Convert nested jsonb to nested MessagePack
CREATE OR REPLACE FUNCTION msgpack_from_jsonb(jsonb)
RETURNS bytea
AS 'MODULE_PATHNAME', 'msgpack_from_jsonb'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION msgpack_from_jsonb(jsonb) IS
'Convert jsonb value (including nested objects/arrays) to MessagePack';

-- SQL-builder style wrappers
CREATE OR REPLACE FUNCTION msgpack_build_object(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'msgpack_build_object'
LANGUAGE C STABLE;

COMMENT ON FUNCTION msgpack_build_object(VARIADIC "any") IS
'Build a MessagePack object from key/value pairs (json_build_object-style)';

CREATE OR REPLACE FUNCTION msgpack_build_array(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'msgpack_build_array'
LANGUAGE C STABLE;

COMMENT ON FUNCTION msgpack_build_array(VARIADIC "any") IS
'Build a MessagePack array from variadic values (json_build_array-style)';

-- Aggregate finalizers
CREATE OR REPLACE FUNCTION msgpack_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'msgpack_agg_final'
LANGUAGE C;

CREATE OR REPLACE FUNCTION msgpack_object_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'msgpack_object_agg_final'
LANGUAGE C;

DROP AGGREGATE IF EXISTS msgpack_agg(anyelement);
CREATE AGGREGATE msgpack_agg(anyelement)
(
    SFUNC = jsonb_agg_transfn,
    STYPE = internal,
    FINALFUNC = msgpack_agg_final
);

COMMENT ON AGGREGATE msgpack_agg(anyelement) IS
'Aggregate values into a MessagePack array (json_agg-style)';

DROP AGGREGATE IF EXISTS msgpack_object_agg(text, anyelement);
CREATE AGGREGATE msgpack_object_agg(text, anyelement)
(
    SFUNC = jsonb_object_agg_transfn,
    STYPE = internal,
    FINALFUNC = msgpack_object_agg_final
);

COMMENT ON AGGREGATE msgpack_object_agg(text, anyelement) IS
'Aggregate key/value pairs into a MessagePack object (json_object_agg-style)';

-- Function to convert a row to CBOR format
CREATE OR REPLACE FUNCTION row_to_cbor(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_cbor'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION row_to_cbor(record) IS
'Convert a PostgreSQL row/record to CBOR binary format';

-- Function to convert a row to ZERA format
CREATE OR REPLACE FUNCTION row_to_zera(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_zera'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION row_to_zera(record) IS
'Convert a PostgreSQL row/record to ZERA binary format (zerialize native protocol)';

-- Batch processing functions (multiple rows at once for better performance)

-- Function to convert an array of rows to FlexBuffers format
CREATE OR REPLACE FUNCTION rows_to_flexbuffers(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_flexbuffers'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_flexbuffers(anyarray) IS
'Convert an array of PostgreSQL rows/records to FlexBuffers binary format (batch processing)';

-- Function to convert an array of rows to MessagePack format
CREATE OR REPLACE FUNCTION rows_to_msgpack(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_msgpack'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_msgpack(anyarray) IS
'Convert an array of PostgreSQL rows/records to MessagePack binary format (batch processing)';

-- Test helper: force generic (slow) MessagePack batch path for parity validation
CREATE OR REPLACE FUNCTION rows_to_msgpack_slow(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_msgpack_slow'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_msgpack_slow(anyarray) IS
'Convert an array of PostgreSQL rows/records to MessagePack using generic slow path (test/parity helper)';

-- Function to convert an array of rows to CBOR format
CREATE OR REPLACE FUNCTION rows_to_cbor(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_cbor'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_cbor(anyarray) IS
'Convert an array of PostgreSQL rows/records to CBOR binary format (batch processing)';

-- Function to convert an array of rows to ZERA format
CREATE OR REPLACE FUNCTION rows_to_zera(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_zera'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_zera(anyarray) IS
'Convert an array of PostgreSQL rows/records to ZERA binary format (batch processing)';
