-- pg_zerialize extension SQL definitions, version 1.12

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

CREATE OR REPLACE FUNCTION msgpack_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'msgpack_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION msgpack_to_jsonb(bytea) IS
'Decode one MessagePack value to jsonb; binary values use a tagged base64 array';

CREATE OR REPLACE FUNCTION flexbuffers_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'flexbuffers_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION flexbuffers_to_jsonb(bytea) IS
'Decode one verified FlexBuffer value to jsonb; blobs use a tagged base64 array';

CREATE OR REPLACE FUNCTION cbor_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'cbor_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION cbor_to_jsonb(bytea) IS
'Strictly decode one CBOR value to jsonb; byte strings use a tagged base64 array';

CREATE OR REPLACE FUNCTION zera_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'zera_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION zera_to_jsonb(bytea) IS
'Validate and decode one ZERA v1 document to jsonb; U8 typed arrays use base64';

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

CREATE AGGREGATE msgpack_agg(anyelement)
(
    SFUNC = jsonb_agg_transfn,
    STYPE = internal,
    FINALFUNC = msgpack_agg_final
);

COMMENT ON AGGREGATE msgpack_agg(anyelement) IS
'Aggregate values into a MessagePack array (json_agg-style)';

CREATE AGGREGATE msgpack_object_agg(text, anyelement)
(
    SFUNC = jsonb_object_agg_transfn,
    STYPE = internal,
    FINALFUNC = msgpack_object_agg_final
);

COMMENT ON AGGREGATE msgpack_object_agg(text, anyelement) IS
'Aggregate key/value pairs into a MessagePack object (json_object_agg-style)';

-- CBOR builder API

CREATE OR REPLACE FUNCTION cbor_from_jsonb(jsonb)
RETURNS bytea
AS 'MODULE_PATHNAME', 'cbor_from_jsonb'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION cbor_from_jsonb(jsonb) IS
'Convert jsonb value (including nested objects/arrays) to CBOR';

CREATE OR REPLACE FUNCTION cbor_build_object(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'cbor_build_object'
LANGUAGE C STABLE;

COMMENT ON FUNCTION cbor_build_object(VARIADIC "any") IS
'Build a CBOR object from key/value pairs (json_build_object-style)';

CREATE OR REPLACE FUNCTION cbor_build_array(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'cbor_build_array'
LANGUAGE C STABLE;

COMMENT ON FUNCTION cbor_build_array(VARIADIC "any") IS
'Build a CBOR array from variadic values (json_build_array-style)';

CREATE OR REPLACE FUNCTION cbor_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'cbor_agg_final'
LANGUAGE C;

CREATE OR REPLACE FUNCTION cbor_object_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'cbor_object_agg_final'
LANGUAGE C;

CREATE AGGREGATE cbor_agg(anyelement)
(
    SFUNC = jsonb_agg_transfn,
    STYPE = internal,
    FINALFUNC = cbor_agg_final
);

COMMENT ON AGGREGATE cbor_agg(anyelement) IS
'Aggregate values into a CBOR array (json_agg-style)';

CREATE AGGREGATE cbor_object_agg(text, anyelement)
(
    SFUNC = jsonb_object_agg_transfn,
    STYPE = internal,
    FINALFUNC = cbor_object_agg_final
);

COMMENT ON AGGREGATE cbor_object_agg(text, anyelement) IS
'Aggregate key/value pairs into a CBOR object (json_object_agg-style)';

-- ZERA builder API

CREATE OR REPLACE FUNCTION zera_from_jsonb(jsonb)
RETURNS bytea
AS 'MODULE_PATHNAME', 'zera_from_jsonb'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION zera_from_jsonb(jsonb) IS
'Convert jsonb value (including nested objects/arrays) to ZERA';

CREATE OR REPLACE FUNCTION zera_build_object(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'zera_build_object'
LANGUAGE C STABLE;

COMMENT ON FUNCTION zera_build_object(VARIADIC "any") IS
'Build a ZERA object from key/value pairs (json_build_object-style)';

CREATE OR REPLACE FUNCTION zera_build_array(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'zera_build_array'
LANGUAGE C STABLE;

COMMENT ON FUNCTION zera_build_array(VARIADIC "any") IS
'Build a ZERA array from variadic values (json_build_array-style)';

CREATE OR REPLACE FUNCTION zera_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'zera_agg_final'
LANGUAGE C;

CREATE OR REPLACE FUNCTION zera_object_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'zera_object_agg_final'
LANGUAGE C;

CREATE AGGREGATE zera_agg(anyelement)
(
    SFUNC = jsonb_agg_transfn,
    STYPE = internal,
    FINALFUNC = zera_agg_final
);

COMMENT ON AGGREGATE zera_agg(anyelement) IS
'Aggregate values into a ZERA array (json_agg-style)';

CREATE AGGREGATE zera_object_agg(text, anyelement)
(
    SFUNC = jsonb_object_agg_transfn,
    STYPE = internal,
    FINALFUNC = zera_object_agg_final
);

COMMENT ON AGGREGATE zera_object_agg(text, anyelement) IS
'Aggregate key/value pairs into a ZERA object (json_object_agg-style)';

-- FlexBuffers builder API

CREATE OR REPLACE FUNCTION flexbuffers_from_jsonb(jsonb)
RETURNS bytea
AS 'MODULE_PATHNAME', 'flexbuffers_from_jsonb'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION flexbuffers_from_jsonb(jsonb) IS
'Convert jsonb value (including nested objects/arrays) to FlexBuffers';

CREATE OR REPLACE FUNCTION flexbuffers_build_object(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'flexbuffers_build_object'
LANGUAGE C STABLE;

COMMENT ON FUNCTION flexbuffers_build_object(VARIADIC "any") IS
'Build a FlexBuffers object from key/value pairs (json_build_object-style)';

CREATE OR REPLACE FUNCTION flexbuffers_build_array(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'flexbuffers_build_array'
LANGUAGE C STABLE;

COMMENT ON FUNCTION flexbuffers_build_array(VARIADIC "any") IS
'Build a FlexBuffers array from variadic values (json_build_array-style)';

CREATE OR REPLACE FUNCTION flexbuffers_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'flexbuffers_agg_final'
LANGUAGE C;

CREATE OR REPLACE FUNCTION flexbuffers_object_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'flexbuffers_object_agg_final'
LANGUAGE C;

CREATE AGGREGATE flexbuffers_agg(anyelement)
(
    SFUNC = jsonb_agg_transfn,
    STYPE = internal,
    FINALFUNC = flexbuffers_agg_final
);

COMMENT ON AGGREGATE flexbuffers_agg(anyelement) IS
'Aggregate values into a FlexBuffers array (json_agg-style)';

CREATE AGGREGATE flexbuffers_object_agg(text, anyelement)
(
    SFUNC = jsonb_object_agg_transfn,
    STYPE = internal,
    FINALFUNC = flexbuffers_object_agg_final
);

COMMENT ON AGGREGATE flexbuffers_object_agg(text, anyelement) IS
'Aggregate key/value pairs into a FlexBuffers object (json_object_agg-style)';

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

-- Decode-to-composite API: the reverse of row_to_X, decoding a binary
-- document directly into a typed composite value. Mirrors
-- jsonb_populate_record's anyelement/base-row polymorphism: columns missing
-- from the document keep base's value, and base can be NULL::sometype to
-- select the target type without supplying default values.

CREATE OR REPLACE FUNCTION msgpack_populate_record(base anyelement, data bytea)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'msgpack_populate_record'
LANGUAGE C STABLE;

COMMENT ON FUNCTION msgpack_populate_record(anyelement, bytea) IS
'Decode a MessagePack document into a typed composite, using base for columns the document omits';

CREATE OR REPLACE FUNCTION cbor_populate_record(base anyelement, data bytea)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'cbor_populate_record'
LANGUAGE C STABLE;

COMMENT ON FUNCTION cbor_populate_record(anyelement, bytea) IS
'Decode a CBOR document into a typed composite, using base for columns the document omits';

CREATE OR REPLACE FUNCTION zera_populate_record(base anyelement, data bytea)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'zera_populate_record'
LANGUAGE C STABLE;

COMMENT ON FUNCTION zera_populate_record(anyelement, bytea) IS
'Decode a ZERA document into a typed composite, using base for columns the document omits';

CREATE OR REPLACE FUNCTION flexbuffers_populate_record(base anyelement, data bytea)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'flexbuffers_populate_record'
LANGUAGE C STABLE;

COMMENT ON FUNCTION flexbuffers_populate_record(anyelement, bytea) IS
'Decode a FlexBuffer document into a typed composite, using base for columns the document omits';

-- Batch decode-to-recordset API: the reverse of rows_to_X, decoding a
-- binary array of documents into a set of typed composites. Mirrors
-- jsonb_populate_recordset's anyelement/base-row polymorphism: the same
-- base row supplies the fallback for columns each document omits, and
-- base can be NULL::sometype to select the result type without supplying
-- defaults. data = NULL yields an empty result set.

CREATE OR REPLACE FUNCTION msgpack_populate_recordset(base anyelement, data bytea)
RETURNS SETOF anyelement
AS 'MODULE_PATHNAME', 'msgpack_populate_recordset'
LANGUAGE C STABLE;

COMMENT ON FUNCTION msgpack_populate_recordset(anyelement, bytea) IS
'Decode a MessagePack array of documents into a set of typed composites, using base for columns each document omits';

CREATE OR REPLACE FUNCTION cbor_populate_recordset(base anyelement, data bytea)
RETURNS SETOF anyelement
AS 'MODULE_PATHNAME', 'cbor_populate_recordset'
LANGUAGE C STABLE;

COMMENT ON FUNCTION cbor_populate_recordset(anyelement, bytea) IS
'Decode a CBOR array of documents into a set of typed composites, using base for columns each document omits';

CREATE OR REPLACE FUNCTION zera_populate_recordset(base anyelement, data bytea)
RETURNS SETOF anyelement
AS 'MODULE_PATHNAME', 'zera_populate_recordset'
LANGUAGE C STABLE;

COMMENT ON FUNCTION zera_populate_recordset(anyelement, bytea) IS
'Decode a ZERA array of documents into a set of typed composites, using base for columns each document omits';

CREATE OR REPLACE FUNCTION flexbuffers_populate_recordset(base anyelement, data bytea)
RETURNS SETOF anyelement
AS 'MODULE_PATHNAME', 'flexbuffers_populate_recordset'
LANGUAGE C STABLE;

COMMENT ON FUNCTION flexbuffers_populate_recordset(anyelement, bytea) IS
'Decode a FlexBuffer array of documents into a set of typed composites, using base for columns each document omits';

-- Ion binary and BSON document protocols (see UPSTREAM.md; BSON is
-- a reduced, map/document-rooted-only subset -- see BSON.md and
-- pg_zerialize.cpp's forward-declaration comment).

-- Ion builder API

CREATE OR REPLACE FUNCTION ion_from_jsonb(jsonb)
RETURNS bytea
AS 'MODULE_PATHNAME', 'ion_from_jsonb'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION ion_from_jsonb(jsonb) IS
'Convert jsonb value (including nested objects/arrays) to Ion';

CREATE OR REPLACE FUNCTION ion_build_object(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'ion_build_object'
LANGUAGE C STABLE;

COMMENT ON FUNCTION ion_build_object(VARIADIC "any") IS
'Build an Ion struct from key/value pairs (json_build_object-style)';

CREATE OR REPLACE FUNCTION ion_build_array(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'ion_build_array'
LANGUAGE C STABLE;

COMMENT ON FUNCTION ion_build_array(VARIADIC "any") IS
'Build an Ion list from variadic values (json_build_array-style)';

CREATE OR REPLACE FUNCTION ion_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'ion_agg_final'
LANGUAGE C;

CREATE OR REPLACE FUNCTION ion_object_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'ion_object_agg_final'
LANGUAGE C;

CREATE AGGREGATE ion_agg(anyelement)
(
    SFUNC = jsonb_agg_transfn,
    STYPE = internal,
    FINALFUNC = ion_agg_final
);

COMMENT ON AGGREGATE ion_agg(anyelement) IS
'Aggregate values into an Ion list (json_agg-style)';

CREATE AGGREGATE ion_object_agg(text, anyelement)
(
    SFUNC = jsonb_object_agg_transfn,
    STYPE = internal,
    FINALFUNC = ion_object_agg_final
);

COMMENT ON AGGREGATE ion_object_agg(text, anyelement) IS
'Aggregate key/value pairs into an Ion struct (json_object_agg-style)';

-- BSON builder API (map/document-rooted subset only -- see above)

CREATE OR REPLACE FUNCTION bson_from_jsonb(jsonb)
RETURNS bytea
AS 'MODULE_PATHNAME', 'bson_from_jsonb'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION bson_from_jsonb(jsonb) IS
'Convert jsonb value to a BSON document; a jsonb array or scalar argument encodes but will not decode back as one (see BSON.md)';

CREATE OR REPLACE FUNCTION bson_build_object(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'bson_build_object'
LANGUAGE C STABLE;

COMMENT ON FUNCTION bson_build_object(VARIADIC "any") IS
'Build a BSON document from key/value pairs (json_build_object-style)';

CREATE OR REPLACE FUNCTION bson_object_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'bson_object_agg_final'
LANGUAGE C;

CREATE AGGREGATE bson_object_agg(text, anyelement)
(
    SFUNC = jsonb_object_agg_transfn,
    STYPE = internal,
    FINALFUNC = bson_object_agg_final
);

COMMENT ON AGGREGATE bson_object_agg(text, anyelement) IS
'Aggregate key/value pairs into a BSON document (json_object_agg-style)';

-- Function to convert a row to Ion format

CREATE OR REPLACE FUNCTION row_to_ion(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_ion'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION row_to_ion(record) IS
'Convert a PostgreSQL row/record to Ion binary format';

-- Function to convert a row to a BSON document

CREATE OR REPLACE FUNCTION row_to_bson(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_bson'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION row_to_bson(record) IS
'Convert a PostgreSQL row/record to a BSON document';

-- Function to decode one Ion value to jsonb

CREATE OR REPLACE FUNCTION ion_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'ion_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION ion_to_jsonb(bytea) IS
'Decode the first Ion value in a stream to jsonb; blobs use a tagged base64 array';

-- Function to decode one BSON document to jsonb

CREATE OR REPLACE FUNCTION bson_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'bson_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION bson_to_jsonb(bytea) IS
'Decode one BSON document to jsonb; an unannotated root always decodes as an object (see BSON.md); binary values use a tagged base64 array';

-- Function to convert an array of rows to Ion format
--
-- No rows_to_bson: array_to_binary<z::Bson> would produce a bare-array-root
-- BSON document, which cannot be read back as an array (see above).

CREATE OR REPLACE FUNCTION rows_to_ion(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_ion'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_ion(anyarray) IS
'Convert an array of PostgreSQL rows/records to Ion binary format (batch processing)';

-- Decode-to-composite API: the reverse of row_to_ion/row_to_bson.

CREATE OR REPLACE FUNCTION ion_populate_record(base anyelement, data bytea)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'ion_populate_record'
LANGUAGE C STABLE;

COMMENT ON FUNCTION ion_populate_record(anyelement, bytea) IS
'Decode an Ion document into a typed composite, using base for columns the document omits';

CREATE OR REPLACE FUNCTION bson_populate_record(base anyelement, data bytea)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'bson_populate_record'
LANGUAGE C STABLE;

COMMENT ON FUNCTION bson_populate_record(anyelement, bytea) IS
'Decode a BSON document into a typed composite, using base for columns the document omits';

-- Batch decode-to-recordset API: the reverse of rows_to_ion.
--
-- No bson_populate_recordset: it would require decoding a bare-array-root
-- BSON document, which cannot be told apart from a document on read.

CREATE OR REPLACE FUNCTION ion_populate_recordset(base anyelement, data bytea)
RETURNS SETOF anyelement
AS 'MODULE_PATHNAME', 'ion_populate_recordset'
LANGUAGE C STABLE;

COMMENT ON FUNCTION ion_populate_recordset(anyelement, bytea) IS
'Decode an Ion array of documents into a set of typed composites, using base for columns each document omits';

-- BEVE binary protocol (see UPSTREAM.md and vendor/glaze/UPSTREAM.md).

-- BEVE builder API

CREATE OR REPLACE FUNCTION beve_from_jsonb(jsonb)
RETURNS bytea
AS 'MODULE_PATHNAME', 'beve_from_jsonb'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION beve_from_jsonb(jsonb) IS
'Convert jsonb value (including nested objects/arrays) to BEVE';

CREATE OR REPLACE FUNCTION beve_build_object(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'beve_build_object'
LANGUAGE C STABLE;

COMMENT ON FUNCTION beve_build_object(VARIADIC "any") IS
'Build a BEVE object from key/value pairs (json_build_object-style)';

CREATE OR REPLACE FUNCTION beve_build_array(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'beve_build_array'
LANGUAGE C STABLE;

COMMENT ON FUNCTION beve_build_array(VARIADIC "any") IS
'Build a BEVE array from variadic values (json_build_array-style)';

CREATE OR REPLACE FUNCTION beve_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'beve_agg_final'
LANGUAGE C;

CREATE OR REPLACE FUNCTION beve_object_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'beve_object_agg_final'
LANGUAGE C;

CREATE AGGREGATE beve_agg(anyelement)
(
    SFUNC = jsonb_agg_transfn,
    STYPE = internal,
    FINALFUNC = beve_agg_final
);

COMMENT ON AGGREGATE beve_agg(anyelement) IS
'Aggregate values into a BEVE array (json_agg-style)';

CREATE AGGREGATE beve_object_agg(text, anyelement)
(
    SFUNC = jsonb_object_agg_transfn,
    STYPE = internal,
    FINALFUNC = beve_object_agg_final
);

COMMENT ON AGGREGATE beve_object_agg(text, anyelement) IS
'Aggregate key/value pairs into a BEVE object (json_object_agg-style)';

-- Function to convert a row to BEVE format

CREATE OR REPLACE FUNCTION row_to_beve(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_beve'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION row_to_beve(record) IS
'Convert a PostgreSQL row/record to BEVE binary format';

-- Function to decode one BEVE value to jsonb

CREATE OR REPLACE FUNCTION beve_to_jsonb(bytea)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'beve_to_jsonb'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

COMMENT ON FUNCTION beve_to_jsonb(bytea) IS
'Decode one BEVE value to jsonb; blobs use a tagged base64 array';

-- Function to convert an array of rows to BEVE format

CREATE OR REPLACE FUNCTION rows_to_beve(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_beve'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_beve(anyarray) IS
'Convert an array of PostgreSQL rows/records to BEVE binary format (batch processing)';

-- Decode-to-composite API: the reverse of row_to_beve.

CREATE OR REPLACE FUNCTION beve_populate_record(base anyelement, data bytea)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'beve_populate_record'
LANGUAGE C STABLE;

COMMENT ON FUNCTION beve_populate_record(anyelement, bytea) IS
'Decode a BEVE document into a typed composite, using base for columns the document omits';

-- Batch decode-to-recordset API: the reverse of rows_to_beve.

CREATE OR REPLACE FUNCTION beve_populate_recordset(base anyelement, data bytea)
RETURNS SETOF anyelement
AS 'MODULE_PATHNAME', 'beve_populate_recordset'
LANGUAGE C STABLE;

COMMENT ON FUNCTION beve_populate_recordset(anyelement, bytea) IS
'Decode a BEVE array of documents into a set of typed composites, using base for columns each document omits';

-- Columnar batch serialization (see pg_zerialize--1.11--1.12.sql for full rationale).

-- Adds opt-in "columnar batch" serialization: rows_to_<fmt>_columnar(anyarray)
-- converts an array of records into a single document shaped
-- {"col1": [v,v,...], "col2": [v,v,...], ...} (one array per column, values
-- in row order) instead of rows_to_<fmt>'s [{row},{row},...] array-of-documents
-- shape.
--
-- Motivation: profiling (see pg_zerialize.cpp's "Columnar batch
-- serialization" comment) showed columnar batching collapses key-encoding
-- work from N_rows*N_columns calls down to N_columns calls, and amortizes
-- fixed per-document overhead (Ion's local symbol table, FlexBuffers' key
-- dedup) across the whole batch -- a real throughput win confirmed via a
-- standalone profiling harness, unlike an earlier jsonb/SQL-level batching
-- experiment (nats_publish_from_sql.py's --batch-size) that only won on
-- payload size, not throughput, because the jsonb_agg()/GROUP BY
-- construction cost outweighed the encoder-side saving.
--
-- Available for all 7 formats including BSON: unlike rows_to_bson (which
-- doesn't exist -- BSON's wire format can't distinguish a root array from a
-- root document), a columnar document's root is always an object, so no
-- such ambiguity applies here.
--
-- Requirements (see columnar_batch_schema() in pg_zerialize.cpp):
--   - All non-null rows must share the same composite type OID.
--   - No nested composite/array-of-composite columns (flat scalar schemas
--     only).
--   - Empty array input produces an empty document ({}).

CREATE OR REPLACE FUNCTION rows_to_msgpack_columnar(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_msgpack_columnar'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_msgpack_columnar(anyarray) IS
'Convert an array of PostgreSQL rows/records to a columnar (object-of-column-arrays) MessagePack document';

CREATE OR REPLACE FUNCTION rows_to_cbor_columnar(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_cbor_columnar'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_cbor_columnar(anyarray) IS
'Convert an array of PostgreSQL rows/records to a columnar (object-of-column-arrays) CBOR document';

CREATE OR REPLACE FUNCTION rows_to_zera_columnar(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_zera_columnar'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_zera_columnar(anyarray) IS
'Convert an array of PostgreSQL rows/records to a columnar (object-of-column-arrays) ZERA document';

CREATE OR REPLACE FUNCTION rows_to_flexbuffers_columnar(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_flexbuffers_columnar'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_flexbuffers_columnar(anyarray) IS
'Convert an array of PostgreSQL rows/records to a columnar (object-of-column-arrays) FlexBuffers document';

CREATE OR REPLACE FUNCTION rows_to_ion_columnar(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_ion_columnar'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_ion_columnar(anyarray) IS
'Convert an array of PostgreSQL rows/records to a columnar (object-of-column-arrays) Ion document';

CREATE OR REPLACE FUNCTION rows_to_bson_columnar(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_bson_columnar'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_bson_columnar(anyarray) IS
'Convert an array of PostgreSQL rows/records to a columnar (object-of-column-arrays) BSON document';

CREATE OR REPLACE FUNCTION rows_to_beve_columnar(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_beve_columnar'
LANGUAGE C STABLE STRICT;

COMMENT ON FUNCTION rows_to_beve_columnar(anyarray) IS
'Convert an array of PostgreSQL rows/records to a columnar (object-of-column-arrays) BEVE document';
