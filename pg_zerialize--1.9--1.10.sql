-- pg_zerialize extension upgrade from 1.9 to 1.10.
--
-- Adds two new binary protocols, Ion and BSON, vendored from
-- mrayva/zerialize v1.2.0 (see vendor/zerialize/UPSTREAM.md). Both go
-- through zerialize's generic dynamic-tree write path and a shared
-- Reader-based JSON decoder rather than the hand-tuned per-format fast
-- paths MessagePack/CBOR/ZERA/FlexBuffers have -- correct and fully
-- tested, not yet performance-tuned to the same degree.
--
-- Ion gets full parity with the other four protocols. BSON gets a reduced
-- surface: a BSON document and a bare array are wire-identical at the root
-- (no parent element header to carry the type tag there), so anything that
-- would produce or require a bare-array-root BSON value is deliberately
-- not provided -- see BSON.md and pg_zerialize.cpp's forward-declaration
-- comment. bson_build_array, bson_agg, rows_to_bson, and
-- bson_populate_recordset are therefore absent; only the map/document-rooted
-- subset is exposed.

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
