-- pg_zerialize extension upgrade from 1.10 to 1.11.
--
-- Adds a third new binary protocol, BEVE, vendored from
-- mrayva/zerialize v1.2.0's beve.hpp (see vendor/zerialize/UPSTREAM.md)
-- plus a pruned vendored subset of glaze v8.0.0 (see vendor/glaze/UPSTREAM.md
-- for why glaze can't come from apt like jsoncons could for BSON). This
-- extension's PG_CPPFLAGS was bumped from -std=c++20 to -std=c++23, glaze's
-- own minimum -- confirmed the rest of pg_zerialize.cpp compiles unchanged
-- under c++23 before making that change.
--
-- BEVE gets full parity with MessagePack/CBOR/ZERA/FlexBuffers/Ion: unlike
-- BSON, BEVE has explicit type tags at every position including the root,
-- so there's no bare-array-root ambiguity to work around. Like Ion, it goes
-- through zerialize's generic dynamic-tree write path and the shared
-- Reader-based JSON decoder (reader_value_to_json in pg_zerialize.cpp)
-- rather than a hand-tuned per-format fast path.

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
