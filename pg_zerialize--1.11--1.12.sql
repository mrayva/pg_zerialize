-- pg_zerialize extension upgrade from 1.11 to 1.12.
--
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
