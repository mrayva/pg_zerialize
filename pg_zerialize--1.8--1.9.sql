-- pg_zerialize extension upgrade from 1.8 to 1.9.
--
-- Adds X_populate_recordset(base anyelement, data bytea) for MessagePack,
-- CBOR, ZERA, and FlexBuffers: the reverse of rows_to_X, decoding a binary
-- array of documents into a set of typed composites. Mirrors
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
