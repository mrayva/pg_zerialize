-- pg_zerialize extension upgrade from 1.7 to 1.8.
--
-- Adds X_populate_record(base anyelement, data bytea) for MessagePack, CBOR,
-- ZERA, and FlexBuffers: the reverse of row_to_X, decoding a binary document
-- directly into a typed composite value. Mirrors jsonb_populate_record's
-- anyelement/base-row polymorphism: columns missing from the document keep
-- base's value, and base can be NULL::sometype to select the target type
-- without supplying default values.

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
