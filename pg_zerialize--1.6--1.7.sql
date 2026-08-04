-- pg_zerialize extension upgrade from 1.6 to 1.7.
--
-- Ports the msgpack_* JSONB-builder API (msgpack_from_jsonb, msgpack_build_object,
-- msgpack_build_array, msgpack_agg, msgpack_object_agg) to CBOR, ZERA, and
-- FlexBuffers, which previously could only serialize existing composite rows via
-- row_to_X/rows_to_X.

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
