-- pg_zerialize extension upgrade from 1.0 to 1.1
-- Update function volatility and preserve strictness for planner correctness.

ALTER FUNCTION row_to_flexbuffers(record) STABLE STRICT;
ALTER FUNCTION row_to_msgpack(record) STABLE STRICT;
CREATE OR REPLACE FUNCTION row_to_msgpack_slow(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_msgpack_slow'
LANGUAGE C STABLE STRICT;
CREATE OR REPLACE FUNCTION msgpack_from_jsonb(jsonb)
RETURNS bytea
AS 'MODULE_PATHNAME', 'msgpack_from_jsonb'
LANGUAGE C STABLE STRICT;
CREATE OR REPLACE FUNCTION msgpack_build_object(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'msgpack_build_object'
LANGUAGE C STABLE;
CREATE OR REPLACE FUNCTION msgpack_build_array(VARIADIC "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'msgpack_build_array'
LANGUAGE C STABLE;
CREATE OR REPLACE FUNCTION msgpack_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'msgpack_agg_final'
LANGUAGE C;
CREATE OR REPLACE FUNCTION msgpack_object_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'msgpack_object_agg_final'
LANGUAGE C;
ALTER FUNCTION row_to_cbor(record) STABLE STRICT;
ALTER FUNCTION row_to_zera(record) STABLE STRICT;

ALTER FUNCTION rows_to_flexbuffers(anyarray) STABLE STRICT;
ALTER FUNCTION rows_to_msgpack(anyarray) STABLE STRICT;
CREATE OR REPLACE FUNCTION rows_to_msgpack_slow(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_msgpack_slow'
LANGUAGE C STABLE STRICT;
DROP AGGREGATE IF EXISTS msgpack_agg(anyelement);
CREATE AGGREGATE msgpack_agg(anyelement)
(
    SFUNC = jsonb_agg_transfn,
    STYPE = internal,
    FINALFUNC = msgpack_agg_final
);
DROP AGGREGATE IF EXISTS msgpack_object_agg(text, anyelement);
CREATE AGGREGATE msgpack_object_agg(text, anyelement)
(
    SFUNC = jsonb_object_agg_transfn,
    STYPE = internal,
    FINALFUNC = msgpack_object_agg_final
);
ALTER FUNCTION rows_to_cbor(anyarray) STABLE STRICT;
ALTER FUNCTION rows_to_zera(anyarray) STABLE STRICT;
