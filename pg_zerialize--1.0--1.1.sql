-- pg_zerialize extension upgrade from 1.0 to 1.1
-- Update function volatility and preserve strictness for planner correctness.

ALTER FUNCTION row_to_flexbuffers(record) STABLE STRICT;
ALTER FUNCTION row_to_msgpack(record) STABLE STRICT;
CREATE OR REPLACE FUNCTION row_to_msgpack_slow(record)
RETURNS bytea
AS 'MODULE_PATHNAME', 'row_to_msgpack_slow'
LANGUAGE C STABLE STRICT;
ALTER FUNCTION row_to_cbor(record) STABLE STRICT;
ALTER FUNCTION row_to_zera(record) STABLE STRICT;

ALTER FUNCTION rows_to_flexbuffers(anyarray) STABLE STRICT;
ALTER FUNCTION rows_to_msgpack(anyarray) STABLE STRICT;
CREATE OR REPLACE FUNCTION rows_to_msgpack_slow(anyarray)
RETURNS bytea
AS 'MODULE_PATHNAME', 'rows_to_msgpack_slow'
LANGUAGE C STABLE STRICT;
ALTER FUNCTION rows_to_cbor(anyarray) STABLE STRICT;
ALTER FUNCTION rows_to_zera(anyarray) STABLE STRICT;
