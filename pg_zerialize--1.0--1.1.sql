-- pg_zerialize extension upgrade from 1.0 to 1.1
-- Update function volatility and preserve strictness for planner correctness.

ALTER FUNCTION row_to_flexbuffers(record) STABLE STRICT;
ALTER FUNCTION row_to_msgpack(record) STABLE STRICT;
ALTER FUNCTION row_to_cbor(record) STABLE STRICT;
ALTER FUNCTION row_to_zera(record) STABLE STRICT;

ALTER FUNCTION rows_to_flexbuffers(anyarray) STABLE STRICT;
ALTER FUNCTION rows_to_msgpack(anyarray) STABLE STRICT;
ALTER FUNCTION rows_to_cbor(anyarray) STABLE STRICT;
ALTER FUNCTION rows_to_zera(anyarray) STABLE STRICT;
