# pg_zerialize

PostgreSQL extension for serializing rows and batches of rows to MessagePack,
CBOR, ZERA, and FlexBuffers. MessagePack also includes SQL builders and
aggregates for constructing nested binary values.

## Support

- PostgreSQL 16, 17, and 18
- C++20 compiler
- Linux/PGXS build environment

CI builds and tests every supported PostgreSQL major version. Local development
and performance work use PostgreSQL 18.

## Requirements

- PostgreSQL server development package (`postgresql-server-dev-<major>`)
- C++20 compiler (GCC 10+ or Clang 10+)
- FlatBuffers development package (`libflatbuffers-dev`)
- fast_float development package (`libfast-float-dev`)
- Python `msgpack` package for independent MessagePack semantic tests

The zerialize headers are vendored under `vendor/`. See
[`vendor/zerialize/UPSTREAM.md`](vendor/zerialize/UPSTREAM.md) for provenance
and local changes.

## Build And Test

```bash
make -j"$(nproc)"
sudo make install

make installcheck
make semantic-check
```

The full manual SQL suite is available when needed:

```bash
psql -v ON_ERROR_STOP=1 -d postgres -f test_pg_zerialize.sql
```

## Row Serialization

```sql
CREATE EXTENSION pg_zerialize;

SELECT row_to_msgpack(users.*) FROM users;
SELECT row_to_cbor(users.*) FROM users;
SELECT row_to_zera(users.*) FROM users;
SELECT row_to_flexbuffers(users.*) FROM users;
```

Each function returns one protocol document as `bytea`. A row is represented as
a map/object whose keys are PostgreSQL attribute names.

## Batch Serialization

```sql
SELECT rows_to_msgpack(array_agg(users.*)) FROM users;
SELECT rows_to_cbor(array_agg(users.*)) FROM users;
SELECT rows_to_zera(array_agg(users.*)) FROM users;
SELECT rows_to_flexbuffers(array_agg(users.*)) FROM users;
```

Batch functions return one protocol array containing row maps. They accept a
one-dimensional array of composite records and preserve null records.

## Nested Values

Named composite columns are recursively represented as nested protocol maps.
This applies to row and batch serialization for all four protocols.

MessagePack also provides JSON-style builders and aggregates:

```sql
SELECT msgpack_from_jsonb(
  jsonb_build_object(
    'department', d.name,
    'staff', COALESCE(e.employees, '[]'::jsonb)
  )
)
FROM departments d
LEFT JOIN (
  SELECT department_id,
         jsonb_agg(jsonb_build_object('id', id, 'name', name)) AS employees
  FROM employees
  GROUP BY department_id
) e ON e.department_id = d.id;

SELECT msgpack_build_object('id', 7, 'active', true);
SELECT msgpack_build_array(1, 'two', NULL, 3.5::numeric);
SELECT msgpack_agg(value ORDER BY id) FROM items;
SELECT msgpack_object_agg(key, value ORDER BY key) FROM items;
SELECT msgpack_to_jsonb(msgpack_build_object('id', 7, 'active', true));
SELECT flexbuffers_to_jsonb(row_to_flexbuffers(users.*)) FROM users;
```

Passing a builder's `bytea` result into another builder encodes that result as a
binary blob. Use one JSONB tree with `msgpack_from_jsonb` when values must be
spliced into one nested MessagePack document.

## Wire Semantics

- `int2`, `int4`, and `int8` are protocol integers.
- `float4` and `float8` are protocol floating-point values.
- Integral `numeric` values fitting in signed 64 bits are exact integers. Other
  `numeric` values are `float64` and may lose decimal precision.
- The default decimal-to-float parser is fast_float. Set
  `pg_zerialize.numeric_float_backend = 'postgres'` to use PostgreSQL's parser.
- Date values are PostgreSQL days since 2000-01-01.
- Timestamp values are PostgreSQL microseconds since 2000-01-01.
- `bytea` and row-level `jsonb` values are binary payloads.
- A `json` value remains its original JSON text string.
- UUID, enum, `name`, internal `"char"`, inet/cidr, and interval values use
  canonical PostgreSQL-compatible text representations.
- PostgreSQL arrays become nested protocol arrays and preserve dimensions and
  null elements. PostgreSQL lower bounds are not represented on the wire.
- Batch serialization still rejects multidimensional outer arrays because its
  outer array is reserved for rows.
- `msgpack_to_jsonb` preserves JSON-compatible structure and exact unsigned
  integers. Binary values become `["~b", "<base64>", "base64"]`; non-finite
  floats become `"NaN"`, `"Infinity"`, or `"-Infinity"`.
- SQL decoding accepts one complete MessagePack value with unique string map
  keys. Extension markers, duplicate/non-string keys, NUL strings, malformed
  input, and trailing bytes are rejected.
- `flexbuffers_to_jsonb` verifies the complete FlexBuffer before decoding.
  Blobs and non-finite floats use the same JSONB conventions as MessagePack.

## Fast Paths

Schema metadata, converter selection, protocol keys, and map headers are cached
per PostgreSQL backend. Flat supported schemas use protocol-specific direct
writers. Schemas containing recursive composites use the generic dynamic tree
path to preserve nested structure.

The following test helpers force MessagePack's generic path for byte-parity
checks:

- `row_to_msgpack_slow(record)`
- `rows_to_msgpack_slow(anyarray)`

## Benchmarking

```bash
make bench
make bench-isolated
PROTOCOLS="msgpack flex" RUNS=10 WARMUP=3 make bench-isolated
```

See [`bench/README.md`](bench/README.md) for workloads, connection settings, and
result format. Benchmark output under `results/` is intentionally untracked.

## Current Limitations

- Deserialization currently targets JSONB and is available for MessagePack and
  FlexBuffer; CBOR and ZERA decoding are not yet exposed to SQL.
- Arbitrary-precision decimal values do not have an exact portable wire type.
- JSON text is not recursively parsed; use JSONB builders when nested JSON
  semantics are required.

## Maintained Documentation

- [`QUICKSTART.md`](QUICKSTART.md): installation and common SQL examples
- [`ARCHITECTURE.md`](ARCHITECTURE.md): conversion paths, caching, and semantics
- [`bench/README.md`](bench/README.md): repeatable benchmark harness
- [`vendor/zerialize/UPSTREAM.md`](vendor/zerialize/UPSTREAM.md): vendored source
  provenance
