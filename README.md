# pg_zerialize

PostgreSQL extension for serializing rows and batches of rows to MessagePack,
CBOR, ZERA, FlexBuffers, Ion, and BSON. Each protocol also includes SQL
builders and aggregates for constructing nested binary values from scratch
(BSON's builder API is a reduced subset -- see Current Limitations).

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
- MessagePack C development package (`libmsgpack-dev`)
- jsoncons development package (`libjsoncons-dev`) -- used by BSON's writer
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

All four protocols provide JSON-style builders and aggregates
(`msgpack_*`/`cbor_*`/`zera_*`/`flexbuffers_*`), for composing a document from
scratch rather than serializing an existing composite row:

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
SELECT cbor_to_jsonb(row_to_cbor(users.*)) FROM users;
SELECT zera_to_jsonb(row_to_zera(users.*)) FROM users;

-- Same builder API for CBOR, ZERA, and FlexBuffers:
SELECT cbor_build_object('id', 7, 'active', true);
SELECT zera_build_array(1, 'two', NULL, 3.5::numeric);
SELECT flexbuffers_agg(value ORDER BY id) FROM items;
SELECT cbor_object_agg(key, value ORDER BY key) FROM items;
SELECT zera_from_jsonb(jsonb_build_object('id', 7, 'active', true));
```

Passing a builder's `bytea` result into another builder of the same protocol
encodes that result as a binary blob. Use one JSONB tree with
`msgpack_from_jsonb`/`cbor_from_jsonb`/`zera_from_jsonb`/`flexbuffers_from_jsonb`
when values must be spliced into one nested document instead.

## Decoding Into Composite Rows

`X_populate_record(base anyelement, data bytea)` is the reverse of `row_to_X`:
it decodes a binary document straight into a typed composite, for all four
protocols. It follows `jsonb_populate_record`'s own anyelement/base-row
polymorphism: columns the document omits keep `base`'s value, an explicit
JSON null clears a column, and `base` can be `NULL::sometype` to select the
result type without supplying defaults. `data = NULL` returns `base`
unchanged (or an all-NULL composite of `base`'s type if `base` is also
`NULL::sometype`).

```sql
CREATE TYPE employee AS (id int, name text, active boolean);

SELECT msgpack_populate_record(NULL::employee, row_to_msgpack(ROW(1, 'Ada', true)::employee));
SELECT cbor_populate_record(ROW(1, 'Ada', true)::employee, cbor_build_object('active', false));
SELECT zera_populate_record(NULL::employee, zera_from_jsonb('{"id":2,"name":"Grace"}'::jsonb));
```

Nested composites, arrays of any dimension (including arrays of composites),
and the `["~b", ...]`/`["~n", ...]` tagged values described below all decode
back to their original PostgreSQL types; a domain column is validated through
the domain's own input function, so `CHECK` constraints are enforced.

`X_populate_recordset(base anyelement, data bytea)` is the batch counterpart,
the reverse of `rows_to_X`: it decodes a binary array of documents into a
`SETOF` typed composites, mirroring `jsonb_populate_recordset`. The same
`base` row supplies the fallback for every document in the array. `data =
NULL` yields an empty result set rather than a single unchanged row, since
there is no singular row to return unchanged.

```sql
SELECT * FROM msgpack_populate_recordset(
  NULL::employee,
  rows_to_msgpack(ARRAY[ROW(1, 'Ada', true), ROW(2, 'Grace', true)]::employee[])
);
```

## Wire Semantics

- `int2`, `int4`, and `int8` are protocol integers.
- `float4` and `float8` are protocol floating-point values.
- Integral `numeric` values fitting in signed 64 bits are exact integers. Other
  `numeric` values are `float64` and may lose decimal precision.
- Set `pg_zerialize.numeric_encoding = 'tagged_decimal'` to preserve every
  `numeric` exactly as `["~n", "<canonical text>", "decimal"]` in all four
  protocols. The default is `float64` for wire compatibility.
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
- `cbor_to_jsonb` strictly parses definite and indefinite CBOR containers.
  Semantic tags, duplicate/non-string map keys, unsupported simple values,
  malformed input, and trailing bytes are rejected.
- `zera_to_jsonb` validates the ZERA v1 header and recursively validates every
  referenced value. Cycles, duplicate keys, corrupt offsets/shapes, and non-U8
  typed arrays are rejected.

## Fast Paths

Schema metadata, converter selection, protocol keys, and map headers are cached
per PostgreSQL backend. Flat supported schemas use protocol-specific direct
writers. MessagePack, CBOR, ZERA, and FlexBuffers also directly write nested
composite fields, one-dimensional composite arrays, and multidimensional
arrays of any directly-writable element type, recursing through the same
cached writer plans at every level; schemas with a descendant column that has
no direct writer (recursively) fall back to the generic dynamic tree.

Ion and BSON don't have a direct writer yet -- every `row_to_ion`/
`row_to_bson`/`rows_to_ion` call goes through the same generic dynamic-tree
path those other four fall back to. Their `X_to_jsonb`/`X_populate_record(set)`
read side is likewise generic: rather than a hand-rolled wire-byte parser per
format (what MessagePack/CBOR/ZERA/FlexBuffers each have), they decode
through zerialize's own `Reader`/`Deserializer` interface via one shared
walker. Both are correct and fully tested, just not yet performance-tuned to
the same degree as the original four.

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

- Deserialization targets JSONB (`X_to_jsonb`) and typed composites, single
  (`X_populate_record`) or batch (`X_populate_recordset`), for MessagePack,
  CBOR, ZERA, FlexBuffers, and Ion. BSON has `X_to_jsonb`/`X_populate_record`
  but not `X_populate_recordset` -- see below.
- Exact decimals use an opt-in tagged-array convention rather than a native
  protocol scalar, so non-pg_zerialize consumers must interpret that tag.
- JSON text is not recursively parsed; use JSONB builders when nested JSON
  semantics are required.
- **BSON has no bare-array-root support.** A BSON document and a BSON array
  are physically identical on the wire -- only a *parent* element's header
  records which one a value is, and the root has no parent, so that
  information doesn't exist to recover on read (confirmed against jsoncons'
  own `bson_parser`, which has the same behavior). Concretely: `bson_to_jsonb`
  always decodes an unannotated root as a JSON object, never an array.
  Because of this, `bson_build_array`, `bson_agg`, `rows_to_bson`, and
  `bson_populate_recordset` are not provided -- each would only ever produce
  or require a bare-array-root document. Arrays nested anywhere below the
  root (inside a document) are unaffected and round-trip normally; use
  `bson_build_object`/`bson_object_agg`/`row_to_bson`/`bson_populate_record`
  instead. See [`vendor/zerialize/include/zerialize/protocols/BSON.md`](vendor/zerialize/include/zerialize/protocols/BSON.md).
- **Ion decodes only the first top-level value in a stream.** Unlike the
  other five protocols here (one value per `bytea`, trailing bytes rejected
  as corruption), Ion's own wire format legitimately allows multiple
  top-level values back to back; `ion_to_jsonb`/`ion_populate_record(set)`
  take the first one and don't treat what follows as an error, matching
  standard Ion reader semantics.

## Maintained Documentation

- [`QUICKSTART.md`](QUICKSTART.md): installation and common SQL examples
- [`ARCHITECTURE.md`](ARCHITECTURE.md): conversion paths, caching, and semantics
- [`bench/README.md`](bench/README.md): repeatable benchmark harness
- [`vendor/zerialize/UPSTREAM.md`](vendor/zerialize/UPSTREAM.md): vendored source
  provenance
