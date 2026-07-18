# Architecture

## Overview

`pg_zerialize` is a PostgreSQL C extension implemented in C++20. SQL-callable
entry points use C linkage and convert PostgreSQL `Datum` values into
MessagePack, CBOR, ZERA, or FlexBuffers documents.

The extension has two serialization paths:

1. Protocol-specific direct writers for supported schemas; MessagePack also
   writes nested composites and composite arrays recursively.
2. A generic `zerialize::dyn::Value` tree for unsupported recursive and
   fallback cases.

Both paths produce the same documented data semantics.

## Entry Points

Single-row functions accept `record` and return one map as `bytea`:

- `row_to_msgpack`
- `row_to_cbor`
- `row_to_zera`
- `row_to_flexbuffers`

Batch functions accept a one-dimensional array of composite records and return
one protocol array:

- `rows_to_msgpack`
- `rows_to_cbor`
- `rows_to_zera`
- `rows_to_flexbuffers`

MessagePack additionally exposes JSONB conversion, variadic builders, and
aggregates. Their SQL definitions are versioned in `pg_zerialize--1.2.sql`.

## Schema Cache

Each PostgreSQL backend maintains schema metadata keyed by composite type OID
and typmod. A cached schema owns:

- a copied `TupleDesc`
- non-dropped attribute metadata
- converter kinds and output function OIDs
- protocol-specific scalar and array writer plans
- preencoded MessagePack and ZERA keys
- a preencoded MessagePack map header
- the selected tuple access strategy

Catalog and relation cache callbacks clear this state after relevant DDL. Wide
schemas use `heap_deform_tuple`; narrow schemas use `heap_getattr`.

## Direct Path

Flat schemas composed of supported scalar and one-dimensional array types use
protocol-specific writers. This avoids building an intermediate dynamic tree.
MessagePack additionally reuses a backend-local output buffer and directly
encodes canonical headers and scalar values.

MessagePack recursively applies cached writer plans to composite columns and
one-dimensional composite arrays. A recursive capability check runs only for
schemas containing those columns; unsupported descendants fall back before any
output is written. Other protocols continue to use recursive dynamic
conversion for nested composites.

## Dynamic Path

The generic path performs this conversion:

```text
HeapTupleHeader
  -> cached TupleDesc and attribute values
  -> zerialize::dyn::Value map
  -> protocol serializer
  -> PostgreSQL bytea
```

`datum_to_dynamic` recursively handles arrays and named composite values.
Composite attributes call `record_to_dynamic_map`, producing nested protocol
maps with the nested type's attribute names.

## Type Semantics

| PostgreSQL type | Dynamic/wire representation |
| --- | --- |
| `int2`, `int4`, `int8` | signed integer |
| `float4`, `float8` | floating point |
| `boolean` | boolean |
| text types, `json` | string |
| integral `numeric` fitting in `int64` | signed integer |
| other `numeric` | `float64` |
| date | days since PostgreSQL epoch |
| timestamp/timestamptz | microseconds since PostgreSQL epoch |
| `bytea`, row-level `jsonb` | binary payload |
| UUID, enum, name, char, inet/cidr, interval | canonical text |
| PostgreSQL array | nested protocol arrays (lower bounds discarded) |
| named composite | protocol map |
| null | protocol null |

Multidimensional row fields preserve their shape as nested protocol arrays.
Batch APIs reject multidimensional outer arrays because that array is reserved
for rows.

## Nested JSONB

Row-level `jsonb` remains an opaque PostgreSQL binary payload for compatibility.
`msgpack_from_jsonb` is a separate semantic API that recursively maps JSONB
objects, arrays, scalars, and nulls into MessagePack.

`msgpack_to_jsonb` validates one complete MessagePack value before invoking the
vendored reader, then recursively maps it to JSONB. MessagePack binary values
use zerialize's tagged base64 JSON convention.

`flexbuffers_to_jsonb` applies FlatBuffers' recursive FlexBuffer verifier before
walking raw references by index. This avoids unchecked offsets and does not
depend on binary-search lookup for hostile map key ordering.

`cbor_to_jsonb` uses a bounded recursive CBOR parser instead of the vendored
reader's unchecked iterator helpers. It accepts definite and indefinite
containers but rejects semantic tags because their JSONB mapping is ambiguous.

`zera_to_jsonb` validates the v1 header, zero padding, envelope graph, arena
spans, map metadata, and U8 blob shapes. Active-reference tracking rejects
cycles before recursive decoding.

## Numeric Conversion

`numeric_out` produces PostgreSQL's canonical decimal text once. Integral text
is parsed with `std::from_chars`; other values use fast_float by default and
fall back to `numeric_float8` for special or rejected values. The GUC
`pg_zerialize.numeric_float_backend` selects `fast_float` or `postgres`.

The default `pg_zerialize.numeric_encoding = 'float64'` retains this behavior.
The opt-in `tagged_decimal` mode emits every `numeric` as
`["~n","<numeric_out text>","decimal"]` across all protocols, preserving
precision, display scale, and special values without changing default wire
compatibility.

## Memory Management

- Returned `bytea` values use PostgreSQL `palloc`.
- C++ containers and zerialize buffers use RAII.
- MessagePack reuses a backend-local malloc buffer for single-row and batch fast
  paths, then copies the completed payload into its returned `bytea`.
- Schema cache entries live until invalidation or backend exit.

## Testing

PGXS regression suites cover core behavior, fast/slow parity, cache
invalidation, deterministic output, builders, semantics, and extension
upgrades. `test/semantic_roundtrip.py` independently decodes MessagePack and
asserts data-level meaning.

```bash
make installcheck
make semantic-check
```

The isolated benchmark harness runs each protocol in a separate `psql` session
to avoid cross-protocol cache and allocator effects. See `bench/README.md`.
