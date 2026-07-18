# Architecture

## Overview

`pg_zerialize` is a PostgreSQL C extension implemented in C++20. SQL-callable
entry points use C linkage and convert PostgreSQL `Datum` values into
MessagePack, CBOR, ZERA, or FlexBuffers documents.

The extension has two serialization paths:

1. Protocol-specific direct writers for supported flat schemas.
2. A generic `zerialize::dyn::Value` tree for recursive composites and fallback
   cases.

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

A schema containing a composite column is deliberately excluded from the flat
fast path. It uses recursive dynamic conversion so nested maps remain correct.

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
| one-dimensional array | protocol array |
| named composite | protocol map |
| null | protocol null |

Multidimensional row arrays currently fall back to PostgreSQL text. Batch APIs
reject multidimensional arrays because their outer array is reserved for rows.

## Nested JSONB

Row-level `jsonb` remains an opaque PostgreSQL binary payload for compatibility.
`msgpack_from_jsonb` is a separate semantic API that recursively maps JSONB
objects, arrays, scalars, and nulls into MessagePack.

## Numeric Conversion

`numeric_out` produces PostgreSQL's canonical decimal text once. Integral text
is parsed with `std::from_chars`; other values use fast_float by default and
fall back to `numeric_float8` for special or rejected values. The GUC
`pg_zerialize.numeric_float_backend` selects `fast_float` or `postgres`.

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

## Remaining Work

- Nested protocol arrays for multidimensional PostgreSQL arrays
- Deserialization APIs
- An explicit exact-decimal wire policy
- Optional direct recursive writers if real nested workloads justify them
