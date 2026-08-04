# Architecture

## Overview

`pg_zerialize` is a PostgreSQL C extension implemented in C++20. SQL-callable
entry points use C linkage and convert PostgreSQL `Datum` values into
MessagePack, CBOR, ZERA, or FlexBuffers documents.

The extension has two serialization paths:

1. Protocol-specific direct writers for supported schemas, recursing into
   nested composites and composite arrays.
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

All four protocols additionally expose JSONB conversion, variadic builders,
and aggregates (`X_to_jsonb`, `X_from_jsonb`, `X_build_object`,
`X_build_array`, `X_agg`, `X_object_agg`). MessagePack's were introduced in
`pg_zerialize--1.2.sql`; CBOR/ZERA/FlexBuffers' were added in
`pg_zerialize--1.7.sql`, mirroring the same design.

`X_populate_record(base anyelement, data bytea)`, added in
`pg_zerialize--1.8.sql`, is the reverse of `row_to_X`: it decodes a binary
document directly into a typed composite, following `jsonb_populate_record`'s
own anyelement/base-row polymorphism. See "Composite Decode Path" below.

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

All four protocols recursively apply cached writer plans to composite columns
and one-dimensional composite arrays: writing a composite value calls the same
protocol writer against that value's own cached schema, to any nesting depth.
A recursive capability check (one per protocol, since each protocol tracks its
own `*_fast_supported` flag) walks the composite type graph — with cycle
guarding, though PostgreSQL composite types cannot actually embed themselves
— and runs only for schemas containing composite-typed columns; a descendant
schema with an unsupported column (recursively) fails the check and the whole
value falls back to the dynamic tree before any output is written.

Array columns are declared with a fixed element type but no fixed
dimensionality — any array-typed column can hold an N-dimensional value at
runtime — so multidimensional arrays are handled per-value rather than
gated by the schema cache. All four protocols write these directly too: a
recursive per-dimension writer walks `ARR_DIMS` and the flat
`deconstruct_array` output emitting nested `begin_array`/`end_array` pairs,
reusing the same per-element-kind writer (and its composite recursion) the
one-dimensional path uses at the innermost level. An array whose element
kind has no direct writer still falls back to the dynamic tree, same as the
one-dimensional case.

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
`X_from_jsonb` is a separate semantic API that recursively maps JSONB objects,
arrays, scalars, and nulls into the target protocol. The JSONB-to-`dyn::Value`
walk (`jsonb_to_dynamic`) is protocol-agnostic; only the final
`z::serialize<Protocol>` call differs, so `msgpack_from_jsonb`,
`cbor_from_jsonb`, `zera_from_jsonb`, and `flexbuffers_from_jsonb` share one
implementation via `dynamic_to_binary<Protocol>`. The builder
(`X_build_object`/`X_build_array`) and aggregate (`X_agg`/`X_object_agg`)
functions follow the same pattern: build or reuse a `dyn::Value` tree, then
call `dynamic_to_binary<Protocol>` once at the end.

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

## Composite Decode Path

`X_populate_record` reuses each protocol's existing, already-validated
`X_to_jsonb` decode logic rather than parsing the wire format a second time:
its decode-to-JSON-text core (`msgpack_decode_to_json_text`,
`cbor_decode_to_json_text`, `zera_decode_to_json_text`,
`flex_decode_to_json_text`) is the same code `X_to_jsonb` calls, split out so
both entry points share one parsing/validation path per protocol. The
resulting `Jsonb*` is then walked by one protocol-agnostic function,
`populate_composite_from_jsonb_container`, using the same
`CachedSchema`/`CachedColumn` metadata the encode side builds.

For each column, the walk looks up the matching JSONB object key
(`getKeyJsonValueFromContainer`) and dispatches on `CachedColumn::kind`:
composite columns recurse into a nested `populate_composite_from_jsonb_container`
call; array columns walk dimensions recursively (`jsonb_array_dim_collect`,
mirroring the encode side's dimension walker in reverse) and reassemble via
`construct_md_array`; everything else goes through
`jsonb_value_to_scalar_datum`. A key missing from the document leaves that
column's slot untouched, which is prefilled from `base` via
`heap_deform_tuple` (or left NULL when there is no `base` row) before the
walk starts — this is what produces `jsonb_populate_record`-style partial
updates. A key present with a JSON `null` clears the column explicitly.

`jsonb_value_to_scalar_datum` mirrors the encode side's per-type wire
conventions in reverse, gated on the target column's type so a coincidental
JSON shape in an unrelated column is never misread as one of these
conventions:

- `bytea` and row-level `jsonb`: reconstructed directly from the `["~b", "<base64>", "base64"]`
  tag with one palloc+memcpy (the reverse of `datum_bytea_span`/`datum_jsonb_span`),
  no `byteain`/`jsonb_in` text round trip.
- `date`/`timestamp`/`timestamptz`: every write path encodes these as a raw
  PG-internal epoch integer, not text (see Type Semantics above), so a
  JSON-number value for one of these columns is reconstructed directly via
  `DateADTGetDatum`/`TimestampGetDatum`/`TimestampTzGetDatum` rather than
  routed through `date_in`/`timestamp_in`, which expect human-readable text.
  A hand-built document that supplies a text date for these columns still
  works, since a JSON-string value always falls through to the generic path
  below.
- `numeric` in `tagged_decimal` mode: the `["~n", "<canonical text>", "decimal"]`
  tag's payload is already `numeric_out`-formatted text, so it is handed
  straight to `numeric`'s input function.
- Everything else: `getTypeInputInfo` + `OidInputFunctionCall` on the
  column's own `typinput`, the same generic mechanism
  `jsonb_populate_record` itself uses. This transparently enforces domain
  `CHECK` constraints too, since a domain's `typinput` is `domain_in`.

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
