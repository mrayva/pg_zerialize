# pg_zerialize

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-blue.svg)](https://www.postgresql.org/)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)

PostgreSQL extension for converting rows to efficient binary formats using the [zerialize](https://github.com/colinator/zerialize) library.

**21% smaller than JSON** with MessagePack/CBOR formats.

## Status: All Formats Complete! 🎉

✅ **FlexBuffers** - Fully implemented
✅ **MessagePack** - Fully implemented
✅ **CBOR** - Fully implemented
✅ **ZERA** - Fully implemented

## Requirements

- PostgreSQL 12+ (with development headers)
- C++20 compatible compiler (GCC 10+, Clang 10+)
- FlatBuffers library (`libflatbuffers-dev`)
- zerialize library (header-only, included in `vendor/`)

## Building

```bash
make
sudo make install
```

## Testing

```bash
# PGXS regression tests (core/parity/cache/determinism)
make installcheck

# Comprehensive manual suite
psql -d postgres -f test_pg_zerialize.sql
```

For MessagePack fast-path parity checks, test-only helper functions are available:
- `row_to_msgpack_slow(record)`
- `rows_to_msgpack_slow(anyarray)`

## Microbenchmark

Run the repeatable in-repo microbenchmark harness:

```bash
make bench
```

Quick/short run:

```bash
make bench-quick
```

Benchmark docs and output format:
- `bench/README.md`
- output logs in `results/microbench_*.out`

## Usage

### Single Record Serialization

```sql
CREATE EXTENSION pg_zerialize;

-- Convert a single row to any of the four binary formats
SELECT row_to_msgpack(ROW('John', 25, true));
SELECT row_to_cbor(ROW('John', 25, true));
SELECT row_to_zera(ROW('John', 25, true));
SELECT row_to_flexbuffers(ROW('John', 25, true));

-- Serialize table rows individually
SELECT row_to_msgpack(users.*) FROM users;
```

### Batch Processing (Faster for Multiple Rows)

```sql
-- Serialize multiple rows in a single call (2-3x faster!)
SELECT rows_to_msgpack(array_agg(users.*)) FROM users;
SELECT rows_to_cbor(array_agg(users.*)) FROM users;
SELECT rows_to_zera(array_agg(users.*)) FROM users;
SELECT rows_to_flexbuffers(array_agg(users.*)) FROM users;

-- Compare sizes across all formats
SELECT
    octet_length(rows_to_msgpack(array_agg(users.*))) as msgpack_bytes,
    octet_length(rows_to_cbor(array_agg(users.*))) as cbor_bytes,
    octet_length(rows_to_zera(array_agg(users.*))) as zera_bytes,
    octet_length(rows_to_flexbuffers(array_agg(users.*))) as flexbuffers_bytes
FROM users;
```

## Performance

Based on real-world testing with user records (5 rows average):

| Format      | Avg Size | vs JSON | Best For |
|-------------|----------|---------|----------|
| MessagePack | 71 bytes | **-21%** 🥇 | Max compression, APIs, caching |
| CBOR        | 71 bytes | **-21%** 🥈 | IoT, IETF standard (RFC 8949) |
| JSON        | 90 bytes | baseline | Human-readable, debugging |
| FlexBuffers | 142 bytes | +58% | Zero-copy reads, lazy access |
| ZERA        | 209 bytes | +132% | Zerialize ecosystem, advanced features |

**MessagePack** and **CBOR** are the most compact, both saving ~21% vs JSON.
**FlexBuffers** trades size for zero-copy deserialization capability.
**ZERA** includes additional structure for advanced features but is larger.

## Performance Optimizations

All major performance optimizations complete! **Combined speedup: ~3-5x faster than original!**

✅ **Schema Caching** - TupleDesc lookups cached, **20-30% faster** bulk operations

✅ **Batch Processing** - Multiple rows in single call, **2-3x faster** for bulk operations

✅ **Buffer Pre-allocation** - Map/array capacity reserved upfront, **5-10% faster** with reduced memory fragmentation

## Next Steps

1. ✅ ~~Implement FlexBuffers support~~
2. ✅ ~~Implement MessagePack support~~
3. ✅ ~~Implement CBOR support~~
4. ✅ ~~Implement ZERA support~~
5. ✅ ~~Add array support for PostgreSQL arrays~~
6. ✅ ~~Add proper NUMERIC/DECIMAL handling~~
7. ✅ ~~Schema caching optimization~~
8. ✅ ~~Batch processing for multiple rows~~
9. ✅ ~~Buffer pre-allocation optimization~~
10. Add nested composite type support
11. Add date/timestamp types
12. Add deserialization functions
