# pg_zerialize

PostgreSQL extension for converting rows to efficient binary formats using the [zerialize](https://github.com/colinator/zerialize) library.

## Status: All Formats Complete! 🎉

✅ **FlexBuffers** - Fully implemented
✅ **MessagePack** - Fully implemented
✅ **CBOR** - Fully implemented
✅ **ZERA** - Fully implemented

## Requirements

- PostgreSQL 12+ (with development headers)
- C++20 compatible compiler (GCC 10+, Clang 10+)
- FlatBuffers library (`libflatbuffers-dev`)
- MessagePack C library (`libmsgpack-c-dev`)
- jsoncons library (`libjsoncons-dev`)
- zerialize library (header-only, included in `vendor/`)

## Building

```bash
make
sudo make install
```

## Usage

```sql
CREATE EXTENSION pg_zerialize;

-- Convert a row to any of the four binary formats
SELECT row_to_msgpack(ROW('John', 25, true));
SELECT row_to_cbor(ROW('John', 25, true));
SELECT row_to_zera(ROW('John', 25, true));
SELECT row_to_flexbuffers(ROW('John', 25, true));

-- Compare sizes across all formats
SELECT
    octet_length(row_to_msgpack(users.*)) as msgpack_bytes,
    octet_length(row_to_cbor(users.*)) as cbor_bytes,
    octet_length(row_to_zera(users.*)) as zera_bytes,
    octet_length(row_to_flexbuffers(users.*)) as flexbuffers_bytes,
    octet_length(row_to_json(users.*)::text::bytea) as json_bytes
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

## Next Steps

1. ✅ ~~Implement FlexBuffers support~~
2. ✅ ~~Implement MessagePack support~~
3. ✅ ~~Implement CBOR support~~
4. ✅ ~~Implement ZERA support~~
5. Add array support for PostgreSQL arrays
6. Add proper NUMERIC/DECIMAL handling
7. Add nested composite type support
8. Add date/timestamp types
9. Add deserialization functions
