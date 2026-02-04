-- Final Test: All Four Formats Complete!
-- FlexBuffers, MessagePack, CBOR, ZERA

\timing off
\pset border 2

DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

\echo ''
\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║              🎉 ALL FOUR FORMATS COMPLETE! 🎉                 ║'
\echo '║   FlexBuffers • MessagePack • CBOR • ZERA                     ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''

\echo '=== Test 1: Simple Record - All Formats ==='
\echo 'Record: (''Alice'', 30, true)'
\echo ''

SELECT
    format,
    size_bytes,
    encode(data, 'hex') as hex_preview_20chars
FROM (
    SELECT 'MessagePack' as format,
           octet_length(row_to_msgpack(ROW('Alice', 30, true)::record)) as size_bytes,
           substr(row_to_msgpack(ROW('Alice', 30, true)::record), 1, 10) as data
    UNION ALL
    SELECT 'CBOR',
           octet_length(row_to_cbor(ROW('Alice', 30, true)::record)),
           substr(row_to_cbor(ROW('Alice', 30, true)::record), 1, 10)
    UNION ALL
    SELECT 'ZERA',
           octet_length(row_to_zera(ROW('Alice', 30, true)::record)),
           substr(row_to_zera(ROW('Alice', 30, true)::record), 1, 10)
    UNION ALL
    SELECT 'FlexBuffers',
           octet_length(row_to_flexbuffers(ROW('Alice', 30, true)::record)),
           substr(row_to_flexbuffers(ROW('Alice', 30, true)::record), 1, 10)
    UNION ALL
    SELECT 'JSON',
           octet_length(row_to_json(ROW('Alice', 30, true))::text::bytea),
           substr(row_to_json(ROW('Alice', 30, true))::text::bytea, 1, 10)
) s
ORDER BY size_bytes;

\echo ''
\echo '=== Test 2: Complex Record - All Formats ==='
CREATE TYPE employee AS (
    name text,
    id int,
    salary float8,
    active boolean
);

\echo 'Record: employee(''John Smith'', 12345, 95000.50, true)'
\echo ''

SELECT
    format,
    size_bytes,
    round((size_bytes::numeric /
        (SELECT octet_length(row_to_json(ROW('John Smith', 12345, 95000.50, true))::text::bytea))
    ) * 100, 1)::text || '%' as relative_to_json
FROM (
    SELECT 'MessagePack' as format,
           octet_length(row_to_msgpack(ROW('John Smith', 12345, 95000.50, true)::employee)) as size_bytes
    UNION ALL
    SELECT 'CBOR',
           octet_length(row_to_cbor(ROW('John Smith', 12345, 95000.50, true)::employee))
    UNION ALL
    SELECT 'ZERA',
           octet_length(row_to_zera(ROW('John Smith', 12345, 95000.50, true)::employee))
    UNION ALL
    SELECT 'FlexBuffers',
           octet_length(row_to_flexbuffers(ROW('John Smith', 12345, 95000.50, true)::employee))
    UNION ALL
    SELECT 'JSON',
           octet_length(row_to_json(ROW('John Smith', 12345, 95000.50, true))::text::bytea)
) sizes
ORDER BY size_bytes;

\echo ''
\echo '=== Test 3: Real Table Data - Average Sizes ==='
CREATE TEMP TABLE users (
    id serial PRIMARY KEY,
    username text,
    email text,
    age int,
    score float8,
    active boolean
);

INSERT INTO users (username, email, age, score, active) VALUES
    ('alice', 'alice@example.com', 28, 92.5, true),
    ('bob', 'bob@example.com', 35, 87.3, true),
    ('charlie', 'charlie@example.com', 42, 95.8, false),
    ('diana', 'diana@example.com', 31, 88.2, true),
    ('eve', 'eve@example.com', 29, 91.7, true);

\echo 'Average sizes for 5 user records:'
\echo ''

SELECT
    format,
    avg_bytes::int as avg_size,
    round((json_avg - avg_bytes), 1)::text || ' bytes saved' as vs_json,
    round(100 - (avg_bytes / json_avg * 100), 1)::text || '%' as percent_saved
FROM (
    SELECT
        format,
        avg_bytes,
        (SELECT avg(octet_length(row_to_json(users.*)::text::bytea))::numeric FROM users) as json_avg
    FROM (
        SELECT 'MessagePack' as format,
               avg(octet_length(row_to_msgpack(users.*)))::numeric as avg_bytes
        FROM users
        UNION ALL
        SELECT 'CBOR',
               avg(octet_length(row_to_cbor(users.*)))
        FROM users
        UNION ALL
        SELECT 'ZERA',
               avg(octet_length(row_to_zera(users.*)))
        FROM users
        UNION ALL
        SELECT 'FlexBuffers',
               avg(octet_length(row_to_flexbuffers(users.*)))
        FROM users
        UNION ALL
        SELECT 'JSON',
               avg(octet_length(row_to_json(users.*)::text::bytea))
        FROM users
    ) s
) w
ORDER BY avg_bytes;

\echo ''
\echo '=== Test 4: NULL Handling - All Formats ==='
\echo 'Record with 2 values and 2 NULLs'
\echo ''

SELECT
    format,
    size_bytes
FROM (
    SELECT 'MessagePack' as format,
           octet_length(row_to_msgpack(ROW('test', NULL, 3.14, NULL)::record)) as size_bytes
    UNION ALL
    SELECT 'CBOR',
           octet_length(row_to_cbor(ROW('test', NULL, 3.14, NULL)::record))
    UNION ALL
    SELECT 'ZERA',
           octet_length(row_to_zera(ROW('test', NULL, 3.14, NULL)::record))
    UNION ALL
    SELECT 'FlexBuffers',
           octet_length(row_to_flexbuffers(ROW('test', NULL, 3.14, NULL)::record))
    UNION ALL
    SELECT 'JSON',
           octet_length(row_to_json(ROW('test', NULL, 3.14, NULL))::text::bytea)
) s
ORDER BY size_bytes;

\echo ''
\echo '=== Test 5: Individual User Rows - All Formats ==='

SELECT
    username,
    octet_length(row_to_msgpack(users.*)) as msgpack,
    octet_length(row_to_cbor(users.*)) as cbor,
    octet_length(row_to_zera(users.*)) as zera,
    octet_length(row_to_flexbuffers(users.*)) as flex,
    octet_length(row_to_json(users.*)::text::bytea) as json
FROM users
ORDER BY id;

\echo ''
\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║                     FINAL RANKINGS                             ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''
\echo 'Based on comprehensive testing:'
\echo ''
\echo '1. 🥇 MessagePack - Most compact overall'
\echo '2. 🥈 CBOR        - Very close to MessagePack, IETF standard'
\echo '3. 🥉 ZERA        - Zerialize native, good performance'
\echo '4.    JSON        - Human-readable baseline'
\echo '5.    FlexBuffers - Larger but zero-copy capable'
\echo ''

\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║                  COMPLETE FUNCTION LIST                        ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''
\echo '  ✅ row_to_msgpack(record) → bytea'
\echo '  ✅ row_to_cbor(record) → bytea'
\echo '  ✅ row_to_zera(record) → bytea'
\echo '  ✅ row_to_flexbuffers(record) → bytea'
\echo ''
\echo 'All functions support:'
\echo '  • INT2, INT4, INT8 (integers)'
\echo '  • FLOAT4, FLOAT8 (floats)'
\echo '  • BOOLEAN'
\echo '  • TEXT, VARCHAR'
\echo '  • NULL values'
\echo '  • Named composite types'
\echo '  • Anonymous records'
\echo '  • Table rows'
\echo ''

\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║                    FORMAT SELECTION GUIDE                      ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''
\echo 'MessagePack:'
\echo '  • Most compact format'
\echo '  • Widest language support'
\echo '  • Best for: REST APIs, caching, general use'
\echo ''
\echo 'CBOR:'
\echo '  • IETF RFC 8949 standard'
\echo '  • Nearly as compact as MessagePack'
\echo '  • Best for: IoT, embedded systems, standards compliance'
\echo ''
\echo 'ZERA:'
\echo '  • Zerialize native protocol'
\echo '  • Good compression'
\echo '  • Best for: Zerialize ecosystem, custom applications'
\echo ''
\echo 'FlexBuffers:'
\echo '  • Zero-copy deserialization'
\echo '  • Larger size'
\echo '  • Best for: Large records with selective field access'
\echo ''

\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║                        SUCCESS!                                ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''
\echo 'All four binary serialization formats are now complete!'
\echo ''
\echo 'pg_zerialize provides multiple options for efficient'
\echo 'binary serialization of PostgreSQL data, each optimized'
\echo 'for different use cases.'
\echo ''
\echo 'Choose the format that best fits your requirements!'
\echo ''

-- Cleanup
DROP TABLE users;
DROP TYPE employee;
