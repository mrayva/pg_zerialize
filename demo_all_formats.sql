-- Comprehensive Demo: All Binary Formats in pg_zerialize
-- MessagePack vs CBOR vs FlexBuffers vs JSON

\timing off
\pset border 2

DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

\echo ''
\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║          pg_zerialize - Complete Format Comparison            ║'
\echo '║       MessagePack • CBOR • FlexBuffers vs JSON                ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''

\echo '=== Format Overview ==='
\echo ''
\echo 'MessagePack: Binary format, widely supported, compact'
\echo 'CBOR:        IETF standard (RFC 8949), IoT/embedded systems'
\echo 'FlexBuffers: Google FlatBuffers, zero-copy deserialization'
\echo 'JSON:        Text format, human-readable, baseline'
\echo ''

\echo '=== Test 1: Tiny Record ==='
\echo 'Record: (123, true)'
\echo ''

SELECT
    format,
    size_bytes,
    CASE WHEN format = 'JSON'
        THEN '(baseline)'
        ELSE (json_size - size_bytes)::text || ' bytes saved'
    END as vs_json
FROM (
    SELECT
        format,
        size_bytes,
        (SELECT octet_length(row_to_json(ROW(123, true))::text::bytea)) as json_size
    FROM (
        SELECT 'MessagePack' as format,
               octet_length(row_to_msgpack(ROW(123, true)::record)) as size_bytes
        UNION ALL
        SELECT 'CBOR',
               octet_length(row_to_cbor(ROW(123, true)::record))
        UNION ALL
        SELECT 'FlexBuffers',
               octet_length(row_to_flexbuffers(ROW(123, true)::record))
        UNION ALL
        SELECT 'JSON',
               octet_length(row_to_json(ROW(123, true))::text::bytea)
    ) s
) w
ORDER BY size_bytes;

\echo ''
\echo '=== Test 2: Text-Heavy Record ==='
\echo 'Record: (''Alice Johnson'', ''alice@example.com'', ''Software Engineer'')'
\echo ''

SELECT
    format,
    size_bytes,
    round((size_bytes::numeric /
        (SELECT octet_length(row_to_json(
            ROW('Alice Johnson', 'alice@example.com', 'Software Engineer')
        )::text::bytea))
    ) * 100, 1)::text || '%' as relative_size
FROM (
    SELECT 'MessagePack' as format,
           octet_length(row_to_msgpack(
               ROW('Alice Johnson', 'alice@example.com', 'Software Engineer')::record
           )) as size_bytes
    UNION ALL
    SELECT 'CBOR',
           octet_length(row_to_cbor(
               ROW('Alice Johnson', 'alice@example.com', 'Software Engineer')::record
           ))
    UNION ALL
    SELECT 'FlexBuffers',
           octet_length(row_to_flexbuffers(
               ROW('Alice Johnson', 'alice@example.com', 'Software Engineer')::record
           ))
    UNION ALL
    SELECT 'JSON',
           octet_length(row_to_json(
               ROW('Alice Johnson', 'alice@example.com', 'Software Engineer')
           )::text::bytea)
) sizes
ORDER BY size_bytes;

\echo ''
\echo '=== Test 3: Number-Heavy Record ==='
\echo 'Record: (1.5, 2.7, 3.14159, 42, 1000000)'
\echo ''

SELECT
    format,
    size_bytes,
    round((size_bytes::numeric /
        (SELECT octet_length(row_to_json(ROW(1.5, 2.7, 3.14159, 42, 1000000))::text::bytea))
    ) * 100, 1)::text || '%' as relative_size
FROM (
    SELECT 'MessagePack' as format,
           octet_length(row_to_msgpack(ROW(1.5, 2.7, 3.14159, 42, 1000000)::record)) as size_bytes
    UNION ALL
    SELECT 'CBOR',
           octet_length(row_to_cbor(ROW(1.5, 2.7, 3.14159, 42, 1000000)::record))
    UNION ALL
    SELECT 'FlexBuffers',
           octet_length(row_to_flexbuffers(ROW(1.5, 2.7, 3.14159, 42, 1000000)::record))
    UNION ALL
    SELECT 'JSON',
           octet_length(row_to_json(ROW(1.5, 2.7, 3.14159, 42, 1000000))::text::bytea)
) sizes
ORDER BY size_bytes;

\echo ''
\echo '=== Test 4: Real-World User Database ==='
CREATE TEMP TABLE users (
    id serial PRIMARY KEY,
    username text,
    email text,
    age int,
    score float8,
    active boolean,
    created_at timestamp DEFAULT now()
);

INSERT INTO users (username, email, age, score, active) VALUES
    ('alice', 'alice@example.com', 28, 92.5, true),
    ('bob', 'bob@example.com', 35, 87.3, true),
    ('charlie', 'charlie@example.com', 42, 95.8, false),
    ('diana', 'diana@example.com', 31, 88.2, true),
    ('eve', 'eve@example.com', 29, 91.7, true),
    ('frank', 'frank@example.com', 45, 76.4, false),
    ('grace', 'grace@example.com', 33, 94.1, true),
    ('henry', 'henry@example.com', 27, 89.3, true);

\echo 'Statistics for 8 user records:'
\echo ''

WITH format_sizes AS (
    SELECT
        'MessagePack' as format,
        octet_length(row_to_msgpack(users.*)) as size
    FROM users
    UNION ALL
    SELECT 'CBOR', octet_length(row_to_cbor(users.*)) FROM users
    UNION ALL
    SELECT 'FlexBuffers', octet_length(row_to_flexbuffers(users.*)) FROM users
    UNION ALL
    SELECT 'JSON', octet_length(row_to_json(users.*)::text::bytea) FROM users
),
json_baseline AS (
    SELECT avg(size) as avg_size FROM format_sizes WHERE format = 'JSON'
)
SELECT
    format,
    min(size)::int as min_bytes,
    round(avg(size))::int as avg_bytes,
    max(size)::int as max_bytes,
    round(100 - (avg(size) / (SELECT avg_size FROM json_baseline) * 100), 1)::text || '%' as avg_savings
FROM format_sizes
GROUP BY format
ORDER BY avg(size);

\echo ''
\echo '=== Test 5: NULL Value Impact ==='
\echo 'Comparing record with values vs. record with NULLs'
\echo ''

CREATE TYPE test_record AS (a int, b text, c float8, d boolean);

WITH null_comparison AS (
    SELECT
        'All Values' as scenario,
        'MessagePack' as format,
        octet_length(row_to_msgpack(ROW(123, 'test', 3.14, true)::test_record)) as size
    UNION ALL
    SELECT 'All Values', 'CBOR',
           octet_length(row_to_cbor(ROW(123, 'test', 3.14, true)::test_record))
    UNION ALL
    SELECT 'All Values', 'FlexBuffers',
           octet_length(row_to_flexbuffers(ROW(123, 'test', 3.14, true)::test_record))
    UNION ALL
    SELECT 'Half NULLs', 'MessagePack',
           octet_length(row_to_msgpack(ROW(123, NULL, 3.14, NULL)::test_record))
    UNION ALL
    SELECT 'Half NULLs', 'CBOR',
           octet_length(row_to_cbor(ROW(123, NULL, 3.14, NULL)::test_record))
    UNION ALL
    SELECT 'Half NULLs', 'FlexBuffers',
           octet_length(row_to_flexbuffers(ROW(123, NULL, 3.14, NULL)::test_record))
)
SELECT
    format,
    max(CASE WHEN scenario = 'All Values' THEN size END) as with_values,
    max(CASE WHEN scenario = 'Half NULLs' THEN size END) as with_nulls,
    (max(CASE WHEN scenario = 'All Values' THEN size END) -
     max(CASE WHEN scenario = 'Half NULLs' THEN size END))::text || ' bytes saved'
        as null_savings
FROM null_comparison
GROUP BY format
ORDER BY with_nulls;

\echo ''
\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║                     FINAL COMPARISON                           ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''

\echo 'Overall Rankings (Average across all tests):'
\echo ''
\echo '1. MessagePack:  Most compact overall (~25-30% smaller than JSON)'
\echo '2. CBOR:         Very close to MessagePack, IETF standard'
\echo '3. JSON:         Baseline, human-readable'
\echo '4. FlexBuffers:  Larger but offers zero-copy deserialization'
\echo ''

\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║                    USE CASE GUIDE                              ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''
\echo 'Choose MessagePack when:'
\echo '  • Maximum compression is priority'
\echo '  • Building REST/gRPC APIs'
\echo '  • Wide language support needed'
\echo '  • Network bandwidth is limited'
\echo ''
\echo 'Choose CBOR when:'
\echo '  • Need IETF standard compliance (RFC 8949)'
\echo '  • IoT or embedded systems'
\echo '  • Interoperability with other CBOR systems'
\echo '  • Similar to MessagePack but standardized'
\echo ''
\echo 'Choose FlexBuffers when:'
\echo '  • Zero-copy deserialization is critical'
\echo '  • Large records, reading only some fields'
\echo '  • Using Google FlatBuffers ecosystem'
\echo '  • Memory efficiency during reads'
\echo ''
\echo 'Choose JSON when:'
\echo '  • Human readability required'
\echo '  • Debugging and development'
\echo '  • Web browsers (native support)'
\echo '  • Simple integrations'
\echo ''

\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║                  AVAILABLE FUNCTIONS                           ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''
\echo '  row_to_msgpack(record) → bytea       MessagePack format'
\echo '  row_to_cbor(record) → bytea          CBOR format'
\echo '  row_to_flexbuffers(record) → bytea   FlexBuffers format'
\echo ''
\echo 'All functions support:'
\echo '  • All PostgreSQL basic types (int, float, text, bool)'
\echo '  • NULL values'
\echo '  • Named composite types'
\echo '  • Anonymous records'
\echo '  • Direct table row conversion'
\echo ''

-- Cleanup
DROP TABLE users;
DROP TYPE test_record;
