-- Demo: Compare all available binary formats in pg_zerialize
-- FlexBuffers vs MessagePack vs JSON

\timing off
\pset border 2

DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

\echo ''
\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║           pg_zerialize - Multi-Format Demo                    ║'
\echo '║     FlexBuffers & MessagePack Binary Serialization            ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''

\echo '=== Test 1: Size Comparison - Simple Record ==='
\echo 'Record: (''Alice'', 30, true)'
\echo ''

SELECT
    format,
    size_bytes,
    CASE
        WHEN format = 'JSON' THEN '100%'
        ELSE round((size_bytes::numeric / json_size) * 100, 1)::text || '%'
    END as relative_to_json
FROM (
    SELECT
        'JSON' as format,
        octet_length(row_to_json(ROW('Alice', 30, true))::text::bytea) as size_bytes,
        octet_length(row_to_json(ROW('Alice', 30, true))::text::bytea) as json_size
    UNION ALL
    SELECT
        'FlexBuffers',
        octet_length(row_to_flexbuffers(ROW('Alice', 30, true)::record)),
        octet_length(row_to_json(ROW('Alice', 30, true))::text::bytea)
    UNION ALL
    SELECT
        'MessagePack',
        octet_length(row_to_msgpack(ROW('Alice', 30, true)::record)),
        octet_length(row_to_json(ROW('Alice', 30, true))::text::bytea)
) sizes
ORDER BY size_bytes;

\echo ''
\echo '=== Test 2: Complex Record with Multiple Types ==='
CREATE TYPE person AS (
    name text,
    age int,
    height float8,
    active boolean,
    score int
);

\echo 'Record: person(''John Smith'', 42, 1.75, true, 95)'
\echo ''

SELECT
    format,
    size_bytes,
    round((size_bytes::numeric /
        (SELECT octet_length(row_to_json(ROW('John Smith', 42, 1.75, true, 95))::text::bytea))
    ) * 100, 1)::text || '%' as relative_to_json
FROM (
    SELECT 'MessagePack' as format,
           octet_length(row_to_msgpack(ROW('John Smith', 42, 1.75, true, 95)::person)) as size_bytes
    UNION ALL
    SELECT 'FlexBuffers',
           octet_length(row_to_flexbuffers(ROW('John Smith', 42, 1.75, true, 95)::person))
    UNION ALL
    SELECT 'JSON',
           octet_length(row_to_json(ROW('John Smith', 42, 1.75, true, 95))::text::bytea)
) sizes
ORDER BY size_bytes;

\echo ''
\echo '=== Test 3: Real-World Table Data ==='
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
    avg_bytes::int as avg_size_bytes,
    round(savings_percent, 1)::text || '%' as space_saved_vs_json
FROM (
    SELECT
        'JSON' as format,
        avg(octet_length(row_to_json(users.*)::text::bytea)) as avg_bytes,
        0.0 as savings_percent
    FROM users
    UNION ALL
    SELECT
        'FlexBuffers',
        avg(octet_length(row_to_flexbuffers(users.*))),
        100 - (avg(octet_length(row_to_flexbuffers(users.*))) /
               avg(octet_length(row_to_json(users.*)::text::bytea)) * 100)
    FROM users
    UNION ALL
    SELECT
        'MessagePack',
        avg(octet_length(row_to_msgpack(users.*))),
        100 - (avg(octet_length(row_to_msgpack(users.*))) /
               avg(octet_length(row_to_json(users.*)::text::bytea)) * 100)
    FROM users
) comparison
ORDER BY avg_bytes;

\echo ''
\echo '=== Test 4: Individual Row Comparison ==='
SELECT
    username,
    octet_length(row_to_msgpack(users.*)) as msgpack,
    octet_length(row_to_flexbuffers(users.*)) as flexbuffers,
    octet_length(row_to_json(users.*)::text::bytea) as json,
    octet_length(row_to_json(users.*)::text::bytea) -
        octet_length(row_to_msgpack(users.*)) as json_vs_msgpack_savings
FROM users
ORDER BY id;

\echo ''
\echo '=== Test 5: NULL Value Handling ==='
INSERT INTO users (username, email, age, score, active) VALUES
    ('null_test', NULL, NULL, NULL, false);

SELECT
    'MessagePack' as format,
    octet_length(row_to_msgpack(users.*)) as size_with_nulls
FROM users WHERE username = 'null_test'
UNION ALL
SELECT
    'FlexBuffers',
    octet_length(row_to_flexbuffers(users.*))
FROM users WHERE username = 'null_test'
UNION ALL
SELECT
    'JSON',
    octet_length(row_to_json(users.*)::text::bytea)
FROM users WHERE username = 'null_test'
ORDER BY size_with_nulls;

\echo ''
\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║                        SUMMARY                                 ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''
\echo '✓ MessagePack:   Most compact format (~30-40% smaller than JSON)'
\echo '✓ FlexBuffers:   Slightly larger but supports zero-copy reads'
\echo '✓ JSON:          Human-readable but largest size'
\echo ''
\echo 'Use Cases:'
\echo '  • MessagePack:  Network APIs, caching, maximum compression'
\echo '  • FlexBuffers:  When you need lazy/zero-copy deserialization'
\echo '  • JSON:         Debugging, human-readable output'
\echo ''
\echo 'Available Functions:'
\echo '  • row_to_msgpack(record)      - MessagePack binary format'
\echo '  • row_to_flexbuffers(record)  - FlexBuffers binary format'
\echo ''
\echo 'Coming Soon: CBOR and ZERA formats'
\echo ''

-- Cleanup
DROP TABLE users;
DROP TYPE person;
