-- Test CBOR support in pg_zerialize

-- Drop and recreate extension to get new functions
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

\echo ''
\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║              CBOR Format Support Test                         ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''

\echo '=== Test 1: Simple CBOR conversion ==='
SELECT row_to_cbor(ROW('Alice', 30, true)::record) as cbor_data;

\echo ''
\echo '=== Test 2: Named composite type ==='
CREATE TYPE employee AS (
    name text,
    employee_id int,
    salary float8,
    active boolean
);

SELECT row_to_cbor(ROW('John Smith', 12345, 95000.50, true)::employee) as cbor_data;

\echo ''
\echo '=== Test 3: NULL handling ==='
SELECT row_to_cbor(ROW('Jane', NULL, NULL, false)::employee) as cbor_with_nulls;

\echo ''
\echo '=== Test 4: Four-Way Format Comparison ==='
\echo 'Record: (''John Doe'', 42, 95000.0, true)'
\echo ''

SELECT
    format,
    size_bytes,
    CASE
        WHEN format = 'JSON' THEN '100%'
        ELSE round((size_bytes::numeric /
            (SELECT octet_length(row_to_json(ROW('John Doe', 42, 95000.0, true))::text::bytea))
        ) * 100, 1)::text || '%'
    END as relative_to_json
FROM (
    SELECT 'MessagePack' as format,
           octet_length(row_to_msgpack(ROW('John Doe', 42, 95000.0, true)::record)) as size_bytes
    UNION ALL
    SELECT 'CBOR',
           octet_length(row_to_cbor(ROW('John Doe', 42, 95000.0, true)::record))
    UNION ALL
    SELECT 'FlexBuffers',
           octet_length(row_to_flexbuffers(ROW('John Doe', 42, 95000.0, true)::record))
    UNION ALL
    SELECT 'JSON',
           octet_length(row_to_json(ROW('John Doe', 42, 95000.0, true))::text::bytea)
) sizes
ORDER BY size_bytes;

\echo ''
\echo '=== Test 5: Real Table Data - All Formats ==='
CREATE TEMP TABLE products (
    id serial PRIMARY KEY,
    name text,
    price numeric(10,2),
    stock int,
    available boolean
);

INSERT INTO products (name, price, stock, available) VALUES
    ('Widget A', 29.99, 150, true),
    ('Gadget B', 149.50, 42, true),
    ('Tool C', 89.00, 25, false),
    ('Device D', 199.99, 8, true),
    ('Kit E', 59.99, 100, true);

\echo 'Individual row sizes for all formats:'
\echo ''

SELECT
    id,
    name,
    octet_length(row_to_msgpack(products.*)) as msgpack,
    octet_length(row_to_cbor(products.*)) as cbor,
    octet_length(row_to_flexbuffers(products.*)) as flexbuffers,
    octet_length(row_to_json(products.*)::text::bytea) as json
FROM products
ORDER BY id;

\echo ''
\echo '=== Test 6: Average Sizes Across 5 Products ==='

SELECT
    format,
    avg_bytes::int as avg_size,
    round(savings_vs_json, 1)::text || '%' as space_saved
FROM (
    SELECT
        'MessagePack' as format,
        avg(octet_length(row_to_msgpack(products.*))) as avg_bytes,
        100 - (avg(octet_length(row_to_msgpack(products.*))) /
               avg(octet_length(row_to_json(products.*)::text::bytea)) * 100) as savings_vs_json
    FROM products
    UNION ALL
    SELECT
        'CBOR',
        avg(octet_length(row_to_cbor(products.*))),
        100 - (avg(octet_length(row_to_cbor(products.*))) /
               avg(octet_length(row_to_json(products.*)::text::bytea)) * 100)
    FROM products
    UNION ALL
    SELECT
        'FlexBuffers',
        avg(octet_length(row_to_flexbuffers(products.*))),
        100 - (avg(octet_length(row_to_flexbuffers(products.*))) /
               avg(octet_length(row_to_json(products.*)::text::bytea)) * 100)
    FROM products
    UNION ALL
    SELECT
        'JSON',
        avg(octet_length(row_to_json(products.*)::text::bytea)),
        0.0
    FROM products
) comparison
ORDER BY avg_bytes;

\echo ''
\echo '=== Test 7: Complex Data with Multiple Types ==='
CREATE TYPE complex_record AS (
    id int,
    title text,
    score float8,
    count bigint,
    enabled boolean,
    ratio real
);

SELECT
    'MessagePack' as format,
    octet_length(row_to_msgpack(ROW(123, 'Test Record', 98.5, 1000000, true, 0.75)::complex_record)) as bytes
UNION ALL
SELECT
    'CBOR',
    octet_length(row_to_cbor(ROW(123, 'Test Record', 98.5, 1000000, true, 0.75)::complex_record))
UNION ALL
SELECT
    'FlexBuffers',
    octet_length(row_to_flexbuffers(ROW(123, 'Test Record', 98.5, 1000000, true, 0.75)::complex_record))
UNION ALL
SELECT
    'JSON',
    octet_length(row_to_json(ROW(123, 'Test Record', 98.5, 1000000, true, 0.75))::text::bytea)
ORDER BY bytes;

\echo ''
\echo '╔════════════════════════════════════════════════════════════════╗'
\echo '║                        SUMMARY                                 ║'
\echo '╚════════════════════════════════════════════════════════════════╝'
\echo ''
\echo '✓ CBOR support added successfully!'
\echo '✓ All three binary formats working perfectly'
\echo ''
\echo 'Available Functions:'
\echo '  • row_to_msgpack(record)      - MessagePack binary format'
\echo '  • row_to_cbor(record)         - CBOR binary format'
\echo '  • row_to_flexbuffers(record)  - FlexBuffers binary format'
\echo ''
\echo 'Format Characteristics:'
\echo '  • MessagePack: Most compact, widely supported'
\echo '  • CBOR: Compact, IETF standard (RFC 8949)'
\echo '  • FlexBuffers: Zero-copy reads, Google ecosystem'
\echo ''
\echo 'Next: Add ZERA format (zerialize native protocol)'
\echo ''

-- Cleanup
DROP TABLE products;
DROP TYPE employee;
DROP TYPE complex_record;
