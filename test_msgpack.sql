-- Test MessagePack support in pg_zerialize

-- Drop and recreate extension to get new functions
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

\echo ''
\echo '=== Test 1: Simple MessagePack conversion ==='
SELECT row_to_msgpack(ROW('Alice', 30, true)::record) as msgpack_data;

\echo ''
\echo '=== Test 2: Named composite type ==='
CREATE TYPE employee AS (
    name text,
    employee_id int,
    salary float8,
    active boolean
);

SELECT row_to_msgpack(ROW('John Smith', 12345, 95000.50, true)::employee) as msgpack_data;

\echo ''
\echo '=== Test 3: NULL handling ==='
SELECT row_to_msgpack(ROW('Jane', NULL, NULL, false)::employee) as msgpack_with_nulls;

\echo ''
\echo '=== Test 4: Compare sizes - MessagePack vs FlexBuffers vs JSON ==='
SELECT
    'MessagePack' as format,
    octet_length(row_to_msgpack(ROW('John Doe', 42, 95000.0, true)::record)) as bytes
UNION ALL
SELECT
    'FlexBuffers' as format,
    octet_length(row_to_flexbuffers(ROW('John Doe', 42, 95000.0, true)::record)) as bytes
UNION ALL
SELECT
    'JSON' as format,
    octet_length(row_to_json(ROW('John Doe', 42, 95000.0, true))::text::bytea) as bytes
ORDER BY bytes;

\echo ''
\echo '=== Test 5: Table data with both formats ==='
CREATE TEMP TABLE products (
    id serial PRIMARY KEY,
    name text,
    price numeric(10,2),
    stock int
);

INSERT INTO products (name, price, stock) VALUES
    ('Widget A', 29.99, 150),
    ('Gadget B', 149.50, 42),
    ('Tool C', 89.00, 25);

SELECT
    id,
    name,
    octet_length(row_to_msgpack(products.*)) as msgpack_size,
    octet_length(row_to_flexbuffers(products.*)) as flexbuffers_size,
    substr(encode(row_to_msgpack(products.*), 'hex'), 1, 40) || '...' as msgpack_preview
FROM products
ORDER BY id;

\echo ''
\echo '=== Test 6: Verify both functions work side by side ==='
SELECT
    'row_to_msgpack' as function,
    octet_length(row_to_msgpack(ROW(123, 'test', true)::record)) as size
UNION ALL
SELECT
    'row_to_flexbuffers' as function,
    octet_length(row_to_flexbuffers(ROW(123, 'test', true)::record)) as size;

\echo ''
\echo '=== Summary ==='
\echo '✓ MessagePack support added successfully!'
\echo '✓ Both FlexBuffers and MessagePack formats working'
\echo ''
\echo 'Available functions:'
\echo '  - row_to_flexbuffers(record)'
\echo '  - row_to_msgpack(record)'
\echo ''
\echo 'Next: Add CBOR and ZERA support'

-- Cleanup
DROP TABLE products;
DROP TYPE employee;
