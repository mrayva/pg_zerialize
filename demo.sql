-- Demo: pg_zerialize FlexBuffers Extension
-- Shows the extension working with various data types

\timing off
\pset border 2

CREATE EXTENSION IF NOT EXISTS pg_zerialize;

\echo ''
\echo '=== Test 1: Simple anonymous record ==='
\echo 'Converting ROW(''Alice'', 30, true) to FlexBuffers:'
SELECT
    row_to_flexbuffers(ROW('Alice', 30, true)::record) as flexbuffers_data,
    octet_length(row_to_flexbuffers(ROW('Alice', 30, true)::record)) as size_bytes;

\echo ''
\echo '=== Test 2: Named composite type ==='
DROP TYPE IF EXISTS employee CASCADE;
CREATE TYPE employee AS (
    name text,
    employee_id int,
    department text,
    salary float8,
    active boolean
);

\echo 'Converting employee record to FlexBuffers:'
SELECT
    row_to_flexbuffers(ROW('John Smith', 12345, 'Engineering', 95000.50, true)::employee) as flexbuffers_data,
    octet_length(row_to_flexbuffers(ROW('John Smith', 12345, 'Engineering', 95000.50, true)::employee)) as size_bytes;

\echo ''
\echo '=== Test 3: NULL handling ==='
\echo 'Testing NULL values in record:'
SELECT
    row_to_flexbuffers(ROW('Jane Doe', NULL, NULL, 87500.0, false)::employee) as with_nulls;

\echo ''
\echo '=== Test 4: Table data conversion ==='
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
    ('Tool C', 89.00, 0, false);

\echo 'Converting table rows to FlexBuffers:'
SELECT
    id,
    name,
    octet_length(row_to_flexbuffers(products.*)) as flex_size_bytes,
    substr(encode(row_to_flexbuffers(products.*), 'hex'), 1, 60) || '...' as flex_hex_preview
FROM products
ORDER BY id;

\echo ''
\echo '=== Test 5: Size comparison FlexBuffers vs JSON ==='
SELECT
    'FlexBuffers' as format,
    avg(octet_length(row_to_flexbuffers(products.*)))::int as avg_bytes
FROM products
UNION ALL
SELECT
    'JSON' as format,
    avg(octet_length(row_to_json(products.*)::text::bytea))::int as avg_bytes
FROM products
ORDER BY avg_bytes;

\echo ''
\echo '=== Test 6: Different data types ==='
\echo 'Testing various PostgreSQL types:'
SELECT
    'smallint' as type,
    octet_length(row_to_flexbuffers(ROW(123::smallint)::record)) as bytes
UNION ALL
SELECT 'integer', octet_length(row_to_flexbuffers(ROW(123456::integer)::record))
UNION ALL
SELECT 'bigint', octet_length(row_to_flexbuffers(ROW(9223372036854775807::bigint)::record))
UNION ALL
SELECT 'real', octet_length(row_to_flexbuffers(ROW(3.14::real)::record))
UNION ALL
SELECT 'double', octet_length(row_to_flexbuffers(ROW(3.14159265359::double precision)::record))
UNION ALL
SELECT 'boolean', octet_length(row_to_flexbuffers(ROW(true::boolean)::record))
UNION ALL
SELECT 'text', octet_length(row_to_flexbuffers(ROW('Hello, FlexBuffers!'::text)::record));

\echo ''
\echo '=== Summary ==='
\echo 'pg_zerialize extension is working correctly!'
\echo 'FlexBuffers binary format is more compact than JSON.'
\echo ''
\echo 'Next steps:'
\echo '  - Add support for arrays'
\echo '  - Add support for nested composite types'
\echo '  - Implement MessagePack, CBOR, and ZERA formats'
\echo '  - Add deserialization functions'

-- Cleanup
DROP TABLE products;
DROP TYPE employee;
