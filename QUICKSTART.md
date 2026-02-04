# Quick Start Guide

## Installation

### 1. Prerequisites

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-server-dev-all build-essential

# RedHat/CentOS
sudo yum install postgresql-devel gcc-c++

# macOS
brew install postgresql
```

### 2. Build and Install

```bash
cd pg_zerialize
./build.sh
sudo make install
```

### 3. Enable Extension

```sql
psql -d your_database
CREATE EXTENSION pg_zerialize;
```

## Basic Usage

### Convert a row to FlexBuffers

```sql
-- Anonymous record
SELECT row_to_flexbuffers(ROW('Alice', 30, true));

-- Named type
CREATE TYPE person AS (name text, age int, active bool);
SELECT row_to_flexbuffers(ROW('Bob', 25, false)::person);

-- From existing table
SELECT id, row_to_flexbuffers(users.*) FROM users;
```

### Working with Output

FlexBuffers output is binary (`bytea`):

```sql
-- View as hex
SELECT encode(row_to_flexbuffers(ROW('test', 123)), 'hex');

-- Check size
SELECT octet_length(row_to_flexbuffers(ROW('test', 123))) as bytes;

-- Store in table
CREATE TABLE cached_data (
    id serial PRIMARY KEY,
    flex_data bytea
);

INSERT INTO cached_data (flex_data)
SELECT row_to_flexbuffers(users.*) FROM users;
```

## Use Cases

### 1. Efficient API Responses

Instead of JSON, use FlexBuffers for smaller, faster responses:

```sql
-- Before (JSON)
SELECT json_agg(row_to_json(users.*)) FROM users;

-- After (FlexBuffers) - more compact
SELECT row_to_flexbuffers(users.*) FROM users;
```

### 2. Caching

Store pre-serialized data:

```sql
CREATE MATERIALIZED VIEW user_cache AS
SELECT
    user_id,
    row_to_flexbuffers(users.*) as flex_data
FROM users;
```

### 3. Efficient Data Transfer

Send binary data over the wire:

```python
import psycopg2
from flatbuffers import flexbuffers

conn = psycopg2.connect("dbname=mydb")
cur = conn.cursor()
cur.execute("SELECT row_to_flexbuffers(users.*) FROM users")

for row in cur:
    flex_data = bytes(row[0])
    # Deserialize with FlexBuffers library
    root = flexbuffers.GetRoot(flex_data)
    print(root['name'].AsString())
```

## Current Limitations

- Arrays not yet supported
- Nested composite types not yet supported
- NUMERIC converted to string
- Dates/timestamps converted to string

See ARCHITECTURE.md for roadmap.

## Troubleshooting

### Extension not found after install

```bash
# Check PostgreSQL extension directory
pg_config --sharedir

# Verify files installed
ls $(pg_config --sharedir)/extension/pg_zerialize*
```

### Build errors

```bash
# Check PostgreSQL development headers
pg_config --includedir-server

# Verify C++20 support
g++ --version  # Need GCC 10+ or Clang 10+
```

### Function not found

```sql
-- Verify extension loaded
\dx pg_zerialize

-- Check function exists
\df row_to_flexbuffers
```

## Next Steps

1. Read ARCHITECTURE.md for detailed design info
2. Run test.sql to verify installation
3. Experiment with your own data types
4. Report issues or contribute improvements

## Performance Tips

1. **Batch operations**: Process multiple rows at once when possible
2. **Materialize**: Pre-compute for frequently accessed data
3. **Index**: Use GIN/GiST indexes on bytea columns if searching
4. **Compare sizes**: Check FlexBuffers vs JSON size difference

```sql
-- Compare sizes
SELECT
    'JSON' as format,
    avg(octet_length(row_to_json(users.*)::text::bytea)) as avg_bytes
FROM users
UNION ALL
SELECT
    'FlexBuffers' as format,
    avg(octet_length(row_to_flexbuffers(users.*))) as avg_bytes
FROM users;
```
