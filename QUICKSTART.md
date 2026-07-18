# Quick Start

## Install Dependencies

Ubuntu/Debian example for PostgreSQL 18:

```bash
sudo apt-get install \
  build-essential \
  postgresql-18 \
  postgresql-server-dev-18 \
  libflatbuffers-dev \
  libfast-float-dev \
  python3-msgpack
```

Use matching `postgresql-<major>` and `postgresql-server-dev-<major>` packages
for PostgreSQL 16 or 17.

## Build And Enable

```bash
make -j"$(nproc)"
sudo make install
psql -d postgres -c 'CREATE EXTENSION pg_zerialize'
```

## Serialize Rows

```sql
CREATE TYPE person AS (
  name text,
  age integer,
  active boolean
);

SELECT row_to_msgpack(ROW('Ada', 37, true)::person);
SELECT row_to_cbor(ROW('Ada', 37, true)::person);
SELECT row_to_zera(ROW('Ada', 37, true)::person);
SELECT row_to_flexbuffers(ROW('Ada', 37, true)::person);
```

Serialize table rows individually:

```sql
SELECT id, row_to_msgpack(users.*)
FROM users;
```

Serialize a set as one protocol array:

```sql
SELECT rows_to_msgpack(array_agg(users.* ORDER BY id))
FROM users;
```

## Serialize Nested Composites

```sql
CREATE TYPE address AS (
  city text,
  postal_code text
);

CREATE TYPE customer AS (
  id integer,
  name text,
  shipping address
);

SELECT row_to_msgpack(
  ROW(7, 'Ada', ROW('London', 'SW1A')::address)::customer
);
```

The `shipping` field is a nested map, not PostgreSQL composite text.

For nested MessagePack assembled from joins, build one JSONB tree:

```sql
SELECT msgpack_from_jsonb(
  jsonb_build_object(
    'id', d.id,
    'name', d.name,
    'employees', COALESCE(e.items, '[]'::jsonb)
  )
)
FROM departments d
LEFT JOIN (
  SELECT department_id,
         jsonb_agg(jsonb_build_object('id', id, 'name', name) ORDER BY id) AS items
  FROM employees
  GROUP BY department_id
) e ON e.department_id = d.id;
```

## Verify Output

```sql
SELECT encode(row_to_msgpack(ROW(1, 'one')), 'hex');
SELECT octet_length(row_to_msgpack(ROW(1, 'one')));
```

Run repository tests after installation:

```bash
make installcheck
make semantic-check
```

## Connection Troubleshooting

The benchmark harness defaults to:

```text
host=127.0.0.1 port=5432 database=postgres user=postgres password=postgres
```

Override `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, or `PGPASSWORD` when your
local PostgreSQL configuration differs.

## Limitations

- Multidimensional arrays are not emitted as nested arrays.
- Fractional or out-of-range `numeric` values use lossy `float64`.
- Deserialization APIs are not implemented.
- A `json` value remains text; use JSONB builders for recursive JSON objects.
