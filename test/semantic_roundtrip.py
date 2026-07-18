#!/usr/bin/env python3
"""Validate pg_zerialize payloads with the independent python-msgpack decoder."""

from __future__ import annotations

import csv
import io
import math
import os
import subprocess
import sys
from datetime import date, datetime, timezone
from decimal import Decimal

try:
    import msgpack
except ImportError as exc:
    raise SystemExit(
        "python-msgpack is required (Ubuntu/Debian: apt install python3-msgpack)"
    ) from exc


SQL = r"""
SET client_min_messages TO warning;
CREATE EXTENSION IF NOT EXISTS pg_zerialize;
BEGIN;

CREATE TYPE pg_temp.pgz_sem_primitive AS (
    id bigint,
    name text,
    active boolean,
    score double precision,
    nullable text
);

CREATE TYPE pg_temp.pgz_sem_status AS ENUM ('new', 'active', 'closed');

CREATE TYPE pg_temp.pgz_sem_enum_holder AS (
    status pg_temp.pgz_sem_status
);

CREATE TYPE pg_temp.pgz_sem_native AS (
    d date,
    ts timestamp,
    tstz timestamptz,
    payload bytea,
    document jsonb,
    json_text json,
    identifier uuid,
    status pg_temp.pgz_sem_status,
    address inet,
    network cidr,
    duration interval,
    object_name name,
    code "char",
    amount numeric
);

CREATE TYPE pg_temp.pgz_sem_arrays AS (
    ints integer[],
    texts text[],
    bools boolean[],
    json_texts json[],
    uuids uuid[],
    statuses pg_temp.pgz_sem_status[],
    names name[],
    codes "char"[],
    addresses inet[],
    networks cidr[],
    durations interval[]
);

CREATE TYPE pg_temp.pgz_sem_direct_arrays AS (
    uuids uuid[],
    statuses pg_temp.pgz_sem_status[],
    names name[],
    codes "char"[],
    addresses inet[],
    networks cidr[],
    durations interval[]
);

CREATE TYPE pg_temp.pgz_sem_numeric_edges AS (
    trailing_zero numeric,
    min_i64 numeric,
    max_i64 numeric,
    overflow_i64 numeric,
    values numeric[]
);

CREATE TYPE pg_temp.pgz_sem_coordinates AS (
    latitude double precision,
    longitude double precision
);

CREATE TYPE pg_temp.pgz_sem_address AS (
    city text,
    postal_code text,
    coordinates pg_temp.pgz_sem_coordinates
);

CREATE TYPE pg_temp.pgz_sem_customer AS (
    id integer,
    name text,
    shipping pg_temp.pgz_sem_address,
    previous_addresses pg_temp.pgz_sem_address[],
    billing pg_temp.pgz_sem_address
);

CREATE TEMP TABLE pgz_sem_cases (
    label text PRIMARY KEY,
    payload bytea NOT NULL
);

INSERT INTO pgz_sem_cases VALUES
    ('primitive_row', row_to_msgpack(
        ROW(42, 'hello', true, 1.25, NULL)::pg_temp.pgz_sem_primitive)),
    ('unicode_row', row_to_msgpack(
        ROW(7, U&'\3053\3093\306B\3061\306F', false, -0.5, U&'\041F\0440\0438\0432\0435\0442')
        ::pg_temp.pgz_sem_primitive)),
    ('native_row', row_to_msgpack(
        ROW(
            DATE '2025-01-02',
            TIMESTAMP '2025-01-02 03:04:05.123456',
            TIMESTAMPTZ '2025-01-02 03:04:05.123456+00',
            decode('00ff10', 'hex'),
            '{"a":1,"nested":[true,null]}'::jsonb,
            '{"a":1,"nested":[true,null]}'::json,
            '123e4567-e89b-12d3-a456-426614174000'::uuid,
            'active'::pg_temp.pgz_sem_status,
            '192.0.2.1/24'::inet,
            '192.0.2.0/24'::cidr,
            '2 days 03:04:05.000006'::interval,
            'catalog_name'::name,
            'Z'::"char",
            123.5::numeric
        )::pg_temp.pgz_sem_native)),
    ('array_row', row_to_msgpack(
        ROW(
            ARRAY[1, NULL, 3],
            ARRAY['a', NULL, U&'\03A9'],
            ARRAY[true, false, NULL],
            ARRAY['{"id":1}'::json, NULL, '[true,false]'::json],
            ARRAY['123e4567-e89b-12d3-a456-426614174000'::uuid, NULL],
            ARRAY['new'::pg_temp.pgz_sem_status, NULL, 'active'::pg_temp.pgz_sem_status],
            ARRAY['first'::name, NULL, 'second'::name],
            ARRAY['A'::"char", NULL, 'Z'::"char"],
            ARRAY['192.0.2.1/24'::inet, NULL, '2001:db8::1/64'::inet],
            ARRAY['192.0.2.0/24'::cidr, NULL, '2001:db8::/32'::cidr],
            ARRAY['1 day 00:00:01'::interval, NULL, '00:00:00.000001'::interval]
        )::pg_temp.pgz_sem_arrays)),
    ('direct_arrays_no_null', row_to_msgpack(
        ROW(
            ARRAY['123e4567-e89b-12d3-a456-426614174000'::uuid],
            ARRAY['new'::pg_temp.pgz_sem_status, 'closed'::pg_temp.pgz_sem_status],
            ARRAY['first'::name, 'second'::name],
            ARRAY['A'::"char", 'Z'::"char"],
            ARRAY['192.0.2.1/24'::inet, '2001:db8::1/64'::inet],
            ARRAY['192.0.2.0/24'::cidr, '2001:db8::/32'::cidr],
            ARRAY['1 day'::interval, '00:00:00.000001'::interval]
        )::pg_temp.pgz_sem_direct_arrays)),
    ('numeric_edges', row_to_msgpack(
        ROW(
            '-0.000'::numeric,
            '-9223372036854775808'::numeric,
            '9223372036854775807'::numeric,
            '9223372036854775808'::numeric,
            ARRAY['1.25'::numeric, 'NaN'::numeric, 'Infinity'::numeric, '-Infinity'::numeric]
        )::pg_temp.pgz_sem_numeric_edges)),
    ('batch_rows', rows_to_msgpack(ARRAY[
        ROW(1, 'one', true, 1.0, NULL)::pg_temp.pgz_sem_primitive,
        NULL::pg_temp.pgz_sem_primitive,
        ROW(2, 'two', false, 2.5, 'set')::pg_temp.pgz_sem_primitive
    ])),
    ('nested_composite', row_to_msgpack(
        ROW(
            7,
            'Ada',
            ROW(
                'London',
                'SW1A',
                ROW(51.501, -0.142)::pg_temp.pgz_sem_coordinates
            )::pg_temp.pgz_sem_address,
            ARRAY[
                ROW(
                    'Paris',
                    '75001',
                    ROW(48.861, 2.335)::pg_temp.pgz_sem_coordinates
                )::pg_temp.pgz_sem_address,
                NULL::pg_temp.pgz_sem_address
            ],
            NULL::pg_temp.pgz_sem_address
        )::pg_temp.pgz_sem_customer)),
    ('nested_composite_batch', rows_to_msgpack(ARRAY[
        ROW(
            8,
            'Grace',
            ROW('Arlington', '22201', NULL)::pg_temp.pgz_sem_address,
            ARRAY[]::pg_temp.pgz_sem_address[],
            NULL::pg_temp.pgz_sem_address
        )::pg_temp.pgz_sem_customer,
        NULL::pg_temp.pgz_sem_customer
    ])),
    ('nested_jsonb', msgpack_from_jsonb(
        '{"dept":{"id":10,"name":"engineering"},"staff":[{"id":1,"roles":["dev","review"]},{"id":2,"roles":[]}],"open":true}'::jsonb)),
    ('builder_array', msgpack_build_array(1, 'x', true, NULL, 2.5::numeric)),
    ('builder_object', msgpack_build_object('id', 9, 'name', 'nine', 'active', false)),
    ('exact_numeric', msgpack_build_array(9223372036854775807::numeric)),
    ('float_numeric', msgpack_build_array(12345678901234567890.123456789::numeric)),
    ('builder_composite', msgpack_build_array(
        ROW('Rome', '00100', NULL)::pg_temp.pgz_sem_address)),
    ('builder_anonymous_composite', msgpack_build_array(ROW(11, 'anonymous'))),
    ('builder_binary_embedding', msgpack_build_array(msgpack_build_object('a', 1)));

CREATE TEMP TABLE pgz_sem_people(id integer, name text);
INSERT INTO pgz_sem_people VALUES (2, 'two'), (1, 'one');

INSERT INTO pgz_sem_cases
SELECT 'array_aggregate',
       msgpack_agg(jsonb_build_object('id', id, 'name', name) ORDER BY id)
FROM pgz_sem_people;

INSERT INTO pgz_sem_cases
SELECT 'object_aggregate', msgpack_object_agg(name, id ORDER BY name)
FROM pgz_sem_people;

-- Warm the enum-label cache above, then verify catalog invalidation on rename.
ALTER TYPE pg_temp.pgz_sem_status RENAME VALUE 'active' TO 'enabled';
INSERT INTO pgz_sem_cases VALUES
    ('enum_renamed', row_to_msgpack(
        ROW('enabled'::pg_temp.pgz_sem_status)::pg_temp.pgz_sem_enum_holder));

-- Verify recursive schema-cache invalidation at data-semantics level.
ALTER TYPE pg_temp.pgz_sem_address ADD ATTRIBUTE country text;
INSERT INTO pgz_sem_cases VALUES
    ('nested_composite_ddl', row_to_msgpack(
        ROW(
            12,
            'Dorothy',
            ROW('Nashville', '37201', NULL, 'US')::pg_temp.pgz_sem_address,
            ARRAY[]::pg_temp.pgz_sem_address[],
            NULL::pg_temp.pgz_sem_address
        )::pg_temp.pgz_sem_customer));

ALTER TYPE pg_temp.pgz_sem_address DROP ATTRIBUTE postal_code;
INSERT INTO pgz_sem_cases VALUES
    ('nested_composite_dropped', row_to_msgpack(
        ROW(
            13,
            'Mary',
            ROW('New York', NULL, 'US')::pg_temp.pgz_sem_address,
            ARRAY[]::pg_temp.pgz_sem_address[],
            NULL::pg_temp.pgz_sem_address
        )::pg_temp.pgz_sem_customer));

COPY (
    SELECT label, encode(payload, 'hex')
    FROM pgz_sem_cases
    ORDER BY label
) TO STDOUT WITH (FORMAT csv);

ROLLBACK;
"""


def fail(message: str) -> None:
    raise AssertionError(message)


def query_payloads() -> dict[str, object]:
    env = os.environ.copy()
    env.setdefault("PGHOST", "127.0.0.1")
    env.setdefault("PGPORT", "5432")
    env.setdefault("PGUSER", "postgres")
    env.setdefault("PGPASSWORD", "postgres")
    env.setdefault("PGDATABASE", "postgres")

    proc = subprocess.run(
        ["psql", "-X", "-q", "-v", "ON_ERROR_STOP=1"],
        input=SQL,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise SystemExit(proc.returncode)

    payloads: dict[str, object] = {}
    for row in csv.reader(io.StringIO(proc.stdout)):
        if len(row) != 2:
            fail(f"unexpected psql output row: {row!r}")
        label, encoded = row
        payloads[label] = msgpack.unpackb(bytes.fromhex(encoded), raw=False)
    return payloads


def postgres_date_days(value: date) -> int:
    return (value - date(2000, 1, 1)).days


def postgres_timestamp_micros(value: datetime) -> int:
    epoch = datetime(2000, 1, 1, tzinfo=value.tzinfo)
    return int((value - epoch).total_seconds() * 1_000_000)


def main() -> None:
    actual = query_payloads()
    expected_labels = {
        "array_aggregate",
        "array_row",
        "batch_rows",
        "builder_array",
        "builder_anonymous_composite",
        "builder_binary_embedding",
        "builder_composite",
        "builder_object",
        "direct_arrays_no_null",
        "exact_numeric",
        "enum_renamed",
        "float_numeric",
        "native_row",
        "nested_composite",
        "nested_composite_batch",
        "nested_composite_ddl",
        "nested_composite_dropped",
        "nested_jsonb",
        "numeric_edges",
        "object_aggregate",
        "primitive_row",
        "unicode_row",
    }
    if set(actual) != expected_labels:
        fail(f"case labels differ: expected {expected_labels}, got {set(actual)}")

    assert actual["primitive_row"] == {
        "id": 42,
        "name": "hello",
        "active": True,
        "score": 1.25,
        "nullable": None,
    }
    assert actual["unicode_row"] == {
        "id": 7,
        "name": "\u3053\u3093\u306b\u3061\u306f",
        "active": False,
        "score": -0.5,
        "nullable": "\u041f\u0440\u0438\u0432\u0435\u0442",
    }
    assert actual["array_row"] == {
        "ints": [1, None, 3],
        "texts": ["a", None, "\u03a9"],
        "bools": [True, False, None],
        "json_texts": ['{"id":1}', None, "[true,false]"],
        "uuids": ["123e4567-e89b-12d3-a456-426614174000", None],
        "statuses": ["new", None, "active"],
        "names": ["first", None, "second"],
        "codes": ["A", None, "Z"],
        "addresses": ["192.0.2.1/24", None, "2001:db8::1/64"],
        "networks": ["192.0.2.0/24", None, "2001:db8::/32"],
        "durations": ["1 day 00:00:01", None, "00:00:00.000001"],
    }
    assert actual["direct_arrays_no_null"] == {
        "uuids": ["123e4567-e89b-12d3-a456-426614174000"],
        "statuses": ["new", "closed"],
        "names": ["first", "second"],
        "codes": ["A", "Z"],
        "addresses": ["192.0.2.1/24", "2001:db8::1/64"],
        "networks": ["192.0.2.0/24", "2001:db8::/32"],
        "durations": ["1 day", "00:00:00.000001"],
    }
    assert actual["batch_rows"] == [
        {"id": 1, "name": "one", "active": True, "score": 1.0, "nullable": None},
        None,
        {"id": 2, "name": "two", "active": False, "score": 2.5, "nullable": "set"},
    ]
    assert actual["nested_jsonb"] == {
        "dept": {"id": 10, "name": "engineering"},
        "open": True,
        "staff": [
            {"id": 1, "roles": ["dev", "review"]},
            {"id": 2, "roles": []},
        ],
    }
    assert actual["nested_composite"] == {
        "id": 7,
        "name": "Ada",
        "shipping": {
            "city": "London",
            "postal_code": "SW1A",
            "coordinates": {"latitude": 51.501, "longitude": -0.142},
        },
        "previous_addresses": [
            {
                "city": "Paris",
                "postal_code": "75001",
                "coordinates": {"latitude": 48.861, "longitude": 2.335},
            },
            None,
        ],
        "billing": None,
    }
    assert actual["nested_composite_batch"] == [
        {
            "id": 8,
            "name": "Grace",
            "shipping": {
                "city": "Arlington",
                "postal_code": "22201",
                "coordinates": None,
            },
            "previous_addresses": [],
            "billing": None,
        },
        None,
    ]
    assert actual["builder_composite"] == [
        {"city": "Rome", "postal_code": "00100", "coordinates": None}
    ]
    assert actual["builder_anonymous_composite"] == [
        {"f1": 11, "f2": "anonymous"}
    ]
    assert actual["nested_composite_ddl"] == {
        "id": 12,
        "name": "Dorothy",
        "shipping": {
            "city": "Nashville",
            "postal_code": "37201",
            "coordinates": None,
            "country": "US",
        },
        "previous_addresses": [],
        "billing": None,
    }
    assert actual["nested_composite_dropped"] == {
        "id": 13,
        "name": "Mary",
        "shipping": {
            "city": "New York",
            "coordinates": None,
            "country": "US",
        },
        "previous_addresses": [],
        "billing": None,
    }
    assert actual["builder_array"] == [1, "x", True, None, 2.5]
    assert actual["builder_object"] == {"id": 9, "name": "nine", "active": False}
    assert actual["array_aggregate"] == [
        {"id": 1, "name": "one"},
        {"id": 2, "name": "two"},
    ]
    assert actual["object_aggregate"] == {"one": 1, "two": 2}
    assert actual["enum_renamed"] == {"status": "enabled"}
    assert actual["exact_numeric"] == [9223372036854775807]

    numeric_edges = actual["numeric_edges"]
    assert numeric_edges["trailing_zero"] == 0
    assert isinstance(numeric_edges["trailing_zero"], int)
    assert numeric_edges["min_i64"] == -9223372036854775808
    assert numeric_edges["max_i64"] == 9223372036854775807
    assert numeric_edges["overflow_i64"] == float(Decimal("9223372036854775808"))
    assert isinstance(numeric_edges["overflow_i64"], float)
    assert numeric_edges["values"][0] == 1.25
    assert math.isnan(numeric_edges["values"][1])
    assert numeric_edges["values"][2] == math.inf
    assert numeric_edges["values"][3] == -math.inf

    lossy_source = Decimal("12345678901234567890.123456789")
    assert actual["float_numeric"] == [float(lossy_source)]
    assert Decimal.from_float(actual["float_numeric"][0]) != lossy_source

    native = actual["native_row"]
    assert isinstance(native, dict)
    assert native["d"] == postgres_date_days(date(2025, 1, 2))
    assert native["ts"] == postgres_timestamp_micros(datetime(2025, 1, 2, 3, 4, 5, 123456))
    assert native["tstz"] == postgres_timestamp_micros(
        datetime(2025, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)
    )
    assert native["payload"] == b"\x00\xff\x10"
    assert isinstance(native["document"], bytes) and native["document"]
    assert native["json_text"] == '{"a":1,"nested":[true,null]}'
    assert native["identifier"] == "123e4567-e89b-12d3-a456-426614174000"
    assert native["status"] == "active"
    assert native["address"] == "192.0.2.1/24"
    assert native["network"] == "192.0.2.0/24"
    assert native["duration"] == "2 days 03:04:05.000006"
    assert native["object_name"] == "catalog_name"
    assert native["code"] == "Z"
    assert native["amount"] == 123.5

    embedded = actual["builder_binary_embedding"]
    assert embedded == [b"\x81\xa1a\x01"]

    print(f"independent MessagePack semantics: {len(actual)} cases passed")


if __name__ == "__main__":
    main()
