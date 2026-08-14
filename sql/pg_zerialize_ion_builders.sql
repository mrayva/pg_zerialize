SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

-- Mirrors pg_zerialize_cbor_builders.sql for the Ion builder API
-- (ion_from_jsonb, ion_build_object, ion_build_array, ion_agg, ion_object_agg).

SELECT ion_build_object('id', 1, 'name', 'alice', 'active', true) IS NOT NULL AS build_object_ok;
SELECT ion_build_array(1, 'x', true, NULL) IS NOT NULL AS build_array_ok;

-- Array/object semantics parity: builder vs jsonb conversion, within Ion.
SELECT ion_build_array(1, 'x', true, NULL, 42.5::numeric)
       = ion_from_jsonb(jsonb_build_array(1, 'x', true, NULL, 42.5::numeric)) AS array_parity;

SELECT ion_build_object('a', 1, 'b', 'x', 'c', true)
       = ion_from_jsonb(jsonb_build_object('a', 1, 'b', 'x', 'c', true)) AS object_parity;

-- Nested semantics parity (via ion_from_jsonb, which recurses; ion_build_object
-- treats a raw jsonb-typed variadic argument as opaque -- see README's
-- "JSON text is not recursively parsed" note).
SELECT ion_from_jsonb(
           jsonb_build_object(
               'a', jsonb_build_array(1,2,3),
               'b', jsonb_build_object('x', true, 'y', NULL),
               'c', jsonb_build_array(jsonb_build_object('k', 1), jsonb_build_object('k', 2))
           )
       )
       = ion_from_jsonb(
           jsonb_build_object(
               'a', jsonb_build_array(1,2,3),
               'b', jsonb_build_object('x', true, 'y', NULL),
               'c', jsonb_build_array(jsonb_build_object('k', 1), jsonb_build_object('k', 2))
           )
       ) AS nested_stable;

SELECT ion_to_jsonb(ion_from_jsonb(
           jsonb_build_object('arr', jsonb_build_array(1,2,3))
       )) = jsonb_build_object('arr', jsonb_build_array(1,2,3)) AS nested_array_roundtrips;

-- Cross-check against MessagePack's independently-tested builder, decoded
-- through each protocol's own JSONB decoder.
SELECT (ion_to_jsonb(ion_build_object('id', 7, 'active', true))
        = msgpack_to_jsonb(msgpack_build_object('id', 7, 'active', true))) AS matches_msgpack;

CREATE TABLE pgz_ion_bld_emp(id int, dept int, name text, role text);
INSERT INTO pgz_ion_bld_emp VALUES
    (1, 10, 'ann', 'dev'),
    (2, 10, 'bob', 'lead'),
    (3, 20, 'cam', 'ae');

SELECT ion_agg(jsonb_build_object('id', id, 'name', name) ORDER BY id) IS NOT NULL AS agg_ok
FROM pgz_ion_bld_emp;

SELECT ion_agg(jsonb_build_object('id', id, 'name', name) ORDER BY id)
       = ion_from_jsonb(jsonb_agg(jsonb_build_object('id', id, 'name', name) ORDER BY id)) AS agg_parity
FROM pgz_ion_bld_emp;

SELECT ion_object_agg(name, role ORDER BY name) IS NOT NULL AS object_agg_ok
FROM pgz_ion_bld_emp;

SELECT ion_object_agg(name, role ORDER BY name)
       = ion_from_jsonb(jsonb_object_agg(name, role ORDER BY name)) AS object_agg_parity
FROM pgz_ion_bld_emp;

-- Join-shaped nested build, same pattern as the msgpack builder tests.
WITH nested_employees AS (
  SELECT
    dept,
    jsonb_agg(jsonb_build_object('id', id, 'name', name, 'role', role) ORDER BY id) AS staff
  FROM pgz_ion_bld_emp
  GROUP BY dept
)
SELECT ion_from_jsonb(
         jsonb_build_object(
           'dept_id', d.dept,
           'dept_name', d.dept_name,
           'staff', COALESCE(ne.staff, '[]'::jsonb)
         )
       ) IS NOT NULL AS join_nested_not_null
FROM (VALUES (10,'eng'), (20,'sales'), (30,'ops')) AS d(dept, dept_name)
LEFT JOIN nested_employees ne ON ne.dept = d.dept
ORDER BY d.dept;

-- Invalid input checks for builder-style APIs.
SELECT ion_build_object('a', 1, 'b');
SELECT ion_build_object(NULL, 1);

DROP TABLE pgz_ion_bld_emp;
DROP EXTENSION pg_zerialize;
