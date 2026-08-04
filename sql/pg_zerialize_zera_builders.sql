SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

-- Mirrors pg_zerialize_builders.sql / pg_zerialize_builders_semantics.sql for
-- the ZERA builder API (zera_from_jsonb, zera_build_object, zera_build_array,
-- zera_agg, zera_object_agg), added alongside MessagePack's.

SELECT zera_build_object('id', 1, 'name', 'alice', 'active', true) IS NOT NULL AS build_object_ok;
SELECT zera_build_array(1, 'x', true, NULL) IS NOT NULL AS build_array_ok;

-- Array/object semantics parity: builder vs jsonb conversion, within ZERA.
SELECT zera_build_array(1, 'x', true, NULL, 42.5::numeric)
       = zera_from_jsonb(jsonb_build_array(1, 'x', true, NULL, 42.5::numeric)) AS array_parity;

SELECT zera_build_object('a', 1, 'b', 'x', 'c', true)
       = zera_from_jsonb(jsonb_build_object('a', 1, 'b', 'x', 'c', true)) AS object_parity;

-- Nested semantics parity.
SELECT zera_from_jsonb(
           jsonb_build_object(
               'a', jsonb_build_array(1,2,3),
               'b', jsonb_build_object('x', true, 'y', NULL),
               'c', jsonb_build_array(jsonb_build_object('k', 1), jsonb_build_object('k', 2))
           )
       )
       = zera_from_jsonb(
           jsonb_build_object(
               'a', jsonb_build_array(1,2,3),
               'b', jsonb_build_object('x', true, 'y', NULL),
               'c', jsonb_build_array(jsonb_build_object('k', 1), jsonb_build_object('k', 2))
           )
       ) AS nested_stable;

-- Cross-check against MessagePack's independently-tested builder, decoded
-- through each protocol's own JSONB decoder.
SELECT (zera_to_jsonb(zera_build_object('id', 7, 'active', true))
        = msgpack_to_jsonb(msgpack_build_object('id', 7, 'active', true))) AS matches_msgpack;

CREATE TABLE pgz_zera_bld_emp(id int, dept int, name text, role text);
INSERT INTO pgz_zera_bld_emp VALUES
    (1, 10, 'ann', 'dev'),
    (2, 10, 'bob', 'lead'),
    (3, 20, 'cam', 'ae');

SELECT zera_agg(jsonb_build_object('id', id, 'name', name) ORDER BY id) IS NOT NULL AS agg_ok
FROM pgz_zera_bld_emp;

SELECT zera_agg(jsonb_build_object('id', id, 'name', name) ORDER BY id)
       = zera_from_jsonb(jsonb_agg(jsonb_build_object('id', id, 'name', name) ORDER BY id)) AS agg_parity
FROM pgz_zera_bld_emp;

SELECT zera_object_agg(name, role ORDER BY name) IS NOT NULL AS object_agg_ok
FROM pgz_zera_bld_emp;

SELECT zera_object_agg(name, role ORDER BY name)
       = zera_from_jsonb(jsonb_object_agg(name, role ORDER BY name)) AS object_agg_parity
FROM pgz_zera_bld_emp;

-- Join-shaped nested build, same pattern as the msgpack builder tests.
WITH nested_employees AS (
  SELECT
    dept,
    jsonb_agg(jsonb_build_object('id', id, 'name', name, 'role', role) ORDER BY id) AS staff
  FROM pgz_zera_bld_emp
  GROUP BY dept
)
SELECT zera_from_jsonb(
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
SELECT zera_build_object('a', 1, 'b');
SELECT zera_build_object(NULL, 1);

DROP TABLE pgz_zera_bld_emp;
DROP EXTENSION pg_zerialize;
