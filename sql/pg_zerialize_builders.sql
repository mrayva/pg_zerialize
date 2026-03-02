SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

SELECT msgpack_build_object('id', 1, 'name', 'alice', 'active', true) IS NOT NULL AS build_object_ok;
SELECT msgpack_build_array(1, 'x', true, NULL) IS NOT NULL AS build_array_ok;

CREATE TABLE departments (id int PRIMARY KEY, name text);
CREATE TABLE employees (
    id int PRIMARY KEY,
    department_id int REFERENCES departments(id),
    name text,
    role text
);

INSERT INTO departments VALUES (10, 'eng'), (20, 'sales');
INSERT INTO employees VALUES
    (1, 10, 'ann', 'dev'),
    (2, 10, 'bob', 'lead'),
    (3, 20, 'cam', 'ae');

WITH nested_employees AS (
  SELECT
    department_id,
    jsonb_agg(jsonb_build_object(
      'id', id,
      'name', name,
      'role', role
    ) ORDER BY id) AS employees
  FROM employees
  GROUP BY department_id
)
SELECT msgpack_from_jsonb(
  jsonb_build_object(
    'dept_id', d.id,
    'dept_name', d.name,
    'staff', COALESCE(e.employees, '[]'::jsonb)
  )
) IS NOT NULL AS nested_join_ok
FROM departments d
LEFT JOIN nested_employees e ON d.id = e.department_id
ORDER BY d.id;

SELECT msgpack_agg(jsonb_build_object('id', id, 'name', name) ORDER BY id) IS NOT NULL AS agg_ok
FROM employees;

SELECT msgpack_object_agg(name, role ORDER BY name) IS NOT NULL AS object_agg_ok
FROM employees;

DROP TABLE employees;
DROP TABLE departments;
DROP EXTENSION pg_zerialize;
