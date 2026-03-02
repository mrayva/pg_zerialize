SET client_min_messages TO warning;
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;
CREATE EXTENSION pg_zerialize;

CREATE TYPE pgz_ci AS (a int);
SELECT row_to_msgpack(ROW(1)::pgz_ci) IS NOT NULL AS type_before_alter;

ALTER TYPE pgz_ci ADD ATTRIBUTE b text;
SELECT row_to_msgpack(ROW(1, 'x')::pgz_ci) IS NOT NULL AS type_after_alter;

CREATE TABLE pgz_ci_tbl (a int);
INSERT INTO pgz_ci_tbl VALUES (1), (2);
SELECT bool_and(row_to_msgpack(pgz_ci_tbl) IS NOT NULL) AS table_before_alter FROM pgz_ci_tbl;

ALTER TABLE pgz_ci_tbl ADD COLUMN b text DEFAULT 'v';
SELECT bool_and(row_to_msgpack(pgz_ci_tbl) IS NOT NULL) AS table_after_add_col FROM pgz_ci_tbl;

ALTER TABLE pgz_ci_tbl ALTER COLUMN b TYPE varchar(8);
SELECT bool_and(row_to_msgpack(pgz_ci_tbl) IS NOT NULL) AS table_after_type_change FROM pgz_ci_tbl;

DROP TABLE pgz_ci_tbl;
DROP TYPE pgz_ci;
DROP EXTENSION pg_zerialize;
