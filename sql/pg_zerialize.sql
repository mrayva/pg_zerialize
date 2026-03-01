CREATE EXTENSION pg_zerialize;

CREATE TYPE pgz_person AS (
    name text,
    age int,
    active bool
);

SELECT row_to_msgpack(ROW('alice', 30, true)::pgz_person) IS NOT NULL AS msgpack_ok;
SELECT row_to_cbor(ROW('alice', 30, true)::pgz_person) IS NOT NULL AS cbor_ok;
SELECT row_to_zera(ROW('alice', 30, true)::pgz_person) IS NOT NULL AS zera_ok;
SELECT row_to_flexbuffers(ROW('alice', 30, true)::pgz_person) IS NOT NULL AS flex_ok;

SELECT rows_to_msgpack(ARRAY[
    ROW('alice', 30, true)::pgz_person,
    ROW('bob', 40, false)::pgz_person
]) IS NOT NULL AS rows_msgpack_ok;

SELECT rows_to_msgpack(ARRAY[1,2,3]);

DROP TYPE pgz_person;
DROP EXTENSION pg_zerialize;
