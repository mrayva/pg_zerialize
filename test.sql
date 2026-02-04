-- Test pg_zerialize extension

-- Drop extension if it exists
DROP EXTENSION IF EXISTS pg_zerialize CASCADE;

-- Create the extension
CREATE EXTENSION pg_zerialize;

-- Test 1: Simple row with mixed types
SELECT row_to_flexbuffers(ROW('Alice', 30, true)::record);

-- Test 2: Named record type
CREATE TYPE person AS (
    name text,
    age int,
    active boolean,
    salary float8
);

SELECT row_to_flexbuffers(ROW('Bob', 25, false, 75000.50)::person);

-- Test 3: NULL values
SELECT row_to_flexbuffers(ROW('Charlie', NULL, true)::record);

-- Test 4: From table data
CREATE TABLE users (
    id serial PRIMARY KEY,
    username text NOT NULL,
    age int,
    score float8
);

INSERT INTO users (username, age, score) VALUES
    ('user1', 25, 95.5),
    ('user2', 30, 87.3);

SELECT username, row_to_flexbuffers(users.*)
FROM users;

-- Cleanup
DROP TABLE users;
DROP TYPE person;
