#!/usr/bin/env python3
"""
Verify that pg_zerialize produces valid FlexBuffers data
"""

import sys
try:
    from flatbuffers import flexbuffers
except ImportError:
    print("Error: flatbuffers module not found")
    print("Install it with: pip install flatbuffers")
    sys.exit(1)

import psycopg2

def test_flexbuffers():
    """Test FlexBuffers output from pg_zerialize"""

    # Connect to PostgreSQL
    conn = psycopg2.connect("dbname=postgres user=postgres")
    cur = conn.cursor()

    # Create extension
    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_zerialize")

    # Test 1: Simple row
    print("Test 1: Simple row with text, int, bool")
    cur.execute("SELECT row_to_flexbuffers(ROW('Alice', 30, true)::record)")
    flex_data = bytes(cur.fetchone()[0])
    print(f"  Size: {len(flex_data)} bytes")
    print(f"  Hex: {flex_data.hex()}")

    # Deserialize and verify
    root = flexbuffers.GetRoot(flex_data)
    print(f"  Decoded: f1={root['f1'].AsString()}, f2={root['f2'].AsInt()}, f3={root['f3'].AsBool()}")

    # Test 2: Named type
    print("\nTest 2: Named type (person)")
    cur.execute("""
        DROP TYPE IF EXISTS person CASCADE;
        CREATE TYPE person AS (name text, age int, salary float8);
    """)
    cur.execute("SELECT row_to_flexbuffers(ROW('Bob', 25, 75000.50)::person)")
    flex_data = bytes(cur.fetchone()[0])
    print(f"  Size: {len(flex_data)} bytes")

    root = flexbuffers.GetRoot(flex_data)
    print(f"  Decoded: name={root['name'].AsString()}, age={root['age'].AsInt()}, salary={root['salary'].AsFloat()}")

    # Test 3: NULL values
    print("\nTest 3: NULL values")
    cur.execute("SELECT row_to_flexbuffers(ROW('Charlie', NULL, true)::record)")
    flex_data = bytes(cur.fetchone()[0])
    print(f"  Size: {len(flex_data)} bytes")

    root = flexbuffers.GetRoot(flex_data)
    print(f"  Decoded: f1={root['f1'].AsString()}, f2={'NULL' if root['f2'].IsNull() else root['f2'].AsInt()}, f3={root['f3'].AsBool()}")

    # Test 4: Compare sizes with JSON
    print("\nTest 4: Size comparison FlexBuffers vs JSON")
    cur.execute("""
        SELECT
            octet_length(row_to_flexbuffers(ROW('John Doe', 42, true, 95000.0)::record)) as flex_size,
            octet_length(row_to_json(ROW('John Doe', 42, true, 95000.0))::text::bytea) as json_size
    """)
    flex_size, json_size = cur.fetchone()
    print(f"  FlexBuffers: {flex_size} bytes")
    print(f"  JSON: {json_size} bytes")
    print(f"  Savings: {json_size - flex_size} bytes ({(1 - flex_size/json_size)*100:.1f}% smaller)")

    # Cleanup
    cur.execute("DROP TYPE IF EXISTS person CASCADE")

    conn.close()
    print("\n✓ All tests passed! FlexBuffers data is valid.")

if __name__ == "__main__":
    try:
        test_flexbuffers()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
