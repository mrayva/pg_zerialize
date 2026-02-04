#!/bin/bash
set -e

echo "Building pg_zerialize extension..."

# Clean previous builds
make clean 2>/dev/null || true

# Build the extension
make

echo ""
echo "Build successful!"
echo ""
echo "To install:"
echo "  sudo make install"
echo ""
echo "To test:"
echo "  psql -d your_database -f test.sql"
echo ""
echo "Or in psql:"
echo "  CREATE EXTENSION pg_zerialize;"
echo "  SELECT row_to_flexbuffers(ROW('test', 123, true)::record);"
