#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/results"
TS="$(date +%Y%m%d_%H%M%S)"
OUT_FILE="${OUT_DIR}/numeric_float_${TS}.out"
RUNS="${RUNS:-10}"
WARMUP="${WARMUP:-3}"

mkdir -p "${OUT_DIR}"

export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-postgres}"
export PGPASSWORD="${PGPASSWORD:-postgres}"
export PGDATABASE="${PGDATABASE:-postgres}"

echo "Comparing numeric_float8 against numeric_out + fast_float"
echo "  runs=${RUNS} warmup=${WARMUP}"
echo "  result columns: backend|avg_ms|min_ms|max_ms|delta_pct"

psql -v ON_ERROR_STOP=1 -v runs="${RUNS}" -v warmup="${WARMUP}" \
  -f "${ROOT_DIR}/bench/numeric_float_backend.sql" | tee "${OUT_FILE}"

ln -sf "$(basename "${OUT_FILE}")" "${OUT_DIR}/numeric_float_latest.out"
echo "Saved benchmark output: ${OUT_FILE}"
