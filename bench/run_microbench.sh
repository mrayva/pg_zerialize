#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/results"
TS="$(date +%Y%m%d_%H%M%S)"
OUT_FILE="${OUT_DIR}/microbench_${TS}.out"
RUNS="${RUNS:-5}"
WARMUP="${WARMUP:-1}"

mkdir -p "${OUT_DIR}"

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"

export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE

echo "Running pg_zerialize microbench..."
echo "  host=${PGHOST} port=${PGPORT} db=${PGDATABASE} user=${PGUSER}"
echo "  runs=${RUNS} warmup=${WARMUP}"
echo "  output=${OUT_FILE}"

psql -v ON_ERROR_STOP=1 -v runs="${RUNS}" -v warmup="${WARMUP}" \
  -f "${ROOT_DIR}/bench/microbench.sql" | tee "${OUT_FILE}"

echo
echo "Saved benchmark output: ${OUT_FILE}"
echo "Latest symlink update: ${OUT_DIR}/microbench_latest.out"
ln -sf "$(basename "${OUT_FILE}")" "${OUT_DIR}/microbench_latest.out"
