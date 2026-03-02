#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/results"
TS="$(date +%Y%m%d_%H%M%S)"
OUT_FILE="${OUT_DIR}/microbench_isolated_${TS}.out"
RUNS="${RUNS:-5}"
WARMUP="${WARMUP:-1}"
PROTOCOLS="${PROTOCOLS:-msgpack cbor zera flex}"

mkdir -p "${OUT_DIR}"

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"

export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE

echo "Running pg_zerialize microbench (protocol-isolated sessions)..."
echo "  host=${PGHOST} port=${PGPORT} db=${PGDATABASE} user=${PGUSER}"
echo "  runs=${RUNS} warmup=${WARMUP}"
echo "  protocols=${PROTOCOLS}"
echo "  output=${OUT_FILE}"

{
  echo "Running setup SQL..."
  psql -v ON_ERROR_STOP=1 -f "${ROOT_DIR}/bench/microbench_setup.sql"
} | tee "${OUT_FILE}"

TMP_RESULTS="$(mktemp /tmp/microbench_isolated_results.XXXXXX)"
TMP_PROTO="$(mktemp /tmp/microbench_isolated_proto.XXXXXX)"
trap 'rm -f "${TMP_RESULTS}" "${TMP_PROTO}" "${TMP_RESULTS}.final"' EXIT

for proto in ${PROTOCOLS}; do
  var="run_${proto}=1"
  echo
  echo "Running isolated protocol session: ${proto}" | tee -a "${OUT_FILE}"
  psql -v ON_ERROR_STOP=1 -v runs="${RUNS}" -v warmup="${WARMUP}" -v "${var}" \
    -f "${ROOT_DIR}/bench/microbench_protocol.sql" | tee "${TMP_PROTO}" | tee -a "${OUT_FILE}"

  awk -F'|' '/^[a-z_]+\|[0-9]+(\.[0-9]+)?\|[0-9]+(\.[0-9]+)?\|[0-9]+(\.[0-9]+)?\|[0-9]+\|[0-9]+\|/ {print $0}' "${TMP_PROTO}" >> "${TMP_RESULTS}" || true
done

# Keep unique latest record per label (last one wins if duplicate labels appear).
awk -F'|' '{rows[$1]=$0} END {for (k in rows) print rows[k]}' "${TMP_RESULTS}" | sort > "${TMP_RESULTS}.final"

{
  echo
  echo "BENCH_RESULTS_BEGIN"
  cat "${TMP_RESULTS}.final"
  echo "BENCH_RESULTS_END"
} | tee -a "${OUT_FILE}"

echo
echo "Saved benchmark output: ${OUT_FILE}"
echo "Latest symlink update: ${OUT_DIR}/microbench_isolated_latest.out"
ln -sf "$(basename "${OUT_FILE}")" "${OUT_DIR}/microbench_isolated_latest.out"
