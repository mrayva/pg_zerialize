# pg_zerialize Makefile

MODULE_big = pg_zerialize
OBJS = pg_zerialize.o

EXTENSION = pg_zerialize
DATA = pg_zerialize--1.0.sql pg_zerialize--1.1.sql pg_zerialize--1.2.sql \
	pg_zerialize--1.0--1.1.sql pg_zerialize--1.1--1.2.sql
REGRESS = pg_zerialize pg_zerialize_core pg_zerialize_parity pg_zerialize_cache pg_zerialize_deterministic pg_zerialize_builders pg_zerialize_builders_semantics pg_zerialize_semantics_exhaustive pg_zerialize_upgrade

# C++ compilation flags
PG_CPPFLAGS = -std=c++20 -fPIC -Ivendor/zerialize/include
SHLIB_LINK = -lstdc++ -lflatbuffers

# Use C++ compiler
CC = g++
CXX = g++

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# PGXS links MODULE_big with the C driver; avoid passing C-only warning flags
# from PostgreSQL's build into that link. C++ compilation uses CXXFLAGS below.
override CFLAGS :=

# Compile C++ with PostgreSQL's release, warning, and hardening flags.
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<

pg_zerialize.o pg_zerialize.bc: vendor/zerialize/include/zerialize/protocols/msgpack.hpp

.PHONY: bench bench-quick bench-isolated bench-isolated-quick bench-numeric-float semantic-check

semantic-check:
	python3 test/semantic_roundtrip.py

bench:
	./bench/run_microbench.sh

bench-quick:
	RUNS=3 WARMUP=1 ./bench/run_microbench.sh

bench-isolated:
	./bench/run_microbench_isolated.sh

bench-isolated-quick:
	RUNS=3 WARMUP=1 ./bench/run_microbench_isolated.sh

bench-numeric-float:
	./bench/run_numeric_float_bench.sh
