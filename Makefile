# pg_zerialize Makefile

MODULE_big = pg_zerialize
OBJS = pg_zerialize.o

EXTENSION = pg_zerialize
DATA = pg_zerialize--1.0.sql pg_zerialize--1.1.sql pg_zerialize--1.0--1.1.sql
REGRESS = pg_zerialize pg_zerialize_core pg_zerialize_parity pg_zerialize_cache pg_zerialize_deterministic pg_zerialize_builders pg_zerialize_builders_semantics pg_zerialize_semantics_exhaustive

# C++ compilation flags
PG_CPPFLAGS = -std=c++20 -fPIC -Ivendor/zerialize/include
SHLIB_LINK = -lstdc++ -lflatbuffers

# Use C++ compiler
CC = g++
CXX = g++

# Override CFLAGS to remove gcc-specific flags that don't work with g++
override CFLAGS :=

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Override the implicit rule to use C++ compiler
%.o: %.cpp
	$(CXX) $(PG_CPPFLAGS) $(CPPFLAGS) -c -o $@ $<

.PHONY: bench bench-quick bench-isolated bench-isolated-quick

bench:
	./bench/run_microbench.sh

bench-quick:
	RUNS=3 WARMUP=1 ./bench/run_microbench.sh

bench-isolated:
	./bench/run_microbench_isolated.sh

bench-isolated-quick:
	RUNS=3 WARMUP=1 ./bench/run_microbench_isolated.sh
