# pg_zerialize Makefile

MODULE_big = pg_zerialize
OBJS = pg_zerialize.o

EXTENSION = pg_zerialize
DATA = pg_zerialize--1.0.sql

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
