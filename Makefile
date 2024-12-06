EXTENSION = pg_incremental

MODULE_big = $(EXTENSION)

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--1.0.sql
SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))
REGRESS = sequence time_interval

PG_CPPFLAGS = -Iinclude
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

# PostgreSQL does not allow declaration after statement, but we do
override CFLAGS := $(filter-out -Wdeclaration-after-statement,$(CFLAGS))
