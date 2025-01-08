MODULE_big = moonshot

OBJS = moonshot.o moonshot_worker.o moonshot_jvm.o moonshot_spi.o

EXTENSION = moonshot
DATA = moonshot--0.0.1.sql
PGFILEDESC = "moonshot - java language handler"

JAVA_HOME=$(shell readlink -f /usr/bin/javac | sed "s:bin/javac::")

PG_CPPFLAGS += -std=c99 -Wno-error=vla -I$(JAVA_HOME)include -I$(JAVA_HOME)include/linux -fvisibility=default

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
