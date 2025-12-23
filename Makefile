# contrib/yabit/Makefile

MODULE_big = yabit
EXTENSION = yabit
DATA = sql/yabit--0.1.sql
PGFILEDESC = "Yet another Bitmap index method - updatable and applicable to large cardinality columns"

OPTIMIZE = -O0
DEBUG =
PG_CFLAGS += $(OPTIMIZE) $(DEBUG)

# 添加src目录到头文件搜索路径
PG_CPPFLAGS = -I$(srcdir)/src

# 指定所有源文件
OBJS = \
    yabit.o \
    src/bitmap.o \
    src/bitmapattutil.o \
    src/bitmappages.o \
    src/bitmapinsert.o \
    src/bitmapsearch.o \
    src/bitmaputil.o

# 确保子目录被创建
subdirobj = yes

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/yabit
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif