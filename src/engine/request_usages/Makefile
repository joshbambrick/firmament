ROOT_DIR=../../..
SUFFIX=engine/request_usages

include $(ROOT_DIR)/include/Makefile.config

LIBS += -lANN

LIB = libfirmament_engine_request_usages.a
OBJS = request.o usage_record.o usage_record_list.o \
       compared_usage_record_list.o request_usages.o
PBS =
TESTS =

OBJ_LIB = $(addprefix $(OBJ_DIR)/, $(LIB))
TESTS_BIN = $(addprefix $(TEST_OBJ_DIR)/, $(TESTS))

include $(ROOT_DIR)/include/Makefile.common

protobufs: $(addprefix $(OBJ_DIR)/, $(PBS))

all: protobufs .setup $(OBJ_LIB) $(TESTS_BIN)

$(OBJ_LIB): $(addprefix $(OBJ_DIR)/, $(OBJS)) $(addprefix $(OBJ_DIR)/, $(PBS))
