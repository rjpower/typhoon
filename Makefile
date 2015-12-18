.SUFFIXES:
MAKEFLAGS+=-r

ROOT=.

CAPNP_ROOT=./third_party/capnproto/c++/

PROTO_ROOT=./third_party/grpc/third_party/protobuf/src/
PROTOC=./third_party/grpc/bins/opt/protobuf/protoc

GRPC_ROOT=./third_party/grpc/include
GRPC_CPP_PLUGIN=./third_party/grpc/bins/opt/grpc_cpp_plugin
GRPC_PYTHON_PLUGIN=./third_party/grpc/bins/opt/grpc_python_plugin

SPARSEHASH_ROOT=./third_party/sparsehash/src

INCLUDE=-I${CAPNP_ROOT}/src -I${PROTO_ROOT} -I${GRPC_ROOT} -I${SPARSEHASH_ROOT} -I.

ifeq ($(DBG), 1)
  TARGET=build/dbg
  CXX_FLAGS=-std=c++11 -Wall -fPIC -O0 -g2 ${INCLUDE}
else
  TARGET=build/opt
  CXX_FLAGS=-std=c++11 -Wall -fPIC -DNDEBUG -O3 -g2 ${INCLUDE}
endif

LIBS=-Lthird_party/grpc/libs/opt/\
  -Lthird_party/grpc/third_party/protobuf/src/.libs/\
  -lgrpc++_unsecure -lgrpc_unsecure -lprotobuf -lgpr -lpthread -lz


GENERATED_PROTO=typhoon/typhoon.pb.cc typhoon/typhoon.grpc.pb.cc
SOURCE := ${GENERATED_PROTO}\
  typhoon/table.cc\
  typhoon/inputs.cc\
  typhoon/typhoon_c.cc
  

OBJ := $(patsubst %.cc,${TARGET}/%.o,${SOURCE})
STATIC_LIB := ${TARGET}/libtyphoon.a 



all: ${TARGET}/libtyphoon.a\
  ${TARGET}/libtyphoon.so\
  build/latest\
  ${TARGET}/test/kmer-local

build/latest: 
	-mkdir -p build
	rm -f build/latest
	ln -s $(abspath ${TARGET}) build/latest

${TARGET}/libtyphoon.so: ${OBJ}
	g++ -shared -o $@ $^ ${LIBS}

${STATIC_LIB}: ${OBJ}
	ld -o $@ -r $^
	
${TARGET}/test/kmer-local: ${TARGET}/test/kmer-local.o ${STATIC_LIB}
	g++ -o $@ $^ ${LIBS}

clean:
	rm -rf ${TARGET}/*
	rm -f typhoon/*.pb.*
	rm -f typhoon/*_pb2.py

${TARGET}/%.o : %.cc ${GENERATED_PROTO}
	@mkdir -p ${TARGET}/$(dir $<)
	g++ -MMD -MP -c ${CXX_FLAGS} $< -o $@

${TARGET}/%.o : %.capnp.c++
	@mkdir -p ${TARGET}/$(dir $^)
	g++ -MD -MP -c ${CXX_FLAGS} $^ -o $@

typhoon/%.pb.cc typhoon/%.grpc.pb.cc typhoon/%_pb2.py: typhoon/typhoon.proto
	${PROTOC} ./typhoon/typhoon.proto --grpc_out=. --cpp_out=. --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
	${PROTOC} ./typhoon/typhoon.proto --python_out=. --grpc_out=. --plugin=protoc-gen-grpc=${GRPC_PYTHON_PLUGIN}

	sed -i'' -e 's/import typhoon\.typhoon_pb2//g' ./typhoon/typhoon_pb2.py
	sed -i'' -e 's/typhoon\.typhoon_pb2\.//g' ./typhoon/typhoon_pb2.py

.PHONY: build/latest
-include ${TARGET}/*/*.d
