ROOT=.

CAPNP_ROOT=${ROOT}/third_party/capnproto/c++
CAPNP=${CAPNP_ROOT}/capnp

PROTO_ROOT=./third_party/grpc/third_party/protobuf/src/
PROTOC=./third_party/grpc/bins/opt/protobuf/protoc

GRPC_ROOT=./third_party/grpc/include
GRPC_PLUGIN=./third_party/grpc/bins/opt/grpc_cpp_plugin

INCLUDE=-I${CAPNP_ROOT}/src -I${PROTO_ROOT} -I. -I${GRPC_ROOT}
CXX_FLAGS=-std=c++11 -Wall -fPIC -DNDEBUG -O0 -g2 ${INCLUDE}

LIBS=-Lthird_party/grpc/libs/opt/ -Lthird_party/grpc/third_party/protobuf/src/.libs/\
  -lgrpc++_unsecure -lgrpc_unsecure -lprotobuf -lgpr -lpthread -lz


GENERATED_PROTO=typhoon/typhoon.pb.h
HEADERS=$(wildcard typhoon/*.h)

all: build/libtyphoon.a build/wordcount 

build/wordcount: build/libtyphoon.a build/contrib/count.o
	g++ -o $@ $^ ${LIBS}

build/libtyphoon.a: \
  build/typhoon/typhoon.pb.o \
  build/typhoon/typhoon.grpc.pb.o \
  build/typhoon/worker.o \
  build/typhoon/master.o \
  build/typhoon/common.o
	ar cr $@ $^ 

clean:
	rm -rf build/*
	rm -f typhoon/proto.pb.*

build/%.o : %.cc ${GENERATED_PROTO} ${HEADERS}
	mkdir -p build/$(dir $<)
	g++ -c ${CXX_FLAGS} $< -o $@

build/%.o : %.capnp.c++
	mkdir -p build/$(dir $^)
	g++ -c ${CXX_FLAGS} $^ -o $@

typhoon/proto.capnp.h typhoon/proto.capnp.c++: typhoon/proto.capnp
	PATH=${PATH}:${PROTO_ROOT} ${CAPNP} compile ./typhoon/proto.capnp --output=c++:./

${GENERATED_PROTO}: typhoon/typhoon.proto
	${PROTOC} ./typhoon/typhoon.proto --cpp_out=.
	${PROTOC} ./typhoon/typhoon.proto --plugin=protoc-gen-grpc=${GRPC_PLUGIN} --grpc_out=.

