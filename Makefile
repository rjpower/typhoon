ROOT=.
PROTO_ROOT=${ROOT}/third_party/capnproto/c++

INCLUDE=-I${PROTO_ROOT}/src -I.
CXX_FLAGS=-std=c++11 -Wall -fPIC -DNDEBUG -O2 ${INCLUDE}
GENERATED_PROTO=typhoon/proto.capnp.h
HEADERS=$(wildcard typhoon/*.h)

all: build/typhoon/proto.o build/typhoon/worker.o build/typhoon/master.o build/typhoon/common.o build/contrib/kmer.o

clean:
	rm -rf build/*
	rm typhoon/proto.capnp.*

build/%.o : %.cc ${GENERATED_PROTO} ${HEADERS}
	mkdir -p build/$(dir $<)
	g++ -c ${CXX_FLAGS} $< -o $@

build/%.o : %.capnp.c++
	mkdir -p build/$(dir $^)
	g++ -c ${CXX_FLAGS} $^ -o $@

typhoon/proto.capnp.h typhoon/proto.capnp.c++: typhoon/proto.capnp
	PATH=${PATH}:${PROTO_ROOT} ${PROTO_ROOT}/capnp compile ./typhoon/proto.capnp --output=c++:./

