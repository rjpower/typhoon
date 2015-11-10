ROOT=.

PROTO_ROOT=./third_party/grpc/third_party/protobuf/src/
PROTOC=./third_party/grpc/bins/opt/protobuf/protoc

GRPC_ROOT=./third_party/grpc/include
GRPC_CPP_PLUGIN=./third_party/grpc/bins/opt/grpc_cpp_plugin
GRPC_PYTHON_PLUGIN=./third_party/grpc/bins/opt/grpc_python_plugin

INCLUDE=-I${CAPNP_ROOT}/src -I${PROTO_ROOT} -I. -I${GRPC_ROOT}
CXX_FLAGS=-std=c++11 -Wall -fPIC -DNDEBUG -O0 -g2 ${INCLUDE}

LIBS=-Lthird_party/grpc/libs/opt/ -Lthird_party/grpc/third_party/protobuf/src/.libs/\
  -lgrpc++_unsecure -lgrpc_unsecure -lprotobuf -lgpr -lpthread -lz


GENERATED_PROTO=typhoon/typhoon.pb.h
HEADERS := $(wildcard typhoon/*.h)
SOURCE := $(wildcard typhoon/*.cc) $(wildcard typhoon/*.c++)
OBJ := $(patsubst %.cc,build/%.o,${SOURCE})

all: build/libtyphoon.a build/wordcount

build/wordcount: build/libtyphoon.a build/contrib/count.o
	g++ -o $@ $^ ${LIBS}

build/libtyphoon.a: ${OBJ}
	ld -o $@ -r $^ 

clean:
	rm -rf build/*
	rm -f typhoon/*.pb.*
	rm -f typhoon/*_pb2.py

build/%.o : %.cc ${GENERATED_PROTO} ${HEADERS}
	@mkdir -p build/$(dir $<)
	g++ -c ${CXX_FLAGS} $< -o $@

build/%.o : %.capnp.c++
	@mkdir -p build/$(dir $^)
	g++ -c ${CXX_FLAGS} $^ -o $@

${GENERATED_PROTO}: typhoon/typhoon.proto
	${PROTOC} ./typhoon/typhoon.proto --grpc_out=. --cpp_out=. --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
	${PROTOC} ./typhoon/typhoon.proto --python_out=. --grpc_out=. --plugin=protoc-gen-grpc=${GRPC_PYTHON_PLUGIN}
	
	sed -i -e 's/import typhoon\.typhoon_pb2//g' ./typhoon/typhoon_pb2.py
	sed -i -e 's/typhoon\.typhoon_pb2\.//g' ./typhoon/typhoon_pb2.py

