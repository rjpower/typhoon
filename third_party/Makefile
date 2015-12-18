default: ./capnproto/c++/.libs/libkj.a grpc/libs/opt/libgrpc.a

capnproto/c++/.libs/libkj.a:
	cd capnproto/c++ &&\
	autoreconf -i &&\
	./configure --disable-shared --disable-fast-install --disable-dependency-tracking --with-pic &&\
	make -j4

grpc/libs/opt/libgrpc.a:
	cd grpc && git submodule update --init
	cd grpc && make
	find . -name '*.dylib' -exec rm {} \;

.PHONY: default capnproto
