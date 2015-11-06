#ifndef TYPHOON_MASTER_H
#define TYPHOON_MASTER_H

#include "typhoon/common.h"
#include "typhoon/typhoon.grpc.pb.h"

#include "grpc++/grpc++.h"
#include <stdlib.h>


class Master {
public:
  static bool shouldStart(int argc, char** argv) {
    return getenv("MASTER") != NULL;
  }

  static Master* start(TyphoonConfig config) {
    return new Master;
  }

  void run() {
    MapRequest request;
    MapResponse response;
    grpc::ClientContext context;

    auto worker = TyphoonWorker::Stub(
        grpc::CreateChannel("localhost:50051", grpc::InsecureCredentials())
    );
    grpc::Status status = worker.map(&context, request, &response);
  }
};

#endif
