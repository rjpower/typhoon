#ifndef TYPHOON_MASTER_H
#define TYPHOON_MASTER_H

#include "typhoon/common.h"

class Master {
public:
  static bool shouldStart(int argc, char** argv);
  static Master* start(TyphoonConfig config);

  void run();
};

#endif
