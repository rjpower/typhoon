#include "typhoon/worker.h"
#include "typhoon/colgroup.h"

#include <execinfo.h>

void handler(int sig) {
  void *array[64];
  size_t size = backtrace(array, 64);
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

int main(int argc, char** argv) {
  signal(SIGSEGV, handler);
  signal(SIGABRT, handler);
  Worker w;
  w.run();
}
