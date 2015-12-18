#include "typhoon/common.h"
#include <stdio.h>

class TextSink : public Sink {
private:
  FILE* out_;

public:
  TextSink() {
    out_ = fopen("./testdata/counts.txt", "w");
  }

  virtual ~TextSink() {
    fclose(out_);
  }

  void write(const Table& rows) {
    for (size_t i = 0; i < rows.length(); ++i) {
      for (size_t j = 0; j < rows.cols(); ++j) {
        const Column& col = rows.col(j);
        fprintf(out_, "%s: %s", col.name.c_str(), col.toString(i).c_str());
        if (j != rows.cols() - 1) {
          fprintf(out_, " ");
        }
      }
      fprintf(out_, "\n");
    }
  }

  virtual void flush() {
    fflush(out_);
  }
};
