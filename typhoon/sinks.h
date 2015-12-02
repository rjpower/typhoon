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

  void write(const ColGroup& rows) {
    if (rows.data.empty()) {
      return;
    }

    for (size_t i = 0; i < rows.data[0].size(); ++i) {
      for (size_t j = 0; j < rows.data.size(); ++j) {
        TypeUtil& typeUtil = typeFromCode(rows.data[j].type);
        fprintf(out_, "%s: %s",
            rows.data[j].name.c_str(),
            typeUtil.toString(rows.data[j], i).c_str());
        if (j != rows.data.size() - 1) {
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
