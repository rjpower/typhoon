
extern "C" {
#include "typhoon_c.h"
}

#include <typhoon/table.h>
#include "inputs.h"

using namespace typhoon;

extern "C" {
Ty_Obj Ty_BlockInput(const char* file, int start, int stop) {
  return (Ty_Obj)NewBlockSource(file, start, stop);
}

Ty_Obj Ty_Select(const char** names) {
  std::vector<std::string> strNames;
  const char** cur = names;
  while (cur) {
    strNames.push_back(*cur);
    ++cur;
  }

//  return (Ty_Obj)NewSelect(strNames);
  return NULL;
}

Ty_Obj Ty_Series(const char* name, int len);
Ty_Obj Ty_GroupBy(const char* name, Ty_Obj combiner);

}
