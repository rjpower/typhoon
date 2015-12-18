#ifndef TYPHOON_C_H
#define TYPHOON_C_H

typedef struct Ty_Opaque* Ty_Obj;

Ty_Obj Ty_BlockInput(const char* file, int start, int stop);
Ty_Obj Ty_Select(const char** names);
Ty_Obj Ty_Series(const char* name, int len);
Ty_Obj Ty_GroupBy(const char* name, Ty_Obj combiner);
Ty_Obj Ty_CountCombiner();

const char* Ty_NumColumns(Ty_Obj table);
const char* Ty_ColumnName(Ty_Obj table, int);

Ty_Obj Ty_Iterator(Ty_Obj table);

#endif
