#!/usr/bin/env python

import os

from cffi import FFI
ffi = FFI()

ffi.set_source('typhoon', '''
#include "typhoon/typhoon_c.h"
''')

ffi.cdef('''
typedef struct Ty_OpaqueTable* Ty_Table;
typedef struct Ty_OpaqueOp* Ty_Operation;
typedef struct Ty_OpaqueCombiner* Ty_Combiner;

Ty_Table Ty_BlockInput(const char* file, int start, int stop);
Ty_Operation Ty_Select(const char** names);
Ty_Operation Ty_Series(const char* name, int len);
Ty_Operation Ty_GroupBy(const char* name, Ty_Combiner combiner);
''')

ffi.compile()
typhoon = ffi.dlopen('./build/latest/libtyphoon.so')

source = typhoon.Ty_BlockInput('./testdata/hs_alt_CHM1_1.1_chr1.fa', 0, 100000)
series = typhoon.Ty_Series('content', 4)
combiner = typhoon.Ty_CountCombiner()
group = Ty_GroupBy('content', count)

iter = Ty_Iterator(group)
while not Ty_Done(iter):
    rows = Ty_Value(iter)
    Ty_Next(iter)