/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under tpcds/gen/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#ifndef TDEF_FUNCTIONS_H
#define TDEF_FUNCTIONS_H
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"

/*
* table functions.
* NOTE: This table contains the function declarations in the table descriptions;
it must be kept in sync with the
*    declararions of assocaited constants, found in tdefs.h

*/
typedef struct TABLE_FUNC_T {
  const char* name; /* -- name of the table; */
  int (*builder)(
      void*,
      ds_key_t,
      DSDGenContext& dsdGenContext); /* -- function to prep output */
  int (*loader[2])(void*); /* -- functions to present output */
  /* -- data validation function */
  int (*validate)(int nTable, ds_key_t kRow, int* Permutation);
} table_func_t;

extern table_func_t w_tdef_funcs[MAX_TABLE];
extern table_func_t s_tdef_funcs[MAX_TABLE];
extern table_func_t* tdef_funcs;

int validateGeneric(int nTable, ds_key_t kRow, int* Permutation);
int validateSCD(int nTable, ds_key_t kRow, int* Permutation);

#endif /* TDEF_FUNCTIONS_H */
extern table_func_t s_tdef_funcs[];
extern table_func_t w_tdef_funcs[];

table_func_t* getTdefFunctionsByNumber(int nTable);
