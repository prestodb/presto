/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under external/dsdgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */

#ifndef TDEFS_H
#define TDEFS_H

#include <stdio.h>
#include "columns.h"
#include "dist.h"
#include "tables.h"
#include "tdef_functions.h"

#define tdefIsFlagSet(t, f) (tdefs[t].flags & f)
ds_key_t GetRowcountByName(char* szName, DSDGenContext& dsdGenContext);
int GetTableNumber(char* szName, DSDGenContext& dsdGenContext);
const char* getTableNameByID(int id, DSDGenContext& dsdGenContext);
int getTableFromColumn(int id, DSDGenContext& dsdGenContext);
int initSpareKeys(int id, DSDGenContext& dsdGenContext);
tdef* getSimpleTdefsByNumber(int nTable, DSDGenContext& dsdGenContext);
tdef* getTdefsByNumber(int nTable, DSDGenContext& dsdGenContext);

#endif
