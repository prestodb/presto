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

#ifndef TDEFS_H
#define TDEFS_H

#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdef_functions.h"

#define tdefIsFlagSet(t, f) (tdefs[t].flags & f)
ds_key_t GetRowcountByName(char* szName, DSDGenContext& dsdGenContext);
int GetTableNumber(char* szName, DSDGenContext& dsdGenContext);
const char* getTableNameByID(int id, DSDGenContext& dsdGenContext);
int getTableFromColumn(int id, DSDGenContext& dsdGenContext);
int initSpareKeys(int id, DSDGenContext& dsdGenContext);
tdef* getSimpleTdefsByNumber(int nTable, DSDGenContext& dsdGenContext);
tdef* getTdefsByNumber(int nTable, DSDGenContext& dsdGenContext);

#endif
