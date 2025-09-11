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

#ifndef SCD_H
#define SCD_H

#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"

extern char arBKeys[MAX_TABLE][17];
int setSCDKeys(
    int nTableID,
    ds_key_t hgIndex,
    char* szBKey,
    ds_key_t* hgBeginDateKey,
    ds_key_t* hgEndDateKey,
    DSDGenContext& dsdGenContext);
ds_key_t
scd_join(int tbl, int col, ds_key_t jDate, DSDGenContext& dsdGenContext);
ds_key_t matchSCDSK(
    ds_key_t kUnique,
    ds_key_t jDate,
    int nTable,
    DSDGenContext& dsdGenContext);
ds_key_t getSKFromID(ds_key_t kID, int nColumn, DSDGenContext& dsdGenContext);
ds_key_t getFirstSK(ds_key_t kID);
/*
 * handle the partial change of a history keeping record
 * TODO: remove the macros in favor of straight fucntion calls
 */
#define SCD_INT 0
#define SCD_CHAR 1
#define SCD_DEC 2
#define SCD_KEY 3
#define SCD_PTR 4
void changeSCD(
    int nDataType,
    void* pNewData,
    void* pOldData,
    int* nFlags,
    int bFirst);
int validateSCD(int nTable, ds_key_t kRow, int* Permutation);
void printValidation(int nTable, ds_key_t kRow);
#endif
