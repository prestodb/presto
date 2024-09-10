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

#ifndef SCD_H
#define SCD_H

#include "decimal.h"
#include "tables.h"

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
