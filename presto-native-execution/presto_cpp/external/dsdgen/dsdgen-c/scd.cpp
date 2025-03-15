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

#include "scd.h"
#include <stdio.h>
#include "build_support.h"
#include "config.h"
#include "constants.h"
#include "dist.h"
#include "genrand.h"
#include "parallel.h"
#include "params.h"
#include "permute.h"
#include "porting.h"
#include "scaling.h"
#include "tables.h"
#include "tdef_functions.h"
#include "tdefs.h"

/* an array of the most recent business key for each table */
char arBKeys[MAX_TABLE][17];

/*
 * Routine: setSCDKey
 * Purpose: handle the versioning and date stamps for slowly changing dimensions
 * Algorithm:
 * Data Structures:
 *
 * Params: 1 if there is a new id; 0 otherwise
 * Returns:
 * Called By:
 * Calls:
 * Assumptions: Table indexs (surrogate keys) are 1-based. This assures that the
 *arBKeys[] entry for each table is initialized. Otherwise, parallel generation
 *would be more difficult. Side Effects:
 * TODO: None
 */
int setSCDKeys(
    int nColumnID,
    ds_key_t kIndex,
    char* szBKey,
    ds_key_t* pkBeginDateKey,
    ds_key_t* pkEndDateKey,
    DSDGenContext& dsdGenContext) {
  int bNewBKey = 0, nModulo;
  ds_key_t jMinimumDataDate, jMaximumDataDate, jH1DataDate, jT1DataDate,
      jT2DataDate;
  date_t dtTemp;
  int nTableID;

  strtodt(&dtTemp, DATA_START_DATE);
  jMinimumDataDate = dtTemp.julian;
  strtodt(&dtTemp, DATA_END_DATE);
  jMaximumDataDate = dtTemp.julian;
  jH1DataDate = jMinimumDataDate + (jMaximumDataDate - jMinimumDataDate) / 2;
  jT2DataDate = (jMaximumDataDate - jMinimumDataDate) / 3;
  jT1DataDate = jMinimumDataDate + jT2DataDate;
  jT2DataDate += jT1DataDate;

  nTableID = getTableFromColumn(nColumnID, dsdGenContext);
  nModulo = (int)(kIndex % 6);
  switch (nModulo) {
    case 1: /* 1 revision */
      mk_bkey(arBKeys[nTableID], kIndex, nColumnID);
      bNewBKey = 1;
      *pkBeginDateKey = jMinimumDataDate - nTableID * 6;
      *pkEndDateKey = -1;
      break;
    case 2: /* 1 of 2 revisions */
      mk_bkey(arBKeys[nTableID], kIndex, nColumnID);
      bNewBKey = 1;
      *pkBeginDateKey = jMinimumDataDate - nTableID * 6;
      *pkEndDateKey = jH1DataDate - nTableID * 6;
      break;
    case 3: /* 2 of 2 revisions */
      mk_bkey(arBKeys[nTableID], kIndex - 1, nColumnID);
      *pkBeginDateKey = jH1DataDate - nTableID * 6 + 1;
      *pkEndDateKey = -1;
      break;
    case 4: /* 1 of 3 revisions */
      mk_bkey(arBKeys[nTableID], kIndex, nColumnID);
      bNewBKey = 1;
      *pkBeginDateKey = jMinimumDataDate - nTableID * 6;
      *pkEndDateKey = jT1DataDate - nTableID * 6;
      break;
    case 5: /* 2 of 3 revisions */
      mk_bkey(arBKeys[nTableID], kIndex - 1, nColumnID);
      *pkBeginDateKey = jT1DataDate - nTableID * 6 + 1;
      *pkEndDateKey = jT2DataDate - nTableID * 6;
      break;
    case 0: /* 3 of 3 revisions */
      mk_bkey(arBKeys[nTableID], kIndex - 2, nColumnID);
      *pkBeginDateKey = jT2DataDate - nTableID * 6 + 1;
      *pkEndDateKey = -1;
      break;
  }

  /* can't have a revision in the future, per bug 114 */
  if (*pkEndDateKey > jMaximumDataDate)
    *pkEndDateKey = -1;

  strcpy(szBKey, arBKeys[nTableID]);

  return (bNewBKey);
}

/*
 * Routine: scd_join(int tbl, int col, ds_key_t jDate)
 * Purpose: create joins to slowly changing dimensions
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
ds_key_t
scd_join(int tbl, int col, ds_key_t jDate, DSDGenContext& dsdGenContext) {
  ds_key_t res, kRowcount;
  static int jMinimumDataDate, jMaximumDataDate, jH1DataDate, jT1DataDate,
      jT2DataDate;
  date_t dtTemp;

  if (!dsdGenContext.scd_join_init) {
    strtodt(&dtTemp, DATA_START_DATE);
    jMinimumDataDate = dtTemp.julian;
    strtodt(&dtTemp, DATA_END_DATE);
    jMaximumDataDate = dtTemp.julian;
    jH1DataDate = jMinimumDataDate + (jMaximumDataDate - jMinimumDataDate) / 2;
    jT2DataDate = (jMaximumDataDate - jMinimumDataDate) / 3;
    jT1DataDate = jMinimumDataDate + jT2DataDate;
    jT2DataDate += jT1DataDate;
    dsdGenContext.scd_join_init = 1;
  }

  kRowcount = getIDCount(tbl, dsdGenContext);
  genrand_key(
      &res,
      DIST_UNIFORM,
      1,
      kRowcount,
      0,
      col,
      dsdGenContext); /* pick the id */
  res = matchSCDSK(
      res,
      jDate,
      tbl,
      dsdGenContext); /* map to the date-sensitive surrogate key */

  /* can't have a revision in the future, per bug 114 */
  if (jDate > jMaximumDataDate)
    res = -1;

  return ((res > get_rowcount(tbl, dsdGenContext)) ? -1 : res);
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
ds_key_t matchSCDSK(
    ds_key_t kUnique,
    ds_key_t jDate,
    int nTable,
    DSDGenContext& dsdGenContext) {
  ds_key_t kReturn = -1;
  int jMinimumDataDate, jMaximumDataDate;
  int jH1DataDate, jT1DataDate, jT2DataDate;
  date_t dtTemp;

  strtodt(&dtTemp, DATA_START_DATE);
  jMinimumDataDate = dtTemp.julian;
  strtodt(&dtTemp, DATA_END_DATE);
  jMaximumDataDate = dtTemp.julian;
  jH1DataDate = jMinimumDataDate + (jMaximumDataDate - jMinimumDataDate) / 2;
  jT2DataDate = (jMaximumDataDate - jMinimumDataDate) / 3;
  jT1DataDate = jMinimumDataDate + jT2DataDate;
  jT2DataDate += jT1DataDate;

  switch (kUnique % 3) /* number of revisions for the ID */
  {
    case 1: /* only one occurrence of this ID */
      kReturn = (kUnique / 3) * 6;
      kReturn += 1;
      break;
    case 2: /* two revisions of this ID */
      kReturn = (kUnique / 3) * 6;
      kReturn += 2;
      if (jDate > jH1DataDate)
        kReturn += 1;
      break;
    case 0: /* three revisions of this ID */
      kReturn = (kUnique / 3) * 6;
      kReturn += -2;
      if (jDate > jT1DataDate)
        kReturn += 1;
      if (jDate > jT2DataDate)
        kReturn += 1;
      break;
  }

  if (kReturn > get_rowcount(nTable, dsdGenContext))
    kReturn = get_rowcount(nTable, dsdGenContext);

  return (kReturn);
}

/*
 * Routine:
 * Purpose: map from a unique ID to a random SK
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
ds_key_t getSKFromID(ds_key_t kID, int nColumn, DSDGenContext& dsdGenContext) {
  ds_key_t kTemp = -1;

  switch (kID % 3) {
    case 1: /* single revision */
      kTemp = kID / 3;
      kTemp *= 6;
      kTemp += 1;
      break;
    case 2: /* two revisions */
      kTemp = kID / 3;
      kTemp *= 6;
      kTemp +=
          genrand_integer(NULL, DIST_UNIFORM, 2, 3, 0, nColumn, dsdGenContext);
      break;
    case 0: /* three revisions */
      kTemp = kID / 3;
      kTemp -= 1;
      kTemp *= 6;
      kTemp +=
          genrand_integer(NULL, DIST_UNIFORM, 4, 6, 0, nColumn, dsdGenContext);
      break;
  }

  return (kTemp);
}

/*
 * Routine: getFirstSK
 * Purpose: map from id to an SK that can be mapped back to an id by printID()
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
ds_key_t getFirstSK(ds_key_t kID) {
  ds_key_t kTemp = -1;

  switch (kID % 3) {
    case 1: /* single revision */
      kTemp = kID / 3;
      kTemp *= 6;
      kTemp += 1;
      break;
    case 2: /* two revisions */
      kTemp = kID / 3;
      kTemp *= 6;
      kTemp += 2;
      break;
    case 0: /* three revisions */
      kTemp = kID / 3;
      kTemp -= 1;
      kTemp *= 6;
      kTemp += 4;
      break;
  }

  return (kTemp);
}

/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void changeSCD(
    int nDataType,
    void* pNewData,
    void* pOldData,
    int* nFlags,
    int bFirst) {
  /**
   * if nFlags is odd, then this value will be retained
   */
  if ((*nFlags != ((*nFlags / 2) * 2)) && (bFirst == 0)) {
    /*
     * the method to retain the old value depends on the data type
     */
    switch (nDataType) {
      case SCD_INT:
        *static_cast<int*>(pNewData) = *static_cast<int*>(pOldData);
        break;
      case SCD_PTR:
        pNewData = pOldData;
        break;
      case SCD_KEY:
        *static_cast<ds_key_t*>(pNewData) = *static_cast<ds_key_t*>(pOldData);
        break;
      case SCD_CHAR:
        strcpy(static_cast<char*>(pNewData), static_cast<char*>(pOldData));
        break;
      case SCD_DEC:
        if (pNewData && pOldData)
          memcpy(pNewData, pOldData, sizeof(decimal_t));
        break;
    }
  } else {
    /*
     * the method to set the old value depends on the data type
     */
    switch (nDataType) {
      case SCD_INT:
        *static_cast<int*>(pOldData) = *static_cast<int*>(pNewData);
        break;
      case SCD_PTR:
        pOldData = pNewData;
        break;
      case SCD_KEY:
        *static_cast<ds_key_t*>(pOldData) = *static_cast<ds_key_t*>(pNewData);
        break;
      case SCD_CHAR:
        strcpy(static_cast<char*>(pOldData), static_cast<char*>(pNewData));
        break;
      case SCD_DEC:
        if (pOldData && pNewData)
          memcpy(pOldData, pNewData, sizeof(decimal_t));
        break;
    }
  }

  *nFlags /= 2;

  return;
}
