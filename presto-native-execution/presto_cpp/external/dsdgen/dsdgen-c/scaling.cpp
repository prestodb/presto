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

#include "scaling.h"
#include <assert.h>
#include <stdio.h>
#include <cmath>
#include "columns.h"
#include "config.h"
#include "constants.h"
#include "dist.h"
#include "error_msg.h"
#include "genrand.h"
#include "parallel.h"
#include "porting.h"
#include "r_params.h"
#include "scd.h"
#include "tdef_functions.h"
#include "tdefs.h"
#include "tpcds.idx.h"
#include "w_inventory.h"

void setUpdateScaling(int table, DSDGenContext& dsdGenContext);
int row_skip(int tbl, ds_key_t count, DSDGenContext& dsdGenContext);

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
int getScaleSlot(int nTargetGB, DSDGenContext& dsdGenContext) {
  int i;

  for (i = 0; nTargetGB > dsdGenContext.arScaleVolume[i]; i++)
    ;

  return (i);
}

/*
 * Routine: LogScale(void)
 * Purpose: use the command line volume target, in GB, to calculate the global
 * rowcount multiplier Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects: arRowcounts are set to the appropriate number of rows for the
 * target scale factor
 * TODO: None
 */
static ds_key_t
LogScale(int nTable, int nTargetGB, DSDGenContext& dsdGenContext) {
  int nIndex = 1, nDelta, i;
  float fOffset;
  ds_key_t hgRowcount = 0;

  i = getScaleSlot(nTargetGB, dsdGenContext);

  nDelta = dist_weight(NULL, "rowcounts", nTable + 1, i + 1, dsdGenContext) -
      dist_weight(NULL, "rowcounts", nTable + 1, i, dsdGenContext);
  fOffset =
      static_cast<float>(nTargetGB - dsdGenContext.arScaleVolume[i - 1]) /
      static_cast<float>(
          dsdGenContext.arScaleVolume[i] - dsdGenContext.arScaleVolume[i - 1]);

  hgRowcount = static_cast<int>(fOffset * static_cast<float>(nDelta));
  hgRowcount +=
      dist_weight(NULL, "rowcounts", nTable + 1, nIndex, dsdGenContext);

  return (hgRowcount);
}

/*
 * Routine: StaticScale(void)
 * Purpose: use the command line volume target, in GB, to calculate the global
 * rowcount multiplier Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects: arRowcounts are set to the appropriate number of rows for the
 * target scale factor
 * TODO: None
 */
static ds_key_t
StaticScale(int nTable, int nTargetGB, DSDGenContext& dsdGenContext) {
  return (dist_weight(NULL, "rowcounts", nTable + 1, 1, dsdGenContext));
}

/*
 * Routine: LinearScale(void)
 * Purpose: use the command line volume target, in GB, to calculate the global
 *rowcount multiplier Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions: scale factors defined in rowcounts distribution define
 *1/10/100/1000/... GB with sufficient accuracy Side Effects: arRowcounts are
 *set to the appropriate number of rows for the target scale factor
 * TODO: None
 */
static ds_key_t
LinearScale(int nTable, int nTargetGB, DSDGenContext& dsdGenContext) {
  int i;
  ds_key_t hgRowcount = 0;

  for (i = 8; i >= 0; i--) /* work from large scales down)*/
  {
    /*
     * use the defined rowcounts to build up the target GB volume
     */
    while (nTargetGB >= dsdGenContext.arScaleVolume[i]) {
      hgRowcount +=
          dist_weight(NULL, "rowcounts", nTable + 1, i + 1, dsdGenContext);
      nTargetGB -= dsdGenContext.arScaleVolume[i];
    }
  }

  return (hgRowcount);
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
ds_key_t getIDCount(int nTable, DSDGenContext& dsdGenContext) {
  ds_key_t kRowcount, kUniqueCount;
  tdef* pTdef;

  kRowcount = get_rowcount(nTable, dsdGenContext);
  if (nTable >= PSEUDO_TABLE_START)
    return (kRowcount);
  pTdef = getSimpleTdefsByNumber(nTable, dsdGenContext);
  if (pTdef->flags & FL_TYPE_2) {
    kUniqueCount = (kRowcount / 6) * 3;
    switch (kRowcount % 6) {
      case 1:
        kUniqueCount += 1;
        break;
      case 2:
      case 3:
        kUniqueCount += 2;
        break;
      case 4:
      case 5:
        kUniqueCount += 3;
        break;
    }
    return (kUniqueCount);
  } else {
    return (kRowcount);
  }
}

/*
 * Routine: get_rowcount(int table)
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
 * TODO: 20040820 jms Need to address special case scaling in a more general
 * fashion
 */
ds_key_t get_rowcount(int table, DSDGenContext& dsdGenContext) {
  static double nScale;
  int nTable, nMultiplier, i, nBadScale = 0, nRowcountOffset = 0;
  tdef* pTdef;

  if (!dsdGenContext.get_rowcount_init) {
    nScale = get_dbl("SCALE", dsdGenContext);
    if (nScale > 100000)
      ReportErrorNoLine(QERR_BAD_SCALE, NULL, 1);

    memset(dsdGenContext.arRowcount, 0, sizeof(long) * MAX_TABLE);
    int iScale = nScale < 1 ? 1 : static_cast<int>(nScale);
    for (nTable = CALL_CENTER; nTable <= MAX_TABLE; nTable++) {
      switch (iScale) {
        case 100000:
          dsdGenContext.arRowcount[nTable].kBaseRowcount = dist_weight(
              NULL,
              "rowcounts",
              nTable + nRowcountOffset + 1,
              9,
              dsdGenContext);
          break;
        case 30000:
          dsdGenContext.arRowcount[nTable].kBaseRowcount = dist_weight(
              NULL,
              "rowcounts",
              nTable + nRowcountOffset + 1,
              8,
              dsdGenContext);
          break;
        case 10000:
          dsdGenContext.arRowcount[nTable].kBaseRowcount = dist_weight(
              NULL,
              "rowcounts",
              nTable + nRowcountOffset + 1,
              7,
              dsdGenContext);
          break;
        case 3000:
          dsdGenContext.arRowcount[nTable].kBaseRowcount = dist_weight(
              NULL,
              "rowcounts",
              nTable + nRowcountOffset + 1,
              6,
              dsdGenContext);
          break;
        case 1000:
          dsdGenContext.arRowcount[nTable].kBaseRowcount = dist_weight(
              NULL,
              "rowcounts",
              nTable + nRowcountOffset + 1,
              5,
              dsdGenContext);
          break;
        case 300:
          nBadScale = QERR_BAD_SCALE;
          dsdGenContext.arRowcount[nTable].kBaseRowcount = dist_weight(
              NULL,
              "rowcounts",
              nTable + nRowcountOffset + 1,
              4,
              dsdGenContext);
          break;
        case 100:
          nBadScale = QERR_BAD_SCALE;
          dsdGenContext.arRowcount[nTable].kBaseRowcount = dist_weight(
              NULL,
              "rowcounts",
              nTable + nRowcountOffset + 1,
              3,
              dsdGenContext);
          break;
        case 10:
          nBadScale = QERR_BAD_SCALE;
          dsdGenContext.arRowcount[nTable].kBaseRowcount = dist_weight(
              NULL,
              "rowcounts",
              nTable + nRowcountOffset + 1,
              2,
              dsdGenContext);
          break;
        case 1:
          nBadScale = QERR_QUALIFICATION_SCALE;
          dsdGenContext.arRowcount[nTable].kBaseRowcount = dist_weight(
              NULL,
              "rowcounts",
              nTable + nRowcountOffset + 1,
              1,
              dsdGenContext);
          break;
        default:
          nBadScale = QERR_BAD_SCALE;
          int mem =
              dist_member(NULL, "rowcounts", nTable + 1, 3, dsdGenContext);
          switch (mem) {
            case 2:
              dsdGenContext.arRowcount[nTable].kBaseRowcount =
                  LinearScale(nTable + nRowcountOffset, nScale, dsdGenContext);
              break;
            case 1:
              dsdGenContext.arRowcount[nTable].kBaseRowcount =
                  StaticScale(nTable + nRowcountOffset, nScale, dsdGenContext);
              break;
            case 3:
              dsdGenContext.arRowcount[nTable].kBaseRowcount =
                  LogScale(nTable + nRowcountOffset, nScale, dsdGenContext);
              break;
          } /* switch(FL_SCALE_MASK) */
          break;
      } /* switch(nScale) */

      /* now adjust for the multiplier */
      nMultiplier = 1;
      if (nTable < PSEUDO_TABLE_START) {
        pTdef = getSimpleTdefsByNumber(nTable, dsdGenContext);
        nMultiplier = (pTdef->flags & FL_TYPE_2) ? 2 : 1;
      }
      for (i = 1;
           i <= dist_member(NULL, "rowcounts", nTable + 1, 2, dsdGenContext);
           i++) {
        nMultiplier *= 10;
      }
      if (dsdGenContext.arRowcount[nTable].kBaseRowcount >= 0) {
        if (nScale < 1) {
          int mem =
              dist_member(NULL, "rowcounts", nTable + 1, 3, dsdGenContext);
          if ((mem > 1)) {
            dsdGenContext.arRowcount[nTable].kBaseRowcount = static_cast<int>(
                dsdGenContext.arRowcount[nTable].kBaseRowcount * nScale);
          }
          if (dsdGenContext.arRowcount[nTable].kBaseRowcount == 0) {
            dsdGenContext.arRowcount[nTable].kBaseRowcount = 1;
          }
        }
      }
      dsdGenContext.arRowcount[nTable].kBaseRowcount *= nMultiplier;
    } /* for each table */

    //		if (nBadScale && !is_set("QUIET"))
    //			ReportErrorNoLine(nBadScale, NULL, 0);

    dsdGenContext.get_rowcount_init = 1;
  }

  if (table == INVENTORY)
    return (sc_w_inventory(nScale, dsdGenContext));
  if (table == S_INVENTORY)
    return (
        getIDCount(ITEM, dsdGenContext) *
        get_rowcount(WAREHOUSE, dsdGenContext) * 6);

  return (dsdGenContext.arRowcount[table].kBaseRowcount);
}

/*
 * Routine: setUpdateDates
 * Purpose: determine the dates for fact table updates
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
void setUpdateDates(DSDGenContext& dsdGenContext) {
  assert(0);
  int nDay, nUpdate, i;
  date_t dtTemp;

  nUpdate = get_int("UPDATE", dsdGenContext);
  while (nUpdate--) {
    /* pick two adjacent days in the low density zone */
    dsdGenContext.arUpdateDates[0] =
        getSkewedJulianDate(calendar_low, 0, dsdGenContext);
    jtodt(&dtTemp, dsdGenContext.arUpdateDates[0]);
    dist_weight(
        &nDay,
        "calendar",
        day_number(&dtTemp) + 1,
        calendar_low,
        dsdGenContext);
    if (nDay)
      dsdGenContext.arUpdateDates[1] = dsdGenContext.arUpdateDates[0] + 1;
    else
      dsdGenContext.arUpdateDates[1] = dsdGenContext.arUpdateDates[0] - 1;

    /*
     * pick the related Thursdays for inventory
     * 1. shift first date to the Thursday in the current update week
     * 2. move forward/back to get into correct comparability zone
     * 3. set next date to next/prior Thursday based on comparability zone
     */
    jtodt(&dtTemp, dsdGenContext.arUpdateDates[0] + (4 - set_dow(&dtTemp)));
    dist_weight(
        &nDay, "calendar", day_number(&dtTemp), calendar_low, dsdGenContext);
    dsdGenContext.arInventoryUpdateDates[0] = dtTemp.julian;
    if (!nDay) {
      jtodt(&dtTemp, dtTemp.julian - 7);
      dsdGenContext.arInventoryUpdateDates[0] = dtTemp.julian;
      dist_weight(
          &nDay, "calendar", day_number(&dtTemp), calendar_low, dsdGenContext);
      if (!nDay)
        dsdGenContext.arInventoryUpdateDates[0] += 14;
    }

    dsdGenContext.arInventoryUpdateDates[1] =
        dsdGenContext.arInventoryUpdateDates[0] + 7;
    jtodt(&dtTemp, dsdGenContext.arInventoryUpdateDates[1]);
    dist_weight(
        &nDay,
        "calendar",
        day_number(&dtTemp) + 1,
        calendar_low,
        dsdGenContext);
    if (!nDay)
      dsdGenContext.arInventoryUpdateDates[1] -= 14;

    /* repeat for medium calendar zone */
    dsdGenContext.arUpdateDates[2] =
        getSkewedJulianDate(calendar_medium, 0, dsdGenContext);
    jtodt(&dtTemp, dsdGenContext.arUpdateDates[2]);
    dist_weight(
        &nDay,
        "calendar",
        day_number(&dtTemp) + 1,
        calendar_medium,
        dsdGenContext);
    if (nDay)
      dsdGenContext.arUpdateDates[3] = dsdGenContext.arUpdateDates[2] + 1;
    else
      dsdGenContext.arUpdateDates[3] = dsdGenContext.arUpdateDates[2] - 1;

    jtodt(&dtTemp, dsdGenContext.arUpdateDates[2] + (4 - set_dow(&dtTemp)));
    dist_weight(
        &nDay, "calendar", day_number(&dtTemp), calendar_medium, dsdGenContext);
    dsdGenContext.arInventoryUpdateDates[2] = dtTemp.julian;
    if (!nDay) {
      jtodt(&dtTemp, dtTemp.julian - 7);
      dsdGenContext.arInventoryUpdateDates[2] = dtTemp.julian;
      dist_weight(
          &nDay,
          "calendar",
          day_number(&dtTemp),
          calendar_medium,
          dsdGenContext);
      if (!nDay)
        dsdGenContext.arInventoryUpdateDates[2] += 14;
    }

    dsdGenContext.arInventoryUpdateDates[3] =
        dsdGenContext.arInventoryUpdateDates[2] + 7;
    jtodt(&dtTemp, dsdGenContext.arInventoryUpdateDates[3]);
    dist_weight(
        &nDay, "calendar", day_number(&dtTemp), calendar_medium, dsdGenContext);
    if (!nDay)
      dsdGenContext.arInventoryUpdateDates[3] -= 14;

    /* repeat for high calendar zone */
    dsdGenContext.arUpdateDates[4] =
        getSkewedJulianDate(calendar_high, 0, dsdGenContext);
    jtodt(&dtTemp, dsdGenContext.arUpdateDates[4]);
    dist_weight(
        &nDay,
        "calendar",
        day_number(&dtTemp) + 1,
        calendar_high,
        dsdGenContext);
    if (nDay)
      dsdGenContext.arUpdateDates[5] = dsdGenContext.arUpdateDates[4] + 1;
    else
      dsdGenContext.arUpdateDates[5] = dsdGenContext.arUpdateDates[4] - 1;

    jtodt(&dtTemp, dsdGenContext.arUpdateDates[4] + (4 - set_dow(&dtTemp)));
    dist_weight(
        &nDay, "calendar", day_number(&dtTemp), calendar_high, dsdGenContext);
    dsdGenContext.arInventoryUpdateDates[4] = dtTemp.julian;
    if (!nDay) {
      jtodt(&dtTemp, dtTemp.julian - 7);
      dsdGenContext.arInventoryUpdateDates[4] = dtTemp.julian;
      dist_weight(
          &nDay, "calendar", day_number(&dtTemp), calendar_high, dsdGenContext);
      if (!nDay)
        dsdGenContext.arInventoryUpdateDates[4] += 14;
    }

    dsdGenContext.arInventoryUpdateDates[5] =
        dsdGenContext.arInventoryUpdateDates[4] + 7;
    jtodt(&dtTemp, dsdGenContext.arInventoryUpdateDates[5]);
    dist_weight(
        &nDay, "calendar", day_number(&dtTemp), calendar_high, dsdGenContext);
    if (!nDay)
      dsdGenContext.arInventoryUpdateDates[5] -= 14;
  }

  //	/*
  //	 * output the update dates for this update set
  //	 */
  //	openDeleteFile(1);
  //	for (i = 0; i < 6; i += 2)
  //		print_delete(&arUpdateDates[i]);
  //
  //	/*
  //	 * inventory uses separate dates
  //	 */
  //	openDeleteFile(2);
  //	for (i = 0; i < 6; i += 2)
  //		print_delete(&arInventoryUpdateDates[i]);
  //	openDeleteFile(0);

  return;
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
int getUpdateDate(
    int nTable,
    ds_key_t kRowcount,
    DSDGenContext& dsdGenContext) {
  static int nIndex = 0, nLastTable = -1;

  if (nLastTable != nTable) {
    nLastTable = nTable;
    get_rowcount(nTable, dsdGenContext);
    nIndex = 0;
  }

  for (nIndex = 0;
       kRowcount > dsdGenContext.arRowcount[nTable].kDayRowcount[nIndex];
       nIndex++)
    if (nIndex == 5)
      break;

  if (nTable == S_INVENTORY) {
    return (dsdGenContext.arInventoryUpdateDates[nIndex]);
  } else
    return (dsdGenContext.arUpdateDates[nIndex]);
}

/*
 * Routine: getUpdateID(int nTable, ds_key_t *pDest)
 * Purpose: select the primary key for an update set row
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns: 1 if the row is new, 0 if it is reusing an existing ID
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20040326 jms getUpdateID() this MUST be updated for 64bit -- all usages
 * use casts today
 * TODO:	20060102 jms this will need to be looked at for parallelism at
 * some point
 */
/*
int
getUpdateID(ds_key_t *pDest, int nTable, int nColumn)
{
    int bIsUpdate = 0,
        nTemp;

    if (genrand_integer(NULL, DIST_UNIFORM, 0, 99, 0, nColumn, dsdGenContext) <
arRowcount[nTable].nUpdatePercentage)
    {
        bIsUpdate = 1;
        genrand_integer(&nTemp, DIST_UNIFORM, 1, (int,
dsdGenContext)getIDCount(nTable), 0, nColumn); *pDest = (ds_key_t)nTemp;
    }
    else
    {
        *pDest = ++arRowcount[nTable].kNextInsertValue;
    }

    return(bIsUpdate);
}
*/

/*
 * Routine: getSkewedJulianDate(int nWeight, int nColumn, DSDGenContext&
 * dsdGenContext) Purpose: return a julian date based on the given skew and
 * column Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int getSkewedJulianDate(
    int nWeight,
    int nColumn,
    DSDGenContext& dsdGenContext) {
  int i;
  date_t Date;

  pick_distribution(&i, "calendar", 1, nWeight, nColumn, dsdGenContext);
  genrand_integer(
      &Date.year,
      DIST_UNIFORM,
      YEAR_MINIMUM,
      YEAR_MAXIMUM,
      0,
      nColumn,
      dsdGenContext);
  dist_member(&Date.day, "calendar", i, 3, dsdGenContext);
  dist_member(&Date.month, "calendar", i, 5, dsdGenContext);
  return (dttoj(&Date));
}

/*
 * Routine: dateScaling(int nTable, ds_key_t jDate)
 * Purpose: determine the number of rows to build for a given date and fact
 * table Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
ds_key_t dateScaling(int nTable, ds_key_t jDate, DSDGenContext& dsdGenContext) {
  dist_t* pDist;
  const d_idx_t* pDistIndex;
  date_t Date;
  int nDateWeight = 1, nCalendarTotal, nDayWeight;
  ds_key_t kRowCount = -1;
  tdef* pTdef = getSimpleTdefsByNumber(nTable, dsdGenContext);
  pDistIndex = find_dist("calendar");
  pDist = pDistIndex->dist;
  if (!pDist)
    ReportError(QERR_NO_MEMORY, "dateScaling()", 1);

  jtodt(&Date, static_cast<int>(jDate));

  switch (nTable) {
    case STORE_SALES:
    case CATALOG_SALES:
    case WEB_SALES:
      kRowCount = get_rowcount(nTable, dsdGenContext);
      nDateWeight = calendar_sales;
      break;
    case S_CATALOG_ORDER:
      kRowCount = get_rowcount(CATALOG_SALES, dsdGenContext);
      nDateWeight = calendar_sales;
      break;
    case S_PURCHASE:
      kRowCount = get_rowcount(STORE_SALES, dsdGenContext);
      nDateWeight = calendar_sales;
      break;
    case S_WEB_ORDER:
      kRowCount = get_rowcount(WEB_SALES, dsdGenContext);
      nDateWeight = calendar_sales;
      break;
    case S_INVENTORY:
    case INVENTORY:
      nDateWeight = calendar_uniform;
      kRowCount = get_rowcount(WAREHOUSE, dsdGenContext) *
          getIDCount(ITEM, dsdGenContext);
      break;
    default:
      ReportErrorNoLine(QERR_TABLE_NOP, pTdef->name, 1);
      break;
  }

  if (nTable !=
      INVENTORY) /* inventory rowcount is uniform thorughout the year */
  {
    if (is_leap(Date.year))
      nDateWeight += 1;

    nCalendarTotal = dist_max(pDist, nDateWeight, dsdGenContext);
    nCalendarTotal *= 5; /* assumes date range is 5 years */

    dist_weight(
        &nDayWeight, "calendar", day_number(&Date), nDateWeight, dsdGenContext);
    kRowCount *= nDayWeight;
    kRowCount += nCalendarTotal / 2;
    kRowCount /= nCalendarTotal;
  }

  return (kRowCount);
}

/*
 * Routine: getUpdateBase(int nTable)
 * Purpose: return the offset to the first order in this update set for a given
 * table Algorithm: Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
ds_key_t getUpdateBase(int nTable, DSDGenContext& dsdGenContext) {
  return (dsdGenContext.arRowcount[nTable - S_BRAND].kNextInsertValue);
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
void setUpdateScaling(int nTable, DSDGenContext& dsdGenContext) {
  tdef* pTdef;
  int i, nBaseTable;
  ds_key_t kNewRowcount = 0;

  pTdef = getSimpleTdefsByNumber(nTable, dsdGenContext);
  if (!(pTdef->flags & FL_SOURCE_DDL) || !(pTdef->flags & FL_DATE_BASED) ||
      (pTdef->flags & FL_NOP))
    return;

  switch (nTable) {
    case S_PURCHASE:
      nBaseTable = STORE_SALES;
      break;
    case S_CATALOG_ORDER:
      nBaseTable = CATALOG_SALES;
      break;
    case S_WEB_ORDER:
      nBaseTable = WEB_SALES;
      break;
    case S_INVENTORY:
      nBaseTable = INVENTORY;
      break;
    default: {
      auto result =
          fprintf(stderr, "ERROR: Invalid table in setUpdateScaling\n");
      if (result < 0)
        perror("sprintf failed");
      exit(1);
      break;
    }
  }

  dsdGenContext.arRowcount[nTable].kNextInsertValue =
      dsdGenContext.arRowcount[nTable].kBaseRowcount;

  for (i = 0; i < 6; i++) {
    kNewRowcount +=
        dateScaling(nBaseTable, dsdGenContext.arUpdateDates[i], dsdGenContext);
    dsdGenContext.arRowcount[nTable].kDayRowcount[i] = kNewRowcount;
  }

  dsdGenContext.arRowcount[nTable].kBaseRowcount = kNewRowcount;
  dsdGenContext.arRowcount[nTable].kNextInsertValue +=
      kNewRowcount * (get_int("update", dsdGenContext) - 1);

  return;
}
