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

#include "nulls.h"
#include "config.h"
#include "genrand.h"
#include "porting.h"
#include "tdefs.h"

/*
 * Routine: nullCheck(int nColumn, DSDGenContext& dsdGenContext)
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
int nullCheck(int nColumn, DSDGenContext& dsdGenContext) {
  int nLastTable = 0;
  tdef* pTdef;
  ds_key_t kBitMask = 1;

  nLastTable = getTableFromColumn(nColumn, dsdGenContext);
  pTdef = getSimpleTdefsByNumber(nLastTable, dsdGenContext);

  kBitMask <<= nColumn - pTdef->nFirstColumn;

  return ((pTdef->kNullBitMap & kBitMask) != 0);
}

/*
* Routine: nullSet(int *pDest, int nStream, DSDGenContext& dsdGenContext)
* Purpose: set the kNullBitMap for a particular table
* Algorithm:
*	1. if random[1,100] >= table's NULL pct, clear map and return
*	2. set map

* Data Structures:
*
* Params:
* Returns:
* Called By:
* Calls:
* Assumptions:
* Side Effects: uses 2 RNG calls
* TODO: None
*/
void nullSet(ds_key_t* pDest, int nStream, DSDGenContext& dsdGenContext) {
  int nThreshold;
  ds_key_t kBitMap;
  int nLastTable = 0;
  tdef* pTdef;

  nLastTable = getTableFromColumn(nStream, dsdGenContext);
  pTdef = getSimpleTdefsByNumber(nLastTable, dsdGenContext);

  /* burn the RNG calls */
  genrand_integer(
      &nThreshold, DIST_UNIFORM, 0, 9999, 0, nStream, dsdGenContext);
  genrand_key(&kBitMap, DIST_UNIFORM, 1, MAXINT, 0, nStream, dsdGenContext);

  /* set the bitmap based on threshold and NOT NULL definitions */
  *pDest = 0;
  if (nThreshold < pTdef->nNullPct) {
    *pDest = kBitMap;
    *pDest &= ~pTdef->kNotNullBitMap;
  }

  return;
}
