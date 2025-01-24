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

#include "sparse.h"
#include "config.h"
#include "error_msg.h"
#include "genrand.h"
#include "porting.h"
#include "scaling.h"
#include "tdefs.h"

/*
 * Routine: initSparseKeys()
 * Purpose: set up the set of valid key values for a sparse table.
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions: The total population will fit in 32b
 * Side Effects:
 * TODO: None
 */
int initSparseKeys(int nTable, DSDGenContext& dsdGenContext) {
  ds_key_t kRowcount, kOldSeed;
  int k;
  tdef* pTdef;

  kRowcount = get_rowcount(nTable, dsdGenContext);
  pTdef = getTdefsByNumber(nTable, dsdGenContext);

  pTdef->arSparseKeys = static_cast<ds_key_t*>(
      malloc(static_cast<long>(kRowcount) * sizeof(ds_key_t)));
  MALLOC_CHECK(pTdef->arSparseKeys);
  if (pTdef->arSparseKeys == NULL)
    ReportError(QERR_NO_MEMORY, "initSparseKeys()", 1);
  memset(
      pTdef->arSparseKeys, 0, static_cast<long>(kRowcount) * sizeof(ds_key_t));

  kOldSeed = setSeed(0, nTable, dsdGenContext);
  for (k = 0; k < kRowcount; k++)
    genrand_key(
        &pTdef->arSparseKeys[k],
        DIST_UNIFORM,
        1,
        pTdef->nParam,
        0,
        0,
        dsdGenContext);
  setSeed(0, static_cast<int>(kOldSeed), dsdGenContext);

  return (0);
}

/*
 * Routine: randomSparseKey()
 * Purpose: randomly select one of the valid key values for a sparse table
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
ds_key_t
randomSparseKey(int nTable, int nColumn, DSDGenContext& dsdGenContext) {
  int nKeyIndex;
  ds_key_t kRowcount;
  tdef* pTdef;

  pTdef = getTdefsByNumber(nTable, dsdGenContext);
  kRowcount = get_rowcount(nTable, dsdGenContext);
  genrand_integer(
      &nKeyIndex,
      DIST_UNIFORM,
      1,
      static_cast<long>(kRowcount),
      0,
      nColumn,
      dsdGenContext);

  return (pTdef->arSparseKeys[nKeyIndex]);
}
