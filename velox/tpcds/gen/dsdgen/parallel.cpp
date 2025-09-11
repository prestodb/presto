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

#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/r_params.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

/*
 * Routine: split_work(int tnum, worker_t *w)
 * Purpose: allocate work between processes and threads
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
int split_work(
    int tnum,
    ds_key_t* pkFirstRow,
    ds_key_t* pkRowCount,
    DSDGenContext& dsdGenContext) {
  ds_key_t kTotalRows, kRowsetSize, kExtraRows;
  int nParallel, nChild;

  kTotalRows = get_rowcount(tnum, dsdGenContext);
  nParallel = get_int("PARALLEL", dsdGenContext);
  nChild = get_int("CHILD", dsdGenContext) + 1;

  /*
   * 1. small tables aren't paralelized
   * 2. nothing is parallelized unless a command line arg is supplied
   */
  *pkFirstRow = 1;
  *pkRowCount = kTotalRows;

  if (kTotalRows < 1000000) {
    if (nChild > 1) /* small table; only build it once */
    {
      *pkFirstRow = 1;
      *pkRowCount = 0;
      return (0);
    }
    return (1);
  }

  if (!is_set("PARALLEL", dsdGenContext)) {
    return (1);
  }

  /*
   * at this point, do the calculation to set the rowcount for this part of a
   * parallel build
   */
  kExtraRows = kTotalRows % nParallel;
  kRowsetSize = (kTotalRows - kExtraRows) / nParallel;

  /* start the starting row id */
  *pkFirstRow += (nChild - 1) * kRowsetSize;
  if (kExtraRows && (nChild - 1))
    *pkFirstRow += ((nChild - 1) < kExtraRows) ? (nChild - 1) : kExtraRows;

  /* set the rowcount for this child */
  *pkRowCount = kRowsetSize;
  if (kExtraRows && (nChild <= kExtraRows))
    *pkRowCount += 1;

  return (1);
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
int checkSeeds(tdef* pTdef, DSDGenContext& dsdGenContext) {
  int i, res, nReturnCode = 0;
  static int bSetSeeds = 0;

  if (!dsdGenContext.checkSeeds_init) {
    bSetSeeds = is_set("CHKSEEDS", dsdGenContext);
    dsdGenContext.checkSeeds_init = 1;
  }

  for (i = pTdef->nFirstColumn; i <= pTdef->nLastColumn; i++) {
    while (dsdGenContext.Streams[i].nUsed <
           dsdGenContext.Streams[i].nUsedPerRow)
      genrand_integer(&res, DIST_UNIFORM, 1, 100, 0, i, dsdGenContext);
    if (bSetSeeds) {
      if (dsdGenContext.Streams[i].nUsed >
          dsdGenContext.Streams[i].nUsedPerRow) {
        fprintf(
            stderr,
            "Seed overrun on column %d. Used: %d\n",
            i,
            dsdGenContext.Streams[i].nUsed);
        dsdGenContext.Streams[i].nUsedPerRow = dsdGenContext.Streams[i].nUsed;
        nReturnCode = 1;
      }
    }
    dsdGenContext.Streams[i].nUsed = 0; /* reset for the next time */
  }

  return (nReturnCode);
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
int row_stop(int tbl, DSDGenContext& dsdGenContext) {
  tdef* pTdef;

  pTdef = getSimpleTdefsByNumber(tbl, dsdGenContext);
  checkSeeds(pTdef, dsdGenContext);
  if (pTdef->flags & FL_PARENT) {
    pTdef = getSimpleTdefsByNumber(pTdef->nParam, dsdGenContext);
    checkSeeds(pTdef, dsdGenContext);
  }
  return (0);
}

/*
 * Routine: row_skip
 * Purpose: skip over un-used rows in a table
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20020816 jms The second parameter should really be a ds_key_t to allow
 * BIG skips
 */
int row_skip(int tbl, ds_key_t count, DSDGenContext& dsdGenContext) {
  int i;
  for (i = 0; dsdGenContext.Streams[i].nColumn != -1; i++) {
    if (dsdGenContext.Streams[i].nTable == tbl) {
      skip_random(
          i, count * dsdGenContext.Streams[i].nUsedPerRow, dsdGenContext);
      dsdGenContext.Streams[i].nUsed = 0;
      dsdGenContext.Streams[i].nTotal =
          count * dsdGenContext.Streams[i].nUsedPerRow;
    }
    if (dsdGenContext.Streams[i].nDuplicateOf &&
        (dsdGenContext.Streams[i].nDuplicateOf != i)) {
      skip_random(
          dsdGenContext.Streams[i].nDuplicateOf,
          count *
              dsdGenContext.Streams[dsdGenContext.Streams[i].nDuplicateOf]
                  .nUsedPerRow,
          dsdGenContext);
      dsdGenContext.Streams[dsdGenContext.Streams[i].nDuplicateOf].nUsed = 0;
      dsdGenContext.Streams[dsdGenContext.Streams[i].nDuplicateOf].nTotal =
          count * dsdGenContext.Streams[i].nUsedPerRow;
    }
  }

  return (0);
}
