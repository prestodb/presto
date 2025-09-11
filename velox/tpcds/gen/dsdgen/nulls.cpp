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

#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

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
