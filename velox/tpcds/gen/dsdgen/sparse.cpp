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

#include "velox/tpcds/gen/dsdgen/include/sparse.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/error_msg.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

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

  pTdef->arSparseKeys.resize(kRowcount, 0);
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
