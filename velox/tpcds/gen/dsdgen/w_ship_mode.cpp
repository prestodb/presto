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

#include "velox/tpcds/gen/dsdgen/include/w_ship_mode.h"

#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

#include <stdio.h>

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
int mk_w_ship_mode(
    void* info_arr,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext) {
  struct W_SHIP_MODE_TBL* r;
  ds_key_t nTemp;
  tdef* pTdef = getSimpleTdefsByNumber(SHIP_MODE, dsdGenContext);

  r = &dsdGenContext.g_w_ship_mode;

  if (dsdGenContext.mk_w_ship_mode_init) {
    memset(&dsdGenContext.g_w_ship_mode, 0, sizeof(struct W_SHIP_MODE_TBL));
    dsdGenContext.mk_w_ship_mode_init = 1;
  }

  nullSet(&pTdef->kNullBitMap, SM_NULLS, dsdGenContext);
  r->sm_ship_mode_sk = kIndex;
  mk_bkey(&r->sm_ship_mode_id[0], kIndex, SM_SHIP_MODE_ID);
  nTemp = static_cast<long>(kIndex);
  bitmap_to_dist(
      &r->sm_type, "ship_mode_type", &nTemp, 1, SHIP_MODE, dsdGenContext);
  bitmap_to_dist(
      &r->sm_code, "ship_mode_code", &nTemp, 1, SHIP_MODE, dsdGenContext);
  dist_member(
      &r->sm_carrier,
      "ship_mode_carrier",
      static_cast<int>(kIndex),
      1,
      dsdGenContext);
  gen_charset(
      r->sm_contract, ALPHANUM, 1, RS_SM_CONTRACT, SM_CONTRACT, dsdGenContext);

  void* info = append_info_get(info_arr, SHIP_MODE);
  append_row_start(info);
  append_key(SM_SHIP_MODE_SK, info, r->sm_ship_mode_sk);
  append_varchar(SM_SHIP_MODE_ID, info, r->sm_ship_mode_id);
  append_varchar(SM_TYPE, info, r->sm_type);
  append_varchar(SM_CODE, info, r->sm_code);
  append_varchar(SM_CARRIER, info, r->sm_carrier);
  append_varchar(SM_CONTRACT, info, &r->sm_contract[0]);
  append_row_end(info);

  return 0;
}
