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

#include "velox/tpcds/gen/dsdgen/include/w_household_demographics.h"

#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/sparse.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

#include <stdio.h>

/*
 * mk_household_demographics
 */
int mk_w_household_demographics(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  /* begin locals declarations */
  ds_key_t nTemp;
  struct W_HOUSEHOLD_DEMOGRAPHICS_TBL* r;
  tdef* pTdef = getSimpleTdefsByNumber(HOUSEHOLD_DEMOGRAPHICS, dsdGenContext);

  r = &dsdGenContext.g_w_household_demographics;

  nullSet(&pTdef->kNullBitMap, HD_NULLS, dsdGenContext);
  r->hd_demo_sk = index;
  nTemp = r->hd_demo_sk;
  r->hd_income_band_id = (nTemp % distsize("income_band", dsdGenContext)) + 1;
  nTemp /= distsize("income_band", dsdGenContext);
  bitmap_to_dist(
      &r->hd_buy_potential,
      "buy_potential",
      &nTemp,
      1,
      HOUSEHOLD_DEMOGRAPHICS,
      dsdGenContext);
  bitmap_to_dist(
      &r->hd_dep_count,
      "dependent_count",
      &nTemp,
      1,
      HOUSEHOLD_DEMOGRAPHICS,
      dsdGenContext);
  bitmap_to_dist(
      &r->hd_vehicle_count,
      "vehicle_count",
      &nTemp,
      1,
      HOUSEHOLD_DEMOGRAPHICS,
      dsdGenContext);

  void* info = append_info_get(info_arr, HOUSEHOLD_DEMOGRAPHICS);
  append_row_start(info);
  append_key(HD_DEMO_SK, info, r->hd_demo_sk);
  append_key(HD_INCOME_BAND_ID, info, r->hd_income_band_id);
  append_varchar(HD_BUY_POTENTIAL, info, r->hd_buy_potential);
  append_integer(HD_DEP_COUNT, info, r->hd_dep_count);
  append_integer(HD_VEHICLE_COUNT, info, r->hd_vehicle_count);
  append_row_end(info);

  return 0;
}
