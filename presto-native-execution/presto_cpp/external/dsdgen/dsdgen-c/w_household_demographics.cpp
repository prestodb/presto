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

#include "w_household_demographics.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "genrand.h"
#include "nulls.h"
#include "porting.h"
#include "sparse.h"
#include "tables.h"
#include "tdefs.h"

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
