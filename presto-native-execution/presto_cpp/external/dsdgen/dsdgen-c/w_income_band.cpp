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

#include "w_income_band.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "genrand.h"
#include "nulls.h"
#include "porting.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>

/*
 * mk_income_band
 */
int mk_w_income_band(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  struct W_INCOME_BAND_TBL* r;
  tdef* pTdef = getSimpleTdefsByNumber(INCOME_BAND, dsdGenContext);

  r = &dsdGenContext.g_w_income_band;

  nullSet(&pTdef->kNullBitMap, IB_NULLS, dsdGenContext);
  r->ib_income_band_id = static_cast<long>(index);
  dist_member(
      &r->ib_lower_bound,
      "income_band",
      static_cast<long>(index),
      1,
      dsdGenContext);
  dist_member(
      &r->ib_upper_bound,
      "income_band",
      static_cast<long>(index),
      2,
      dsdGenContext);

  void* info = append_info_get(info_arr, INCOME_BAND);
  append_row_start(info);
  append_key(IB_INCOME_BAND_ID, info, r->ib_income_band_id);
  append_integer(IB_LOWER_BOUND, info, r->ib_lower_bound);
  append_integer(IB_UPPER_BOUND, info, r->ib_upper_bound);
  append_row_end(info);

  return 0;
}
