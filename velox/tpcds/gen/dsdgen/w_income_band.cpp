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

#include "velox/tpcds/gen/dsdgen/include/w_income_band.h"

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
