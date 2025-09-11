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

#include "velox/tpcds/gen/dsdgen/include/w_reason.h"

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
 * mk_reason
 */
int mk_w_reason(void* info_arr, ds_key_t index, DSDGenContext& dsdGenContext) {
  struct W_REASON_TBL* r;
  tdef* pTdef = getSimpleTdefsByNumber(REASON, dsdGenContext);

  r = &dsdGenContext.g_w_reason;

  if (!dsdGenContext.mk_w_reason_init) {
    memset(&dsdGenContext.g_w_reason, 0, sizeof(struct W_REASON_TBL));
    dsdGenContext.mk_w_reason_init = 1;
  }

  nullSet(&pTdef->kNullBitMap, R_NULLS, dsdGenContext);
  r->r_reason_sk = index;
  mk_bkey(&r->r_reason_id[0], index, R_REASON_ID);
  dist_member(
      &r->r_reason_description,
      "return_reasons",
      static_cast<int>(index),
      1,
      dsdGenContext);

  void* info = append_info_get(info_arr, REASON);
  append_row_start(info);
  append_key(R_REASON_SK, info, r->r_reason_sk);
  append_varchar(R_REASON_ID, info, r->r_reason_id);
  append_varchar(R_REASON_DESCRIPTION, info, r->r_reason_description);
  append_row_end(info);

  return 0;
}
