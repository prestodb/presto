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

#include "w_reason.h"

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
