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

#include "velox/tpcds/gen/dsdgen/include/w_timetbl.h"

#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/misc.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

#include <stdio.h>

/*
 * mk_time
 */
int mk_w_time(void* info_arr, ds_key_t index, DSDGenContext& dsdGenContext) {
  /* begin locals declarations */
  int nTemp;
  struct W_TIME_TBL* r;
  tdef* pT = getSimpleTdefsByNumber(TIMET, dsdGenContext);

  r = &dsdGenContext.g_w_time;

  nullSet(&pT->kNullBitMap, T_NULLS, dsdGenContext);
  r->t_time_sk = index - 1;
  mk_bkey(&r->t_time_id[0], index, T_TIME_ID);
  r->t_time = static_cast<long>(index - 1);
  nTemp = static_cast<long>(index - 1);
  r->t_second = nTemp % 60;
  nTemp /= 60;
  r->t_minute = nTemp % 60;
  nTemp /= 60;
  r->t_hour = nTemp % 24;
  dist_member(&r->t_am_pm, "hours", r->t_hour + 1, 2, dsdGenContext);
  dist_member(&r->t_shift, "hours", r->t_hour + 1, 3, dsdGenContext);
  dist_member(&r->t_sub_shift, "hours", r->t_hour + 1, 4, dsdGenContext);
  dist_member(&r->t_meal_time, "hours", r->t_hour + 1, 5, dsdGenContext);

  void* info = append_info_get(info_arr, TIMET);
  append_row_start(info);
  append_key(T_TIME_SK, info, r->t_time_sk);
  append_varchar(T_TIME_ID, info, r->t_time_id);
  append_integer(T_TIME, info, r->t_time);
  append_integer(T_HOUR, info, r->t_hour);
  append_integer(T_MINUTE, info, r->t_minute);
  append_integer(T_SECOND, info, r->t_second);
  append_varchar(T_AM_PM, info, r->t_am_pm);
  append_varchar(T_SHIFT, info, r->t_shift);
  append_varchar(T_SUB_SHIFT, info, r->t_sub_shift);
  append_varchar(T_MEAL_TIME, info, r->t_meal_time, false);
  append_row_end(info);

  return 0;
}
