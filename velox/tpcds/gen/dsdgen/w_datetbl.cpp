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

#include "velox/tpcds/gen/dsdgen/include/w_datetbl.h"

#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/parallel.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

#include <stdio.h>

/* extern tdef w_tdefs[]; */

/*
 * Routine: mk_datetbl
 * Purpose: populate the date dimension
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 */
int mk_w_date(void* info_arr, ds_key_t index, DSDGenContext& dsdGenContext) {
  int res = 0;

  /* begin locals declarations */
  date_t base_date;
  int day_index, nTemp;
  date_t temp_date, dTemp2;
  struct W_DATE_TBL* r;
  tdef* pT = getSimpleTdefsByNumber(DATET, dsdGenContext);

  r = &dsdGenContext.g_w_date;

  if (!dsdGenContext.mk_w_date_init) {
    r->d_month_seq = 0;
    r->d_week_seq = 1;
    r->d_quarter_seq = 1;
    r->d_current_month = 0;
    r->d_current_quarter = 0;
    r->d_current_week = 0;
    strtodt(&base_date, "1900-01-01");
    /* Make exceptions to the 1-rng-call-per-row rule */
    dsdGenContext.mk_w_date_init = 1;
  } else {
    strtodt(&base_date, "1900-01-01");
  }

  nullSet(&pT->kNullBitMap, D_NULLS, dsdGenContext);
  nTemp = static_cast<long>(index + base_date.julian);
  r->d_date_sk = nTemp;
  mk_bkey(&r->d_date_id[0], nTemp, D_DATE_ID);
  jtodt(&temp_date, nTemp);
  r->d_year = temp_date.year;
  r->d_dow = set_dow(&temp_date);
  r->d_moy = temp_date.month;
  r->d_dom = temp_date.day;
  /* set the sequence counts; assumes that the date table starts on a year
   * boundary */
  r->d_week_seq = (static_cast<int>(index + 6)) / 7;
  r->d_month_seq = (r->d_year - 1900) * 12 + r->d_moy - 1;
  r->d_quarter_seq = (r->d_year - 1900) * 4 + r->d_moy / 3 + 1;
  day_index = day_number(&temp_date);
  dist_member(&r->d_qoy, "calendar", day_index, 6, dsdGenContext);
  /* fiscal year is identical to calendar year */
  r->d_fy_year = r->d_year;
  r->d_fy_quarter_seq = r->d_quarter_seq;
  r->d_fy_week_seq = r->d_week_seq;
  r->d_day_name = weekday_names[r->d_dow + 1];
  dist_member(&r->d_holiday, "calendar", day_index, 8, dsdGenContext);
  if ((r->d_dow == 5) || (r->d_dow == 6))
    r->d_weekend = 1;
  else
    r->d_weekend = 0;
  if (day_index == 1)
    dist_member(
        &r->d_following_holiday,
        "calendar",
        365 + is_leap(r->d_year - 1),
        8,
        dsdGenContext);
  else
    dist_member(
        &r->d_following_holiday, "calendar", day_index - 1, 8, dsdGenContext);
  date_t_op(&dTemp2, OP_FIRST_DOM, &temp_date, 0);
  r->d_first_dom = dTemp2.julian;
  date_t_op(&dTemp2, OP_LAST_DOM, &temp_date, 0);
  r->d_last_dom = dTemp2.julian;
  date_t_op(&dTemp2, OP_SAME_LY, &temp_date, 0);
  r->d_same_day_ly = dTemp2.julian;
  date_t_op(&dTemp2, OP_SAME_LQ, &temp_date, 0);
  r->d_same_day_lq = dTemp2.julian;
  r->d_current_day = (r->d_date_sk == CURRENT_DAY) ? 1 : 0;
  r->d_current_year = (r->d_year == CURRENT_YEAR) ? 1 : 0;
  if (r->d_current_year) {
    r->d_current_month = (r->d_moy == CURRENT_MONTH) ? 1 : 0;
    r->d_current_quarter = (r->d_qoy == CURRENT_QUARTER) ? 1 : 0;
    r->d_current_week = (r->d_week_seq == CURRENT_WEEK) ? 1 : 0;
  }

  std::vector<char> sQuarterName(7);

  void* info = append_info_get(info_arr, DATET);
  append_row_start(info);

  append_key(D_DATE_SK, info, r->d_date_sk);
  append_varchar(D_DATE_ID, info, r->d_date_id);
  append_date(D_DATE_SK, info, r->d_date_sk);
  append_integer(D_MONTH_SEQ, info, r->d_month_seq);
  append_integer(D_WEEK_SEQ, info, r->d_week_seq);
  append_integer(D_QUARTER_SEQ, info, r->d_quarter_seq);
  append_integer(D_YEAR, info, r->d_year);
  append_integer(D_DOW, info, r->d_dow);
  append_integer(D_MOY, info, r->d_moy);
  append_integer(D_DOM, info, r->d_dom);
  append_integer(D_QOY, info, r->d_qoy);
  append_integer(D_FY_YEAR, info, r->d_fy_year);
  append_integer(D_FY_QUARTER_SEQ, info, r->d_fy_quarter_seq);
  append_integer(D_FY_WEEK_SEQ, info, r->d_fy_week_seq);
  append_varchar(D_DAY_NAME, info, r->d_day_name);
  snprintf(
      sQuarterName.data(), sQuarterName.size(), "%4dQ%d", r->d_year, r->d_qoy);
  append_varchar(D_QUARTER_NAME, info, sQuarterName.data());
  append_varchar(D_HOLIDAY, info, r->d_holiday ? "Y" : "N");
  append_varchar(D_WEEKEND, info, r->d_weekend ? "Y" : "N");
  append_varchar(D_FOLLOWING_HOLIDAY, info, r->d_following_holiday ? "Y" : "N");
  append_integer(D_FIRST_DOM, info, r->d_first_dom);
  append_integer(D_LAST_DOM, info, r->d_last_dom);
  append_integer(D_SAME_DAY_LY, info, r->d_same_day_ly);
  append_integer(D_SAME_DAY_LQ, info, r->d_same_day_lq);
  append_varchar(D_CURRENT_DAY, info, r->d_current_day ? "Y" : "N");
  append_varchar(D_CURRENT_WEEK, info, r->d_current_week ? "Y" : "N");
  append_varchar(D_CURRENT_MONTH, info, r->d_current_month ? "Y" : "N");
  append_varchar(D_CURRENT_QUARTER, info, r->d_current_quarter ? "Y" : "N");
  append_varchar(D_CURRENT_YEAR, info, r->d_current_year ? "Y" : "N");

  append_row_end(info);

  return (res);
}
