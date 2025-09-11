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

#include "velox/tpcds/gen/dsdgen/include/skip_days.h"
#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/parallel.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"

ds_key_t
skipDays(int nTable, ds_key_t* pRemainder, DSDGenContext& dsdGenContext) {
  date_t BaseDate;
  ds_key_t jDate;
  ds_key_t kRowCount, kFirstRow, kDayCount, index = 1;

  if (!dsdGenContext.skipDays_init) {
    *pRemainder = 0;
    dsdGenContext.skipDays_init = 1;
  }
  strtodt(&BaseDate, DATA_START_DATE);

  // set initial conditions
  jDate = BaseDate.julian;
  *pRemainder = dateScaling(nTable, jDate, dsdGenContext) + index;

  // now check to see if we need to move to the
  // the next piece of a parallel build
  // move forward one day at a time
  split_work(nTable, &kFirstRow, &kRowCount, dsdGenContext);
  while (index < kFirstRow) {
    kDayCount = dateScaling(nTable, jDate, dsdGenContext);
    index += kDayCount;
    jDate += 1;
    *pRemainder = index;
  }
  if (index > kFirstRow) {
    jDate -= 1;
  }
  return (jDate);
}
