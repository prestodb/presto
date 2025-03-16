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

#include "skip_days.h"
#include "constants.h"
#include "date.h"
#include "dist.h"
#include "parallel.h"
#include "scaling.h"

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
