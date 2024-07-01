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
    strtodt(&BaseDate, DATA_START_DATE);
    dsdGenContext.skipDays_init = 1;
    *pRemainder = 0;
  } else {
    strtodt(&BaseDate, DATA_START_DATE);
  }

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
