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

#include <stdlib.h>
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#ifndef USE_STDLIB_H
#include <malloc.h>
#endif
#include <math.h>
#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/dist.h"
#include "velox/tpcds/gen/dsdgen/include/mathops.h"

#define D_CHARS "ymdYMD24" /* valid characters in a DBGDATE setting */
#define MIN_DATE_INT 18000101

static int m_days[2][13] = {
    {0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334},
    {0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335}};
static const std::vector<const char*> qtr_start =
    {"", "01-01", "04-01", "07-01", "10-01"};
const std::vector<const char*> weekday_names = {
    "",
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday"};

/*
 * Routine: strtodate(char *str)
 * Purpose: initialize a date_t
 * Algorithm:
 * Data Structures:
 * Params:
 * Returns: date_t
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
date_t strtodate(const char* str) {
  date_t res;

  if (sscanf(str, "%d-%d-%d", &res.year, &res.month, &res.day) != 3)
    INTERNAL("Badly formed string in call to strtodate()");
  res.flags = 0;
  res.julian = dttoj(&res);

  return (res);
}

/*
 * Routine: jtodt(int src, date_t *dest)
 * Purpose: convert a number of julian days to a date_t
 * Algorithm: Fleigel and Van Flandern (CACM, vol 11, #10, Oct. 1968, p. 657)
 * Data Structures:
 *
 * Params: source integer: days since big bang
 * Returns: date_t *; NULL on failure
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO:
 */
int jtodt(date_t* dest, int src) {
  long i = 0, j = 0, l = 0, n = 0;

  if (src < 0)
    return (-1);

  dest->julian = src;
  l = src + 68569;
  n = (int)floor((4 * l) / 146097);
  l = l - (int)floor((146097 * n + 3) / 4);
  i = (int)floor((4000 * (l + 1) / 1461001));
  l = l - (int)floor((1461 * i) / 4) + 31;
  j = (int)floor((80 * l) / 2447);
  dest->day = l - (int)floor((2447 * j) / 80);
  l = (int)floor(j / 11);
  dest->month = j + 2 - 12 * l;
  dest->year = 100 * (n - 49) + i + l;

  return (0);
}

/*
 * Routine: dttoj(date_t *)
 * Purpose: convert a date_t to a number of julian days
 * Algorithm: http://quasar.as.utexas.edu/BillInfo/JulianDatesG.html
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int dttoj(date_t* dt) {
  int y, m, res = 0;

  y = dt->year;
  m = dt->month;
  if (m <= 2) {
    m += 12;
    y -= 1;
  }

  /*
   * added 1 to get dttoj and jtodt to match
   */
  res = dt->day + (153 * m - 457) / 5 + 365 * y + (int)floor(y / 4) -
      (int)floor(y / 100) + (int)floor(y / 400) + 1721118 + 1;

  return (res);
}

/*
 * Routine: strtodt()
 * Purpose: Convert an ascii string to a date_t structure
 * Algorithm:
 * Data Structures:
 *
 * Params: char *s, date_t *dest
 * Returns: int; 0 on success
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: Need to allow for date formats other than Y4MD-
 */
int strtodt(date_t* dest, const char* s) {
  int nRetCode = 0;

  if (s == NULL) {
    dest = NULL;
    return (-1);
  }

  if (sscanf(s, "%4d-%d-%d", &dest->year, &dest->month, &dest->day) != 3) {
    fprintf(stderr, "ERROR: Invalid string to date conversion in strtodt\n");
    nRetCode = -1;
  }

  dest->julian = dttoj(dest);

  return (nRetCode);
}

/*
 * Routine: date_init
 * Purpose: set the date handling parameters
 * Algorithm:
 * Data Structures:
 *
 * Params: None
 * Returns: int; 0 on success
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int date_init(void) {
  printf("date_init is not yet complete\n");
  exit(1);
  return (0);
}

/*
 * Routine: date_t_op(int op, date_t *operand1, date_t *operand2)
 * Purpose: execute arbitrary binary operations on date_t's
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
 *	20010806 jms	Return code is meaningless
 */
int date_t_op(date_t* dest, int op, date_t* d1, date_t* d2) {
  int tJulian;
  std::vector<char> tString(20);
  date_t tDate;

  switch (op) {
    case OP_FIRST_DOM: /* set to first day of month */
      tJulian = d1->julian - d1->day + 1;
      jtodt(dest, tJulian);
      break;
    case OP_LAST_DOM: /* set to last day of month */
      tJulian = d1->julian - d1->day + m_days[is_leap(d1->year)][d1->month];
      jtodt(dest, tJulian);
      break;
    case OP_SAME_LY:
      if (is_leap(d1->year) && (d1->month == 2) && (d1->day == 29))
        snprintf(tString.data(), tString.size(), "%d-02-28", d1->year - 1);
      else
        snprintf(
            tString.data(),
            tString.size(),
            "%4d-%02d-%02d",
            d1->year - 1,
            d1->month,
            d1->day);
      strtodt(dest, tString.data());
      break;
    case OP_SAME_LQ:
      switch (d1->month) {
        case 1:
        case 2:
        case 3:
          snprintf(
              tString.data(), tString.size(), "%4d-%s", d1->year, qtr_start[1]);
          strtodt(&tDate, tString.data());
          tJulian = d1->julian - tDate.julian;
          snprintf(
              tString.data(),
              tString.size(),
              "%4d-%s",
              d1->year - 1,
              qtr_start[4]);
          strtodt(&tDate, tString.data());
          tJulian += tDate.julian;
          jtodt(dest, tJulian);
          break;
        case 4:
        case 5:
        case 6:
          snprintf(
              tString.data(), tString.size(), "%4d-%s", d1->year, qtr_start[2]);
          strtodt(&tDate, tString.data());
          tJulian = d1->julian - tDate.julian;
          snprintf(
              tString.data(), tString.size(), "%4d-%s", d1->year, qtr_start[1]);
          strtodt(&tDate, tString.data());
          tJulian += tDate.julian;
          jtodt(dest, tJulian);
          break;
        case 7:
        case 8:
        case 9:
          snprintf(
              tString.data(), tString.size(), "%4d-%s", d1->year, qtr_start[3]);
          strtodt(&tDate, tString.data());
          tJulian = d1->julian - tDate.julian;
          snprintf(
              tString.data(), tString.size(), "%4d-%s", d1->year, qtr_start[2]);
          strtodt(&tDate, tString.data());
          tJulian += tDate.julian;
          jtodt(dest, tJulian);
          break;
        case 10:
        case 11:
        case 12:
          snprintf(
              tString.data(), tString.size(), "%4d-%s", d1->year, qtr_start[4]);
          strtodt(&tDate, tString.data());
          tJulian = d1->julian - tDate.julian;
          snprintf(
              tString.data(), tString.size(), "%4d-%s", d1->year, qtr_start[3]);
          strtodt(&tDate, tString.data());
          tJulian += tDate.julian;
          jtodt(dest, tJulian);
          break;
      }
      break;
  }

  return (0);
}

/*
 * Routine: set_dow(date *d)
 * Purpose: perpetual calendar stuff
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
int set_dow(date_t* d) {
  int doomsday[4] = {3, 2, 0, 5};
  int known[13] = {0, 3, 0, 0, 4, 9, 6, 11, 8, 5, 10, 7, 12};
  int last_year = -1, dday;
  int res = 0, q = 0, r = 0, s = 0;

  if (d->year != last_year) {
    if (is_leap(d->year)) {
      /* adjust the known dates for january and february */
      known[1] = 4;
      known[2] = 1;
    } else {
      known[1] = 3;
      known[2] = 0;
    }

    /* calculate the doomsday for the century */
    dday = d->year / 100;
    dday -= 15;
    dday %= 4;
    dday = doomsday[dday];

    /* and then calculate the doomsday for the year */
    q = d->year % 100;
    r = q % 12;
    q /= 12;
    s = r / 4;
    dday += q + r + s;
    dday %= 7;
    last_year = d->year;
  }

  res = d->day;
  res -= known[d->month];
  while (res < 0)
    res += 7;
  while (res > 6)
    res -= 7;

  res += dday;
  res %= 7;

  return (res);
}

/*
 * Routine: is_leap(year)
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int is_leap(int year) {
  return (
      ((year % 100) == 0)     ? ((((year % 400) % 2) == 0) ? 1 : 0)
          : ((year % 4) == 0) ? 1
                              : 0);
}

/*
 * Routine: day_number(date_t *)
 * Purpose:
 * Algorithm: NOTE: this is NOT the ordinal day in the year, but the ordinal
 *reference into the calendar distribution for the day; in particular, this
 *needs to skip over the leap day Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int day_number(date_t* d) {
  return (m_days[is_leap(d->year)][d->month] + d->day);
}

/*
 * Routine: date_part(date_t *, int part)
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int date_part(date_t* d, int part) {
  switch (part) {
    case 1:
      return (d->year);
    case 2:
      return (d->month);
    case 3:
      return (d->day);
    default:
      INTERNAL("Invalid call to date_part()");
      return (-1);
  }
}
