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

#include "w_customer.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "constants.h"
#include "genrand.h"
#include "nulls.h"
#include "porting.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>
#include "parallel.h"
/* extern tdef w_tdefs[]; */

/*
 * Routine: mk_customer
 * Purpose: populate the customer dimension
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
int mk_w_customer(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  int nTemp;

  int nBaseDate;
  /* begin locals declarations */
  int nNameIndex, nGender;
  struct W_CUSTOMER_TBL* r;
  date_t dtTemp;
  date_t dtBirthMin, dtBirthMax, dtToday, dt1YearAgo, dt10YearsAgo;
  tdef* pT = getSimpleTdefsByNumber(CUSTOMER, dsdGenContext);
  r = &dsdGenContext.g_w_customer;

  date_t min_date;
  strtodt(&min_date, DATE_MINIMUM);
  nBaseDate = dttoj(&min_date);

  strtodt(&dtBirthMax, "1992-12-31");
  strtodt(&dtBirthMin, "1924-01-01");
  strtodt(&dtToday, TODAYS_DATE);
  jtodt(&dt1YearAgo, dtToday.julian - 365);
  jtodt(&dt10YearsAgo, dtToday.julian - 3650);

  nullSet(&pT->kNullBitMap, C_NULLS, dsdGenContext);
  r->c_customer_sk = index;
  mk_bkey(&r->c_customer_id[0], index, C_CUSTOMER_ID);
  genrand_integer(
      &nTemp, DIST_UNIFORM, 1, 100, 0, C_PREFERRED_CUST_FLAG, dsdGenContext);
  r->c_preferred_cust_flag = (nTemp < C_PREFERRED_PCT) ? 1 : 0;

  /* demographic keys are a composite of values. rebuild them a la
   * bitmap_to_dist */
  r->c_current_hdemo_sk =
      mk_join(C_CURRENT_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1, dsdGenContext);

  r->c_current_cdemo_sk =
      mk_join(C_CURRENT_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1, dsdGenContext);

  r->c_current_addr_sk = mk_join(
      C_CURRENT_ADDR_SK, CUSTOMER_ADDRESS, r->c_customer_sk, dsdGenContext);
  nNameIndex = pick_distribution(
      &r->c_first_name, "first_names", 1, 3, C_FIRST_NAME, dsdGenContext);
  pick_distribution(
      &r->c_last_name, "last_names", 1, 1, C_LAST_NAME, dsdGenContext);
  dist_weight(&nGender, "first_names", nNameIndex, 2, dsdGenContext);
  pick_distribution(
      &r->c_salutation,
      "salutations",
      1,
      (nGender == 0) ? 2 : 3,
      C_SALUTATION,
      dsdGenContext);

  genrand_date(
      &dtTemp,
      DIST_UNIFORM,
      &dtBirthMin,
      &dtBirthMax,
      NULL,
      C_BIRTH_DAY,
      dsdGenContext);
  r->c_birth_day = dtTemp.day;
  r->c_birth_month = dtTemp.month;
  r->c_birth_year = dtTemp.year;
  genrand_email(
      r->c_email_address,
      r->c_first_name,
      r->c_last_name,
      C_EMAIL_ADDRESS,
      dsdGenContext);
  genrand_date(
      &dtTemp,
      DIST_UNIFORM,
      &dt1YearAgo,
      &dtToday,
      NULL,
      C_LAST_REVIEW_DATE,
      dsdGenContext);
  r->c_last_review_date = dtTemp.julian;
  genrand_date(
      &dtTemp,
      DIST_UNIFORM,
      &dt10YearsAgo,
      &dtToday,
      NULL,
      C_FIRST_SALES_DATE_ID,
      dsdGenContext);
  r->c_first_sales_date_id = dtTemp.julian;
  r->c_first_shipto_date_id = r->c_first_sales_date_id + 30;

  pick_distribution(
      &r->c_birth_country, "countries", 1, 1, C_BIRTH_COUNTRY, dsdGenContext);

  void* info = append_info_get(info_arr, CUSTOMER);
  append_row_start(info);

  append_key(C_CUSTOMER_SK, info, r->c_customer_sk);
  append_varchar(C_CUSTOMER_ID, info, r->c_customer_id);
  append_key(C_CURRENT_CDEMO_SK, info, r->c_current_cdemo_sk);
  append_key(C_CURRENT_HDEMO_SK, info, r->c_current_hdemo_sk);
  append_key(C_CURRENT_ADDR_SK, info, r->c_current_addr_sk);
  append_key(C_FIRST_SHIPTO_DATE_ID, info, r->c_first_shipto_date_id);
  append_key(C_FIRST_SALES_DATE_ID, info, r->c_first_sales_date_id);
  append_varchar(C_SALUTATION, info, r->c_salutation);
  append_varchar(C_FIRST_NAME, info, r->c_first_name);
  append_varchar(C_LAST_NAME, info, r->c_last_name);
  append_varchar(
      C_PREFERRED_CUST_FLAG, info, r->c_preferred_cust_flag ? "Y" : "N");
  append_integer(C_BIRTH_DAY, info, r->c_birth_day);
  append_integer(C_BIRTH_MONTH, info, r->c_birth_month);
  append_integer(C_BIRTH_YEAR, info, r->c_birth_year);
  append_varchar(C_BIRTH_COUNTRY, info, r->c_birth_country);
  append_varchar(C_LOGIN, info, &r->c_login[0]);
  append_varchar(C_EMAIL_ADDRESS, info, &r->c_email_address[0]);
  append_key(C_LAST_REVIEW_DATE, info, r->c_last_review_date);

  append_row_end(info);

  return 0;
}
