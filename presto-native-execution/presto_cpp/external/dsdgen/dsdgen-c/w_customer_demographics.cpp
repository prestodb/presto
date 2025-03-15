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

#include "w_customer_demographics.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "constants.h"
#include "genrand.h"
#include "nulls.h"
#include "porting.h"
#include "sparse.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>

/*
 * mk_customer_demographics
 */
int mk_w_customer_demographics(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  struct W_CUSTOMER_DEMOGRAPHICS_TBL* r;
  ds_key_t kTemp;
  tdef* pTdef = getSimpleTdefsByNumber(CUSTOMER_DEMOGRAPHICS, dsdGenContext);

  r = &dsdGenContext.g_w_customer_demographics;

  nullSet(&pTdef->kNullBitMap, CD_NULLS, dsdGenContext);
  r->cd_demo_sk = index;
  kTemp = r->cd_demo_sk - 1;
  bitmap_to_dist(
      &r->cd_gender, "gender", &kTemp, 1, CUSTOMER_DEMOGRAPHICS, dsdGenContext);
  bitmap_to_dist(
      &r->cd_marital_status,
      "marital_status",
      &kTemp,
      1,
      CUSTOMER_DEMOGRAPHICS,
      dsdGenContext);
  bitmap_to_dist(
      &r->cd_education_status,
      "education",
      &kTemp,
      1,
      CUSTOMER_DEMOGRAPHICS,
      dsdGenContext);
  bitmap_to_dist(
      &r->cd_purchase_estimate,
      "purchase_band",
      &kTemp,
      1,
      CUSTOMER_DEMOGRAPHICS,
      dsdGenContext);
  bitmap_to_dist(
      &r->cd_credit_rating,
      "credit_rating",
      &kTemp,
      1,
      CUSTOMER_DEMOGRAPHICS,
      dsdGenContext);
  r->cd_dep_count =
      static_cast<int>((kTemp % static_cast<ds_key_t>(CD_MAX_CHILDREN)));
  kTemp /= static_cast<ds_key_t>(CD_MAX_CHILDREN);
  r->cd_dep_employed_count =
      static_cast<int>((kTemp % static_cast<ds_key_t>(CD_MAX_EMPLOYED)));
  kTemp /= static_cast<ds_key_t>(CD_MAX_EMPLOYED);
  r->cd_dep_college_count =
      static_cast<int>((kTemp % static_cast<ds_key_t>(CD_MAX_COLLEGE)));

  void* info = append_info_get(info_arr, CUSTOMER_DEMOGRAPHICS);
  append_row_start(info);

  append_key(CD_DEMO_SK, info, r->cd_demo_sk);
  append_varchar(CD_GENDER, info, r->cd_gender);
  append_varchar(CD_MARITAL_STATUS, info, r->cd_marital_status);
  append_varchar(CD_EDUCATION_STATUS, info, r->cd_education_status);
  append_integer(CD_PURCHASE_ESTIMATE, info, r->cd_purchase_estimate);
  append_varchar(CD_CREDIT_RATING, info, r->cd_credit_rating);
  append_integer(CD_DEP_COUNT, info, r->cd_dep_count);
  append_integer(CD_DEP_EMPLOYED_COUNT, info, r->cd_dep_employed_count);
  append_integer(CD_DEP_COLLEGE_COUNT, info, r->cd_dep_college_count);

  append_row_end(info);

  return 0;
}
