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

#include "w_store.h"

#include "append_info.h"
#include "build_support.h"
#include "config.h"
#include "constants.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "misc.h"
#include "nulls.h"
#include "parallel.h"
#include "porting.h"
#include "scaling.h"
#include "scd.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>
#include <string>

static struct W_STORE_TBL g_store_OldValues;
/*
 * mk_store
 */
int mk_w_store(void* info_arr, ds_key_t index, DSDGenContext& dsdGenContext) {
  int32_t nFieldChangeFlags = 0, bFirstRecord = 0;

  /* begin locals declarations */
  static decimal_t dRevMin, dRevMax;
  char *sName1 = nullptr, *sName2 = nullptr, *szTemp = nullptr;
  int32_t nHierarchyTotal, nStoreType, nPercentage, nDaysOpen, nMin, nMax;
  static date_t tDate;
  static decimal_t min_rev_growth, max_rev_growth, dMinTaxPercentage,
      dMaxTaxPercentage;
  struct W_STORE_TBL *r, *rOldValues = &g_store_OldValues;
  tdef* pT = getSimpleTdefsByNumber(STORE, dsdGenContext);

  r = &dsdGenContext.g_w_store;

  if (!dsdGenContext.mk_w_store_init) {
    nHierarchyTotal = static_cast<int>(get_rowcount(DIVISIONS, dsdGenContext));
    nHierarchyTotal *= static_cast<int>(get_rowcount(COMPANY, dsdGenContext));
    strtodt(&tDate, DATE_MINIMUM);
    strtodec(&min_rev_growth, STORE_MIN_REV_GROWTH);
    strtodec(&max_rev_growth, STORE_MAX_REV_GROWTH);
    strtodec(&dRevMin, "1.00");
    strtodec(&dRevMax, "1000000.00");
    strtodec(&dMinTaxPercentage, STORE_MIN_TAX_PERCENTAGE);
    strtodec(&dMaxTaxPercentage, STORE_MAX_TAX_PERCENTAGE);

    /* columns that should be dynamic */
    r->rec_end_date_id = -1;
    dsdGenContext.mk_w_store_init = 1;
  }

  nullSet(&pT->kNullBitMap, W_STORE_NULLS, dsdGenContext);
  r->store_sk = index;

  /* if we have generated the required history for this business key and
   * generate a new one then reset associate fields (e.g., rec_start_date
   * minimums)
   */
  if (setSCDKeys(
          S_STORE_ID,
          index,
          r->store_id,
          &r->rec_start_date_id,
          &r->rec_end_date_id,
          dsdGenContext)) {
    bFirstRecord = 1;
  }

  /*
   * this is  where we select the random number that controls if a field
   * changes from one record to the next.
   */
  nFieldChangeFlags = next_random(W_STORE_SCD, dsdGenContext);

  /* the rest of the record in a history-keeping dimension can either be a new
   * data value or not; use a random number and its bit pattern to determine
   * which fields to replace and which to retain
   */
  nPercentage = genrand_integer(
      NULL, DIST_UNIFORM, 1, 100, 0, W_STORE_CLOSED_DATE_ID, dsdGenContext);
  nDaysOpen = genrand_integer(
      NULL,
      DIST_UNIFORM,
      STORE_MIN_DAYS_OPEN,
      STORE_MAX_DAYS_OPEN,
      0,
      W_STORE_CLOSED_DATE_ID,
      dsdGenContext);
  if (nPercentage < STORE_CLOSED_PCT)
    r->closed_date_id = tDate.julian + nDaysOpen;
  else
    r->closed_date_id = -1;
  changeSCD(
      SCD_KEY,
      &r->closed_date_id,
      &rOldValues->closed_date_id,
      &nFieldChangeFlags,
      bFirstRecord);
  if (!r->closed_date_id)
    r->closed_date_id = -1; /* dates use a special NULL indicator */

  mk_word(
      r->store_name,
      "syllables",
      static_cast<long>(index),
      5,
      W_STORE_NAME,
      dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->store_name,
      &rOldValues->store_name,
      &nFieldChangeFlags,
      bFirstRecord);

  /*
   * use the store type to set the parameters for the rest of the attributes
   */
  nStoreType = pick_distribution(
      &szTemp, "store_type", 1, 1, W_STORE_TYPE, dsdGenContext);
  dist_member(&nMin, "store_type", nStoreType, 2, dsdGenContext);
  dist_member(&nMax, "store_type", nStoreType, 3, dsdGenContext);
  genrand_integer(
      &r->employees,
      DIST_UNIFORM,
      nMin,
      nMax,
      0,
      W_STORE_EMPLOYEES,
      dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->employees,
      &rOldValues->employees,
      &nFieldChangeFlags,
      bFirstRecord);

  dist_member(&nMin, "store_type", nStoreType, 4, dsdGenContext);
  dist_member(&nMax, "store_type", nStoreType, 5, dsdGenContext),
      genrand_integer(
          &r->floor_space,
          DIST_UNIFORM,
          nMin,
          nMax,
          0,
          W_STORE_FLOOR_SPACE,
          dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->floor_space,
      &rOldValues->floor_space,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(
      &r->hours, "call_center_hours", 1, 1, W_STORE_HOURS, dsdGenContext);
  changeSCD(
      SCD_PTR, &r->hours, &rOldValues->hours, &nFieldChangeFlags, bFirstRecord);

  pick_distribution(
      &sName1, "first_names", 1, 1, W_STORE_MANAGER, dsdGenContext);
  pick_distribution(
      &sName2, "last_names", 1, 1, W_STORE_MANAGER, dsdGenContext);
  snprintf(r->store_manager, sizeof(r->store_manager), "%s %s", sName1, sName2);
  changeSCD(
      SCD_CHAR,
      &r->store_manager,
      &rOldValues->store_manager,
      &nFieldChangeFlags,
      bFirstRecord);

  r->market_id = genrand_integer(
      NULL, DIST_UNIFORM, 1, 10, 0, W_STORE_MARKET_ID, dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->market_id,
      &rOldValues->market_id,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_decimal(
      &r->dTaxPercentage,
      DIST_UNIFORM,
      &dMinTaxPercentage,
      &dMaxTaxPercentage,
      NULL,
      W_STORE_TAX_PERCENTAGE,
      dsdGenContext);
  changeSCD(
      SCD_DEC,
      &r->dTaxPercentage,
      &rOldValues->dTaxPercentage,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(
      &r->geography_class,
      "geography_class",
      1,
      1,
      W_STORE_GEOGRAPHY_CLASS,
      dsdGenContext);
  changeSCD(
      SCD_PTR,
      &r->geography_class,
      &rOldValues->geography_class,
      &nFieldChangeFlags,
      bFirstRecord);

  gen_text(
      &r->market_desc[0],
      STORE_DESC_MIN,
      RS_S_MARKET_DESC,
      W_STORE_MARKET_DESC,
      dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->market_desc,
      &rOldValues->market_desc,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(
      &sName1, "first_names", 1, 1, W_STORE_MARKET_MANAGER, dsdGenContext);
  pick_distribution(
      &sName2, "last_names", 1, 1, W_STORE_MARKET_MANAGER, dsdGenContext);
  snprintf(
      r->market_manager, sizeof(r->market_manager), "%s %s", sName1, sName2);
  changeSCD(
      SCD_CHAR,
      &r->market_manager,
      &rOldValues->market_manager,
      &nFieldChangeFlags,
      bFirstRecord);

  r->division_id = pick_distribution(
      &r->division_name,
      "divisions",
      1,
      1,
      W_STORE_DIVISION_NAME,
      dsdGenContext);
  changeSCD(
      SCD_KEY,
      &r->division_id,
      &rOldValues->division_id,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->division_name,
      &rOldValues->division_name,
      &nFieldChangeFlags,
      bFirstRecord);

  r->company_id = pick_distribution(
      &r->company_name, "stores", 1, 1, W_STORE_COMPANY_NAME, dsdGenContext);
  changeSCD(
      SCD_KEY,
      &r->company_id,
      &rOldValues->company_id,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->company_name,
      &rOldValues->company_name,
      &nFieldChangeFlags,
      bFirstRecord);

  mk_address(&r->address, W_STORE_ADDRESS, dsdGenContext);
  changeSCD(
      SCD_PTR,
      &r->address.city,
      &rOldValues->address.city,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->address.county,
      &rOldValues->address.county,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_INT,
      &r->address.gmt_offset,
      &rOldValues->address.gmt_offset,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->address.state,
      &rOldValues->address.state,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->address.street_type,
      &rOldValues->address.street_type,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->address.street_name1,
      &rOldValues->address.street_name1,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->address.street_name2,
      &rOldValues->address.street_name2,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_INT,
      &r->address.street_num,
      &rOldValues->address.street_num,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_INT,
      &r->address.zip,
      &rOldValues->address.zip,
      &nFieldChangeFlags,
      bFirstRecord);

  char szTemp2[128];

  void* info = append_info_get(info_arr, STORE);
  append_row_start(info);

  append_key(W_STORE_SK, info, r->store_sk);
  append_varchar(W_STORE_ID, info, r->store_id);
  append_date(W_STORE_REC_START_DATE_ID, info, r->rec_start_date_id);
  append_date(W_STORE_REC_END_DATE_ID, info, r->rec_end_date_id);
  append_key(W_STORE_CLOSED_DATE_ID, info, r->closed_date_id);
  append_varchar(W_STORE_NAME, info, r->store_name);
  append_integer(W_STORE_EMPLOYEES, info, r->employees);
  append_integer(W_STORE_FLOOR_SPACE, info, r->floor_space);
  append_varchar(W_STORE_HOURS, info, r->hours);
  append_varchar(W_STORE_MANAGER, info, &r->store_manager[0]);
  append_integer(W_STORE_MARKET_ID, info, r->market_id);
  append_varchar(W_STORE_GEOGRAPHY_CLASS, info, r->geography_class);
  append_varchar(W_STORE_MARKET_DESC, info, &r->market_desc[0]);
  append_varchar(W_STORE_MARKET_MANAGER, info, &r->market_manager[0]);
  append_integer(W_STORE_DIVISION_ID, info, r->division_id);
  append_varchar(W_STORE_DIVISION_NAME, info, r->division_name);
  append_integer(W_STORE_COMPANY_ID, info, r->company_id);
  append_varchar(W_STORE_COMPANY_NAME, info, r->company_name);
  append_varchar(
      W_STORE_ADDRESS_STREET_NUM, info, std::to_string(r->address.street_num));
  if (r->address.street_name2) {
    snprintf(
        szTemp2,
        sizeof(szTemp2),
        "%s %s",
        r->address.street_name1,
        r->address.street_name2);
    append_varchar(W_STORE_ADDRESS_STREET_NAME1, info, szTemp2);
  } else
    append_varchar(W_STORE_ADDRESS_STREET_NAME1, info, r->address.street_name1);
  append_varchar(W_STORE_ADDRESS_STREET_TYPE, info, r->address.street_type);
  append_varchar(W_STORE_ADDRESS_SUITE_NUM, info, r->address.suite_num);
  append_varchar(W_STORE_ADDRESS_CITY, info, r->address.city);
  append_varchar(W_STORE_ADDRESS_COUNTY, info, r->address.county);
  append_varchar(W_STORE_ADDRESS_STATE, info, r->address.state);
  snprintf(szTemp2, sizeof(szTemp2), "%05d", r->address.zip);
  append_varchar(W_STORE_ADDRESS_ZIP, info, szTemp2);
  append_varchar(W_STORE_ADDRESS_COUNTRY, info, r->address.country);
  append_integer_decimal(
      W_STORE_ADDRESS_GMT_OFFSET, info, r->address.gmt_offset);
  append_decimal(W_STORE_TAX_PERCENTAGE, info, &r->dTaxPercentage);

  append_row_end(info);

  return 0;
}
