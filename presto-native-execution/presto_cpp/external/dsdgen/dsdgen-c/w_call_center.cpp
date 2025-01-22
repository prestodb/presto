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

#include "w_call_center.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "date.h"
#include "decimal.h"
#include "dist.h"
#include "genrand.h"
#include "misc.h"
#include "nulls.h"
#include "parallel.h"
#include "porting.h"
#include "r_params.h"
#include "scaling.h"
#include "scd.h"
#include "tables.h"
#include "tdefs.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static struct CALL_CENTER_TBL g_call_center_OldValues;

/*
 * Routine: mk_w_call_center()
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
 * TODO:
 * 20020830 jms Need to populate open and close dates
 */

int mk_w_call_center(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  int32_t jDateStart, nDaysPerRevision;
  int32_t nSuffix, bFirstRecord = 0, nFieldChangeFlags, jDateEnd, nDateRange;
  char *cp = nullptr, *sName1 = nullptr, *sName2 = nullptr;
  decimal_t dMinTaxPercentage, dMaxTaxPercentage;
  tdef* pTdef = getSimpleTdefsByNumber(CALL_CENTER, dsdGenContext);

  /* begin locals declarations */
  date_t dTemp;
  double nScale;
  struct CALL_CENTER_TBL *r, *rOldValues = &g_call_center_OldValues;

  r = &dsdGenContext.g_w_call_center;

  strtodt(&dTemp, DATA_START_DATE);
  jDateStart = dttoj(&dTemp) - WEB_SITE;
  strtodt(&dTemp, DATA_END_DATE);
  jDateEnd = dttoj(&dTemp);
  nDateRange = jDateEnd - jDateStart + 1;
  nDaysPerRevision = nDateRange / pTdef->nParam + 1;
  nScale = get_dbl("SCALE", dsdGenContext);

  r->cc_division_id = -1;
  r->cc_closed_date_id = -1;
  strcpy(r->cc_division_name, "No Name");

  strtodec(&dMinTaxPercentage, MIN_CC_TAX_PERCENTAGE);
  strtodec(&dMaxTaxPercentage, MAX_CC_TAX_PERCENTAGE);

  nullSet(&pTdef->kNullBitMap, CC_NULLS, dsdGenContext);
  r->cc_call_center_sk = index;

  /* if we have generated the required history for this business key and
   * generate a new one then reset associate fields (e.g., rec_start_date
   * minimums)
   */
  if (setSCDKeys(
          CC_CALL_CENTER_ID,
          index,
          r->cc_call_center_id,
          &r->cc_rec_start_date_id,
          &r->cc_rec_end_date_id,
          dsdGenContext)) {
    r->cc_open_date_id =
        jDateStart -
        genrand_integer(
            NULL, DIST_UNIFORM, -365, 0, 0, CC_OPEN_DATE_ID, dsdGenContext);

    /*
     * some fields are not changed, even when a new version of the row is
     * written
     */
    nSuffix = static_cast<int>(index / distsize("call_centers", dsdGenContext));
    dist_member(
        &cp,
        "call_centers",
        static_cast<int>((index % distsize("call_centers", dsdGenContext)) + 1),
        1,
        dsdGenContext);
    if (nSuffix > 0) {
      snprintf(r->cc_name, RS_CC_NAME + 1, "%s_%d", cp, nSuffix);
    } else
      strcpy(r->cc_name, cp);

    mk_address(&r->cc_address, CC_ADDRESS, dsdGenContext);
    bFirstRecord = 1;
  }

  /*
   * this is  where we select the random number that controls if a field
   * changes from one record to the next.
   */
  nFieldChangeFlags = next_random(CC_SCD, dsdGenContext);

  /* the rest of the record in a history-keeping dimension can either be a new
   * data value or not; use a random number and its bit pattern to determine
   * which fields to replace and which to retain
   */
  pick_distribution(
      &r->cc_class, "call_center_class", 1, 1, CC_CLASS, dsdGenContext);
  changeSCD(
      SCD_PTR,
      &r->cc_class,
      &rOldValues->cc_class,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->cc_employees,
      DIST_UNIFORM,
      1,
      nScale >= 1 ? static_cast<int64_t>(CC_EMPLOYEE_MAX * nScale * nScale)
                  : static_cast<int>(CC_EMPLOYEE_MAX),
      0,
      CC_EMPLOYEES,
      dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->cc_employees,
      &rOldValues->cc_employees,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->cc_sq_ft, DIST_UNIFORM, 100, 700, 0, CC_SQ_FT, dsdGenContext);
  r->cc_sq_ft *= r->cc_employees;
  changeSCD(
      SCD_INT,
      &r->cc_sq_ft,
      &rOldValues->cc_sq_ft,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(
      &r->cc_hours, "call_center_hours", 1, 1, CC_HOURS, dsdGenContext);
  changeSCD(
      SCD_PTR,
      &r->cc_hours,
      &rOldValues->cc_hours,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(&sName1, "first_names", 1, 1, CC_MANAGER, dsdGenContext);
  pick_distribution(&sName2, "last_names", 1, 1, CC_MANAGER, dsdGenContext);
  snprintf(&r->cc_manager[0], RS_CC_MANAGER + 1, "%s %s", sName1, sName2);
  changeSCD(
      SCD_CHAR,
      &r->cc_manager,
      &rOldValues->cc_manager,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->cc_market_id, DIST_UNIFORM, 1, 6, 0, CC_MARKET_ID, dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->cc_market_id,
      &rOldValues->cc_market_id,
      &nFieldChangeFlags,
      bFirstRecord);

  gen_text(
      r->cc_market_class,
      20,
      RS_CC_MARKET_CLASS,
      CC_MARKET_CLASS,
      dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->cc_market_class,
      &rOldValues->cc_market_class,
      &nFieldChangeFlags,
      bFirstRecord);

  gen_text(
      r->cc_market_desc, 20, RS_CC_MARKET_DESC, CC_MARKET_DESC, dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->cc_market_desc,
      &rOldValues->cc_market_desc,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(
      &sName1, "first_names", 1, 1, CC_MARKET_MANAGER, dsdGenContext);
  pick_distribution(
      &sName2, "last_names", 1, 1, CC_MARKET_MANAGER, dsdGenContext);
  snprintf(
      &r->cc_market_manager[0],
      RS_CC_MARKET_MANAGER + 1,
      "%s %s",
      sName1,
      sName2);
  changeSCD(
      SCD_CHAR,
      &r->cc_market_manager,
      &rOldValues->cc_market_manager,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->cc_company, DIST_UNIFORM, 1, 6, 0, CC_COMPANY, dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->cc_company,
      &rOldValues->cc_company,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->cc_division_id, DIST_UNIFORM, 1, 6, 0, CC_COMPANY, dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->cc_division_id,
      &rOldValues->cc_division_id,
      &nFieldChangeFlags,
      bFirstRecord);

  mk_word(
      r->cc_division_name,
      "syllables",
      r->cc_division_id,
      RS_CC_DIVISION_NAME,
      CC_DIVISION_NAME,
      dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->cc_division_name,
      &rOldValues->cc_division_name,
      &nFieldChangeFlags,
      bFirstRecord);

  mk_companyname(
      r->cc_company_name, CC_COMPANY_NAME, r->cc_company, dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->cc_company_name,
      &rOldValues->cc_company_name,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_decimal(
      &r->cc_tax_percentage,
      DIST_UNIFORM,
      &dMinTaxPercentage,
      &dMaxTaxPercentage,
      NULL,
      CC_TAX_PERCENTAGE,
      dsdGenContext);
  changeSCD(
      SCD_DEC,
      &r->cc_tax_percentage,
      &rOldValues->cc_tax_percentage,
      &nFieldChangeFlags,
      bFirstRecord);

  // append the newly created row
  char szTemp[128];

  void* info = append_info_get(info_arr, CALL_CENTER);

  append_row_start(info);
  append_key(CC_CALL_CENTER_SK, info, r->cc_call_center_sk);
  append_varchar(CC_CALL_CENTER_ID, info, r->cc_call_center_id);
  append_date(CC_REC_START_DATE_ID, info, r->cc_rec_start_date_id);
  append_date(CC_REC_END_DATE_ID, info, r->cc_rec_end_date_id);
  append_integer(CC_CLOSED_DATE_ID, info, r->cc_closed_date_id);
  append_integer(CC_OPEN_DATE_ID, info, r->cc_open_date_id);
  append_varchar(CC_NAME, info, r->cc_name);
  append_varchar(CC_CLASS, info, &r->cc_class[0]);
  append_integer(CC_EMPLOYEES, info, r->cc_employees);
  append_integer(CC_SQ_FT, info, r->cc_sq_ft);
  append_varchar(CC_HOURS, info, r->cc_hours);
  append_varchar(CC_MANAGER, info, &r->cc_manager[0]);
  append_integer(CC_MARKET_ID, info, r->cc_market_id);
  append_varchar(CC_MARKET_CLASS, info, &r->cc_market_class[0]);
  append_varchar(CC_MARKET_DESC, info, &r->cc_market_desc[0]);
  append_varchar(CC_MARKET_MANAGER, info, &r->cc_market_manager[0]);
  append_integer(CC_DIVISION, info, r->cc_division_id);
  append_varchar(CC_DIVISION_NAME, info, &r->cc_division_name[0]);
  append_integer(CC_COMPANY, info, r->cc_company);
  append_varchar(CC_COMPANY_NAME, info, &r->cc_company_name[0]);
  append_varchar(CC_ADDRESS, info, std::to_string(r->cc_address.street_num));

  if (r->cc_address.street_name2) {
    snprintf(
        szTemp,
        sizeof(szTemp),
        "%s %s",
        r->cc_address.street_name1,
        r->cc_address.street_name2);
    append_varchar(CC_ADDRESS, info, szTemp);
  } else {
    append_varchar(CC_ADDRESS, info, r->cc_address.street_name1);
  }

  append_varchar(CC_ADDRESS, info, r->cc_address.street_type);
  append_varchar(CC_ADDRESS, info, &r->cc_address.suite_num[0]);
  append_varchar(CC_ADDRESS, info, r->cc_address.city);
  append_varchar(CC_ADDRESS, info, r->cc_address.county);
  append_varchar(CC_ADDRESS, info, r->cc_address.state);
  snprintf(szTemp, sizeof(szTemp), "%05d", r->cc_address.zip);
  append_varchar(CC_ADDRESS, info, szTemp);
  append_varchar(CC_ADDRESS, info, &r->cc_address.country[0]);
  append_integer_decimal(CC_GMT_OFFSET, info, r->cc_address.gmt_offset);
  append_decimal(CC_TAX_PERCENTAGE, info, &r->cc_tax_percentage);

  append_row_end(info);

  return 0;
}
