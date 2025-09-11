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

#include "velox/tpcds/gen/dsdgen/include/w_web_site.h"

#include "velox/tpcds/gen/dsdgen/include/address.h"
#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/misc.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/scd.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

#include <stdio.h>

static struct W_WEB_SITE_TBL g_OldValues;

/*
 * Routine: mk_web_site()
 * Purpose: populate the web_site table
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
int mk_w_web_site(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  int32_t nFieldChangeFlags, bFirstRecord = 0;
  decimal_t dMinTaxPercentage, dMaxTaxPercentage;

  /* begin locals declarations */
  std::vector<char> szTemp(16);
  char *sName1 = nullptr, *sName2 = nullptr;
  struct W_WEB_SITE_TBL *r, *rOldValues = &g_OldValues;
  tdef* pT = getSimpleTdefsByNumber(WEB_SITE, dsdGenContext);

  r = &dsdGenContext.g_w_web_site;

  snprintf(
      szTemp.data(),
      szTemp.size(),
      "%d-%d-%d",
      CURRENT_YEAR,
      CURRENT_MONTH,
      CURRENT_DAY);
  strcpy(r->web_class, "Unknown");
  strtodec(&dMinTaxPercentage, WEB_MIN_TAX_PERCENTAGE);
  strtodec(&dMaxTaxPercentage, WEB_MAX_TAX_PERCENTAGE);

  nullSet(&pT->kNullBitMap, WEB_NULLS, dsdGenContext);
  r->web_site_sk = index;

  /* if we have generated the required history for this business key and
   * generate a new one then reset associate fields (e.g., rec_start_date
   * minimums)
   */
  if (setSCDKeys(
          WEB_SITE_ID,
          index,
          r->web_site_id,
          &r->web_rec_start_date_id,
          &r->web_rec_end_date_id,
          dsdGenContext)) {
    r->web_open_date = mk_join(WEB_OPEN_DATE, DATET, index, dsdGenContext);
    r->web_close_date = mk_join(WEB_CLOSE_DATE, DATET, index, dsdGenContext);
    if (r->web_close_date > r->web_rec_end_date_id)
      r->web_close_date = -1;
    snprintf(
        r->web_name,
        sizeof(r->web_name),
        "site_%d",
        static_cast<int>(index / 6));
    bFirstRecord = 1;
  }

  /*
   * this is  where we select the random number that controls if a field
   * changes from one record to the next.
   */
  nFieldChangeFlags = next_random(WEB_SCD, dsdGenContext);

  /* the rest of the record in a history-keeping dimension can either be a new
   * data value or not; use a random number and its bit pattern to determine
   * which fields to replace and which to retain
   */
  pick_distribution(&sName1, "first_names", 1, 1, WEB_MANAGER, dsdGenContext);
  pick_distribution(&sName2, "last_names", 1, 1, WEB_MANAGER, dsdGenContext);
  snprintf(r->web_manager, sizeof(r->web_manager), "%s %s", sName1, sName2);
  changeSCD(
      SCD_CHAR,
      &r->web_manager,
      &rOldValues->web_manager,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->web_market_id, DIST_UNIFORM, 1, 6, 0, WEB_MARKET_ID, dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->web_market_id,
      &rOldValues->web_market_id,
      &nFieldChangeFlags,
      bFirstRecord);

  gen_text(
      r->web_market_class,
      20,
      RS_WEB_MARKET_CLASS,
      WEB_MARKET_CLASS,
      dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->web_market_class,
      &rOldValues->web_market_class,
      &nFieldChangeFlags,
      bFirstRecord);

  gen_text(
      r->web_market_desc,
      20,
      RS_WEB_MARKET_DESC,
      WEB_MARKET_DESC,
      dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->web_market_desc,
      &rOldValues->web_market_desc,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(
      &sName1, "first_names", 1, 1, WEB_MARKET_MANAGER, dsdGenContext);
  pick_distribution(
      &sName2, "last_names", 1, 1, WEB_MARKET_MANAGER, dsdGenContext);
  snprintf(
      r->web_market_manager,
      sizeof(r->web_market_manager),
      "%s %s",
      sName1,
      sName2);
  changeSCD(
      SCD_CHAR,
      &r->web_market_manager,
      &rOldValues->web_market_manager,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->web_company_id, DIST_UNIFORM, 1, 6, 0, WEB_COMPANY_ID, dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->web_company_id,
      &rOldValues->web_company_id,
      &nFieldChangeFlags,
      bFirstRecord);

  mk_word(
      r->web_company_name,
      "syllables",
      r->web_company_id,
      RS_WEB_COMPANY_NAME,
      WEB_COMPANY_NAME,
      dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->web_company_name,
      &rOldValues->web_company_name,
      &nFieldChangeFlags,
      bFirstRecord);

  mk_address(&r->web_address, WEB_ADDRESS, dsdGenContext);
  changeSCD(
      SCD_PTR,
      &r->web_address.city,
      &rOldValues->web_address.city,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->web_address.county,
      &rOldValues->web_address.county,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_INT,
      &r->web_address.gmt_offset,
      &rOldValues->web_address.gmt_offset,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->web_address.state,
      &rOldValues->web_address.state,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->web_address.street_type,
      &rOldValues->web_address.street_type,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->web_address.street_name1,
      &rOldValues->web_address.street_name1,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_PTR,
      &r->web_address.street_name2,
      &rOldValues->web_address.street_name2,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_INT,
      &r->web_address.street_num,
      &rOldValues->web_address.street_num,
      &nFieldChangeFlags,
      bFirstRecord);
  changeSCD(
      SCD_INT,
      &r->web_address.zip,
      &rOldValues->web_address.zip,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_decimal(
      &r->web_tax_percentage,
      DIST_UNIFORM,
      &dMinTaxPercentage,
      &dMaxTaxPercentage,
      NULL,
      WEB_TAX_PERCENTAGE,
      dsdGenContext);
  changeSCD(
      SCD_DEC,
      &r->web_tax_percentage,
      &rOldValues->web_tax_percentage,
      &nFieldChangeFlags,
      bFirstRecord);

  void* info = append_info_get(info_arr, WEB_SITE);
  append_row_start(info);

  std::vector<char> szStreetName(128);

  append_key(WEB_SITE_SK, info, r->web_site_sk);
  append_varchar(WEB_SITE_ID, info, &r->web_site_id[0]);
  append_date(
      WEB_REC_START_DATE_ID, info, static_cast<int>(r->web_rec_start_date_id));
  append_date(
      WEB_REC_END_DATE_ID, info, static_cast<int>(r->web_rec_end_date_id));
  append_varchar(WEB_NAME, info, &r->web_name[0]);
  append_key(WEB_OPEN_DATE, info, r->web_open_date);
  append_key(WEB_CLOSE_DATE, info, r->web_close_date);
  append_varchar(WEB_CLASS, info, &r->web_class[0]);
  append_varchar(WEB_MANAGER, info, &r->web_manager[0]);
  append_integer(WEB_MARKET_ID, info, r->web_market_id);
  append_varchar(WEB_MARKET_CLASS, info, &r->web_market_class[0]);
  append_varchar(WEB_MARKET_DESC, info, &r->web_market_desc[0]);
  append_varchar(WEB_MARKET_MANAGER, info, &r->web_market_manager[0]);
  append_integer(WEB_COMPANY_ID, info, r->web_company_id);
  append_varchar(WEB_COMPANY_NAME, info, &r->web_company_name[0]);
  append_varchar(
      WEB_ADDRESS_STREET_NUM, info, std::to_string(r->web_address.street_num));
  if (r->web_address.street_name2) {
    snprintf(
        szStreetName.data(),
        szStreetName.size(),
        "%s %s",
        r->web_address.street_name1,
        r->web_address.street_name2);
    append_varchar(WEB_ADDRESS_STREET_NAME1, info, szStreetName.data());
  } else
    append_varchar(WEB_ADDRESS_STREET_NAME1, info, r->web_address.street_name1);
  append_varchar(WEB_ADDRESS_STREET_TYPE, info, r->web_address.street_type);
  append_varchar(WEB_ADDRESS_SUITE_NUM, info, r->web_address.suite_num);
  append_varchar(WEB_ADDRESS_CITY, info, r->web_address.city);
  append_varchar(WEB_ADDRESS_COUNTY, info, r->web_address.county);
  append_varchar(WEB_ADDRESS_STATE, info, r->web_address.state);
  snprintf(
      szStreetName.data(), szStreetName.size(), "%05d", r->web_address.zip);
  append_varchar(WEB_ADDRESS_ZIP, info, szStreetName.data());
  append_varchar(WEB_ADDRESS_COUNTRY, info, r->web_address.country);
  append_integer_decimal(
      WEB_ADDRESS_GMT_OFFSET, info, r->web_address.gmt_offset);
  append_decimal(WEB_TAX_PERCENTAGE, info, &r->web_tax_percentage);

  append_row_end(info);

  return 0;
}
