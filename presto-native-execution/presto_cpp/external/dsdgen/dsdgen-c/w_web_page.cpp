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

#include "w_web_page.h"

#include "append_info.h"
#include "build_support.h"
#include "config.h"
#include "constants.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "misc.h"
#include "nulls.h"
#include "porting.h"
#include "scaling.h"
#include "scd.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>

static struct W_WEB_PAGE_TBL g_web_page_OldValues;

/*
 * Routine: mk_web_page()
 * Purpose: populate the web_page table
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
 * 20020815 jms check text generation/seed usage
 */
int mk_w_web_page(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  int32_t bFirstRecord = 0, nFieldChangeFlags;
  date_t dToday;
  ds_key_t nConcurrent, nRevisions;

  /* begin locals declarations */
  int32_t nTemp, nAccess;
  char szTemp[16];
  struct W_WEB_PAGE_TBL *r, *rOldValues = &g_web_page_OldValues;
  tdef* pT = getSimpleTdefsByNumber(WEB_PAGE, dsdGenContext);

  r = &dsdGenContext.g_w_web_page;

  /* setup invariant values */
  snprintf(
      szTemp,
      sizeof(szTemp),
      "%d-%d-%d",
      CURRENT_YEAR,
      CURRENT_MONTH,
      CURRENT_DAY);
  strtodt(&dToday, szTemp);
  /* set up for the SCD handling */
  nConcurrent =
      static_cast<int>(get_rowcount(CONCURRENT_WEB_SITES, dsdGenContext));
  nRevisions =
      static_cast<int>(get_rowcount(WEB_PAGE, dsdGenContext) / nConcurrent);

  nullSet(&pT->kNullBitMap, WP_NULLS, dsdGenContext);
  r->wp_page_sk = index;

  /* if we have generated the required history for this business key and
   * generate a new one then reset associate fields (e.g., rec_start_date
   * minimums)
   */
  if (setSCDKeys(
          WP_PAGE_ID,
          index,
          r->wp_page_id,
          &r->wp_rec_start_date_id,
          &r->wp_rec_end_date_id,
          dsdGenContext)) {
    /*
     * some fields are not changed, even when a new version of the row is
     * written
     */
    bFirstRecord = 1;
  }

  /*
   * this is  where we select the random number that controls if a field
   * changes from one record to the next.
   */
  nFieldChangeFlags = next_random(WP_SCD, dsdGenContext);

  r->wp_creation_date_sk =
      mk_join(WP_CREATION_DATE_SK, DATET, index, dsdGenContext);
  changeSCD(
      SCD_KEY,
      &r->wp_creation_date_sk,
      &rOldValues->wp_creation_date_sk,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &nAccess,
      DIST_UNIFORM,
      0,
      WP_IDLE_TIME_MAX,
      0,
      WP_ACCESS_DATE_SK,
      dsdGenContext);
  r->wp_access_date_sk = dToday.julian - nAccess;
  changeSCD(
      SCD_KEY,
      &r->wp_access_date_sk,
      &rOldValues->wp_access_date_sk,
      &nFieldChangeFlags,
      bFirstRecord);
  if (r->wp_access_date_sk == 0)
    r->wp_access_date_sk = -1; /* special case for dates */

  genrand_integer(
      &nTemp, DIST_UNIFORM, 0, 99, 0, WP_AUTOGEN_FLAG, dsdGenContext);
  r->wp_autogen_flag = (nTemp < WP_AUTOGEN_PCT) ? 1 : 0;
  changeSCD(
      SCD_INT,
      &r->wp_autogen_flag,
      &rOldValues->wp_autogen_flag,
      &nFieldChangeFlags,
      bFirstRecord);

  r->wp_customer_sk = mk_join(WP_CUSTOMER_SK, CUSTOMER, 1, dsdGenContext);
  changeSCD(
      SCD_KEY,
      &r->wp_customer_sk,
      &rOldValues->wp_customer_sk,
      &nFieldChangeFlags,
      bFirstRecord);

  if (!r->wp_autogen_flag)
    r->wp_customer_sk = -1;

  genrand_url(r->wp_url, WP_URL);
  changeSCD(
      SCD_CHAR,
      &r->wp_url,
      &rOldValues->wp_url,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(&r->wp_type, "web_page_use", 1, 1, WP_TYPE, dsdGenContext);
  changeSCD(
      SCD_PTR,
      &r->wp_type,
      &rOldValues->wp_type,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->wp_link_count,
      DIST_UNIFORM,
      WP_LINK_MIN,
      WP_LINK_MAX,
      0,
      WP_LINK_COUNT,
      dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->wp_link_count,
      &rOldValues->wp_link_count,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->wp_image_count,
      DIST_UNIFORM,
      WP_IMAGE_MIN,
      WP_IMAGE_MAX,
      0,
      WP_IMAGE_COUNT,
      dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->wp_image_count,
      &rOldValues->wp_image_count,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->wp_max_ad_count,
      DIST_UNIFORM,
      WP_AD_MIN,
      WP_AD_MAX,
      0,
      WP_MAX_AD_COUNT,
      dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->wp_max_ad_count,
      &rOldValues->wp_max_ad_count,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_integer(
      &r->wp_char_count,
      DIST_UNIFORM,
      r->wp_link_count * 125 + r->wp_image_count * 50,
      r->wp_link_count * 300 + r->wp_image_count * 150,
      0,
      WP_CHAR_COUNT,
      dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->wp_char_count,
      &rOldValues->wp_char_count,
      &nFieldChangeFlags,
      bFirstRecord);

  void* info = append_info_get(info_arr, WEB_PAGE);
  append_row_start(info);

  append_key(WP_PAGE_SK, info, r->wp_page_sk);
  append_varchar(WP_PAGE_ID, info, r->wp_page_id);
  append_date(WP_REC_START_DATE_ID, info, r->wp_rec_start_date_id);
  append_date(WP_REC_END_DATE_ID, info, r->wp_rec_end_date_id);
  append_key(WP_CREATION_DATE_SK, info, r->wp_creation_date_sk);
  append_key(WP_ACCESS_DATE_SK, info, r->wp_access_date_sk);
  append_varchar(WP_AUTOGEN_FLAG, info, r->wp_autogen_flag ? "Y" : "N");
  append_key(WP_CUSTOMER_SK, info, r->wp_customer_sk);
  append_varchar(WP_URL, info, &r->wp_url[0]);
  append_varchar(WP_TYPE, info, &r->wp_type[0]);
  append_integer(WP_CHAR_COUNT, info, r->wp_char_count);
  append_integer(WP_LINK_COUNT, info, r->wp_link_count);
  append_integer(WP_IMAGE_COUNT, info, r->wp_image_count);
  append_integer(WP_MAX_AD_COUNT, info, r->wp_max_ad_count);
  append_row_end(info);

  return 0;
}
