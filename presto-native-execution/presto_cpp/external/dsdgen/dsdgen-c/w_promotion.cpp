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

#include "w_promotion.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "genrand.h"
#include "misc.h"
#include "nulls.h"
#include "porting.h"
#include "tables.h"
#include "tdefs.h"

#include <stdio.h>

/*
 * Routine: mk_promotion
 * Purpose: populate the promotion table
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
 * 20020829 jms RNG usage on p_promo_name may be too large
 * 20020829 jms RNG usage on P_CHANNEL_DETAILS may be too large
 */
int mk_w_promotion(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  struct W_PROMOTION_TBL* r;

  /* begin locals declarations */
  date_t start_date;
  ds_key_t nTemp;
  int nFlags;
  tdef* pTdef = getSimpleTdefsByNumber(PROMOTION, dsdGenContext);

  r = &dsdGenContext.g_w_promotion;

  if (!dsdGenContext.mk_w_promotion_init) {
    memset(&dsdGenContext.g_w_promotion, 0, sizeof(struct W_PROMOTION_TBL));
    strtodt(&start_date, DATE_MINIMUM);
    dsdGenContext.mk_w_promotion_init = 1;
  } else {
    strtodt(&start_date, DATE_MINIMUM);
  }

  nullSet(&pTdef->kNullBitMap, P_NULLS, dsdGenContext);
  r->p_promo_sk = index;
  mk_bkey(&r->p_promo_id[0], index, P_PROMO_ID);
  nTemp = index;
  r->p_start_date_id = start_date.julian +
      genrand_integer(NULL,
                      DIST_UNIFORM,
                      PROMO_START_MIN,
                      PROMO_START_MAX,
                      PROMO_START_MEAN,
                      P_START_DATE_ID,
                      dsdGenContext);
  r->p_end_date_id = r->p_start_date_id +
      genrand_integer(NULL,
                      DIST_UNIFORM,
                      PROMO_LEN_MIN,
                      PROMO_LEN_MAX,
                      PROMO_LEN_MEAN,
                      P_END_DATE_ID,
                      dsdGenContext);
  r->p_item_sk = mk_join(P_ITEM_SK, ITEM, 1, dsdGenContext);
  strtodec(&r->p_cost, "1000.00");
  r->p_response_target = 1;
  mk_word(
      &r->p_promo_name[0],
      "syllables",
      static_cast<int>(index),
      PROMO_NAME_LEN,
      P_PROMO_NAME,
      dsdGenContext);
  nFlags = static_cast<unsigned int>(genrand_integer(
      NULL, DIST_UNIFORM, 0, 511, 0, P_CHANNEL_DMAIL, dsdGenContext));
  r->p_channel_dmail = nFlags & 0x01;
  nFlags <<= 1;
  r->p_channel_email = nFlags & 0x01;
  nFlags <<= 1;
  r->p_channel_catalog = nFlags & 0x01;
  nFlags <<= 1;
  r->p_channel_tv = nFlags & 0x01;
  nFlags <<= 1;
  r->p_channel_radio = nFlags & 0x01;
  nFlags <<= 1;
  r->p_channel_press = nFlags & 0x01;
  nFlags <<= 1;
  r->p_channel_event = nFlags & 0x01;
  nFlags <<= 1;
  r->p_channel_demo = nFlags & 0x01;
  nFlags <<= 1;
  r->p_discount_active = nFlags & 0x01;
  gen_text(
      &r->p_channel_details[0],
      PROMO_DETAIL_LEN_MIN,
      PROMO_DETAIL_LEN_MAX,
      P_CHANNEL_DETAILS,
      dsdGenContext);
  pick_distribution(
      &r->p_purpose, "promo_purpose", 1, 1, P_PURPOSE, dsdGenContext);

  void* info = append_info_get(info_arr, PROMOTION);
  append_row_start(info);
  append_key(P_PROMO_SK, info, r->p_promo_sk);
  append_varchar(P_PROMO_ID, info, r->p_promo_id);
  append_key(P_START_DATE_ID, info, r->p_start_date_id);
  append_key(P_END_DATE_ID, info, r->p_end_date_id);
  append_key(P_ITEM_SK, info, r->p_item_sk);
  append_decimal(P_COST, info, &r->p_cost);
  append_integer(P_RESPONSE_TARGET, info, r->p_response_target);
  append_varchar(P_PROMO_NAME, info, &r->p_promo_name[0]);
  append_varchar(P_CHANNEL_DMAIL, info, r->p_channel_dmail ? "Y" : "N");
  append_varchar(P_CHANNEL_EMAIL, info, r->p_channel_email ? "Y" : "N");
  append_varchar(P_CHANNEL_CATALOG, info, r->p_channel_catalog ? "Y" : "N");
  append_varchar(P_CHANNEL_TV, info, r->p_channel_tv ? "Y" : "N");
  append_varchar(P_CHANNEL_RADIO, info, r->p_channel_radio ? "Y" : "N");
  append_varchar(P_CHANNEL_PRESS, info, r->p_channel_press ? "Y" : "N");
  append_varchar(P_CHANNEL_EVENT, info, r->p_channel_event ? "Y" : "N");
  append_varchar(P_CHANNEL_DEMO, info, r->p_channel_demo ? "Y" : "N");
  append_varchar(P_CHANNEL_DETAILS, info, &r->p_channel_details[0]);
  append_varchar(P_PURPOSE, info, r->p_purpose);
  append_varchar(P_DISCOUNT_ACTIVE, info, r->p_discount_active ? "Y" : "N");

  append_row_end(info);

  return 0;
}
