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

#include "velox/tpcds/gen/dsdgen/include/w_item.h"

#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/misc.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/scd.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

#include <stdio.h>

/* extern tdef w_tdefs[]; */

static struct W_ITEM_TBL g_OldValues;

/*
 * mk_item
 */
int mk_w_item(void* info_arr, ds_key_t index, DSDGenContext& dsdGenContext) {
  /* begin locals declarations */
  decimal_t dMinPrice, dMaxPrice, dMarkdown;
  decimal_t dMinMarkdown, dMaxMarkdown;
  int32_t bUseSize, bFirstRecord = 0, nFieldChangeFlags, nMin, nMax, nIndex,
                    nTemp;
  char* cp = nullptr;
  struct W_ITEM_TBL* r;
  struct W_ITEM_TBL* rOldValues = &g_OldValues;
  char *szMinPrice = nullptr, *szMaxPrice = nullptr;
  tdef* pT = getSimpleTdefsByNumber(ITEM, dsdGenContext);

  r = &dsdGenContext.g_w_item;

  strtodec(&dMinMarkdown, MIN_ITEM_MARKDOWN_PCT);
  strtodec(&dMaxMarkdown, MAX_ITEM_MARKDOWN_PCT);
  memset(r, 0, sizeof(struct W_ITEM_TBL));

  /* build the new value */
  nullSet(&pT->kNullBitMap, I_NULLS, dsdGenContext);
  r->i_item_sk = index;

  nIndex = pick_distribution(
      &nMin, "i_manager_id", 2, 1, I_MANAGER_ID, dsdGenContext);
  dist_member(&nMax, "i_manager_id", nIndex, 3, dsdGenContext);
  genrand_key(
      &r->i_manager_id,
      DIST_UNIFORM,
      static_cast<ds_key_t>(nMin),
      static_cast<ds_key_t>(nMax),
      0,
      I_MANAGER_ID,
      dsdGenContext);

  /* if we have generated the required history for this business key and
   * generate a new one then reset associated fields (e.g., rec_start_date
   * minimums)
   */
  if (setSCDKeys(
          I_ITEM_ID,
          index,
          r->i_item_id,
          &r->i_rec_start_date_id,
          &r->i_rec_end_date_id,
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
  nFieldChangeFlags = next_random(I_SCD, dsdGenContext);

  /* the rest of the record in a history-keeping dimension can either be a new
   * data value or not; use a random number and its bit pattern to determine
   * which fields to replace and which to retain
   */
  gen_text(r->i_item_desc, 1, RS_I_ITEM_DESC, I_ITEM_DESC, dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->i_item_desc,
      &rOldValues->i_item_desc,
      &nFieldChangeFlags,
      bFirstRecord);

  nIndex = pick_distribution(
      &szMinPrice, "i_current_price", 2, 1, I_CURRENT_PRICE, dsdGenContext);
  dist_member(&szMaxPrice, "i_current_price", nIndex, 3, dsdGenContext);
  strtodec(&dMinPrice, szMinPrice);
  strtodec(&dMaxPrice, szMaxPrice);
  genrand_decimal(
      &r->i_current_price,
      DIST_UNIFORM,
      &dMinPrice,
      &dMaxPrice,
      NULL,
      I_CURRENT_PRICE,
      dsdGenContext);
  changeSCD(
      SCD_INT,
      &r->i_current_price,
      &rOldValues->i_current_price,
      &nFieldChangeFlags,
      bFirstRecord);

  genrand_decimal(
      &dMarkdown,
      DIST_UNIFORM,
      &dMinMarkdown,
      &dMaxMarkdown,
      NULL,
      I_WHOLESALE_COST,
      dsdGenContext);
  decimal_t_op(&r->i_wholesale_cost, OP_MULT, &r->i_current_price, &dMarkdown);
  changeSCD(
      SCD_DEC,
      &r->i_wholesale_cost,
      &rOldValues->i_wholesale_cost,
      &nFieldChangeFlags,
      bFirstRecord);

  hierarchy_item(
      I_CATEGORY, &r->i_category_id, &r->i_category, index, dsdGenContext);
  /*
   * changeSCD(SCD_INT, &r->i_category_id, &rOldValues->i_category_id,
   * &nFieldChangeFlags,  bFirstRecord);
   */

  hierarchy_item(I_CLASS, &r->i_class_id, &r->i_class, index, dsdGenContext);
  changeSCD(
      SCD_KEY,
      &r->i_class_id,
      &rOldValues->i_class_id,
      &nFieldChangeFlags,
      bFirstRecord);

  cp = &r->i_brand[0];
  hierarchy_item(I_BRAND, &r->i_brand_id, &cp, index, dsdGenContext);
  changeSCD(
      SCD_KEY,
      &r->i_brand_id,
      &rOldValues->i_brand_id,
      &nFieldChangeFlags,
      bFirstRecord);

  /* some categories have meaningful sizes, some don't */
  if (r->i_category_id) {
    dist_member(
        &bUseSize,
        "categories",
        static_cast<int>(r->i_category_id),
        3,
        dsdGenContext);
    pick_distribution(
        &r->i_size, "sizes", 1, bUseSize + 2, I_SIZE, dsdGenContext);
    changeSCD(
        SCD_PTR,
        &r->i_size,
        &rOldValues->i_size,
        &nFieldChangeFlags,
        bFirstRecord);
  } else {
    bUseSize = 0;
    r->i_size = NULL;
  }

  nIndex = pick_distribution(
      &nMin, "i_manufact_id", 2, 1, I_MANUFACT_ID, dsdGenContext);
  genrand_integer(
      &nTemp,
      DIST_UNIFORM,
      nMin,
      dist_member(NULL, "i_manufact_id", nIndex, 3, dsdGenContext),
      0,
      I_MANUFACT_ID,
      dsdGenContext);
  r->i_manufact_id = nTemp;
  changeSCD(
      SCD_KEY,
      &r->i_manufact_id,
      &rOldValues->i_manufact_id,
      &nFieldChangeFlags,
      bFirstRecord);

  mk_word(
      r->i_manufact,
      "syllables",
      static_cast<int>(r->i_manufact_id),
      RS_I_MANUFACT,
      ITEM,
      dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->i_manufact,
      &rOldValues->i_manufact,
      &nFieldChangeFlags,
      bFirstRecord);

  gen_charset(
      r->i_formulation,
      DIGITS,
      RS_I_FORMULATION,
      RS_I_FORMULATION,
      I_FORMULATION,
      dsdGenContext);
  embed_string(r->i_formulation, "colors", 1, 2, I_FORMULATION, dsdGenContext);
  changeSCD(
      SCD_CHAR,
      &r->i_formulation,
      &rOldValues->i_formulation,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(&r->i_color, "colors", 1, 2, I_COLOR, dsdGenContext);
  changeSCD(
      SCD_PTR,
      &r->i_color,
      &rOldValues->i_color,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(&r->i_units, "units", 1, 1, I_UNITS, dsdGenContext);
  changeSCD(
      SCD_PTR,
      &r->i_units,
      &rOldValues->i_units,
      &nFieldChangeFlags,
      bFirstRecord);

  pick_distribution(&r->i_container, "container", 1, 1, ITEM, dsdGenContext);
  changeSCD(
      SCD_PTR,
      &r->i_container,
      &rOldValues->i_container,
      &nFieldChangeFlags,
      bFirstRecord);

  mk_word(
      r->i_product_name,
      "syllables",
      static_cast<int>(index),
      RS_I_PRODUCT_NAME,
      ITEM,
      dsdGenContext);

  r->i_promo_sk = mk_join(I_PROMO_SK, PROMOTION, 1, dsdGenContext);
  genrand_integer(&nTemp, DIST_UNIFORM, 1, 100, 0, I_PROMO_SK, dsdGenContext);
  if (nTemp > I_PROMO_PERCENTAGE)
    r->i_promo_sk = -1;

  /*
   * if this is the first of a set of revisions, then baseline the old values
   */
  if (bFirstRecord)
    memcpy(&g_OldValues, r, sizeof(struct W_ITEM_TBL));

  if (index == 1)
    memcpy(&g_OldValues, r, sizeof(struct W_ITEM_TBL));

  void* info = append_info_get(info_arr, ITEM);
  append_row_start(info);

  append_key(I_ITEM_SK, info, r->i_item_sk);
  append_varchar(I_ITEM_ID, info, r->i_item_id);
  append_date(I_REC_START_DATE_ID, info, r->i_rec_start_date_id);
  append_date(I_REC_END_DATE_ID, info, r->i_rec_end_date_id);
  append_varchar(I_ITEM_DESC, info, r->i_item_desc);
  append_decimal(I_CURRENT_PRICE, info, &r->i_current_price);
  append_decimal(I_WHOLESALE_COST, info, &r->i_wholesale_cost);
  append_integer(I_BRAND_ID, info, r->i_brand_id);
  append_varchar(I_BRAND, info, r->i_brand);
  append_integer(I_CLASS_ID, info, r->i_class_id);
  append_varchar(I_CLASS, info, r->i_class);
  append_integer(I_CATEGORY_ID, info, r->i_category_id);
  append_varchar(I_CATEGORY, info, r->i_category);
  append_integer(I_MANUFACT_ID, info, r->i_manufact_id);
  append_varchar(I_MANUFACT, info, r->i_manufact);
  append_varchar(I_SIZE, info, r->i_size);
  append_varchar(I_FORMULATION, info, r->i_formulation);
  append_varchar(I_COLOR, info, r->i_color);
  append_varchar(I_UNITS, info, r->i_units);
  append_varchar(I_CONTAINER, info, r->i_container);
  append_integer(I_MANAGER_ID, info, r->i_manager_id);
  append_varchar(I_PRODUCT_NAME, info, r->i_product_name);

  append_row_end(info);

  return 0;
}
