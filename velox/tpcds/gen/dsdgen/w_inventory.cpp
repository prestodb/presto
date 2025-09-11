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

#include "velox/tpcds/gen/dsdgen/include/w_inventory.h"

#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/scd.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"

#include <stdio.h>

/*
 * Routine:
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
int mk_w_inventory(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  struct W_INVENTORY_TBL* r;
  ds_key_t item_count;
  ds_key_t warehouse_count;
  int jDate;
  date_t base_date_storage;
  date_t* base_date = &base_date_storage;
  int nTemp;
  tdef* pTdef = getSimpleTdefsByNumber(INVENTORY, dsdGenContext);

  r = &dsdGenContext.g_w_inventory;

  if (!dsdGenContext.mk_w_promotion_init) {
    memset(&dsdGenContext.g_w_inventory, 0, sizeof(struct W_INVENTORY_TBL));
    dsdGenContext.mk_w_promotion_init = 1;
  }
  item_count = getIDCount(ITEM, dsdGenContext);
  warehouse_count = get_rowcount(WAREHOUSE, dsdGenContext);
  strtodt(base_date, DATE_MINIMUM);
  jDate = base_date->julian;
  set_dow(base_date);
  /* Make exceptions to the 1-rng-call-per-row rule */

  nullSet(&pTdef->kNullBitMap, INV_NULLS, dsdGenContext);
  nTemp = static_cast<long>(index - 1);
  r->inv_item_sk = (nTemp % item_count) + 1;
  nTemp /= static_cast<long>(item_count);
  r->inv_warehouse_sk = (nTemp % warehouse_count) + 1;
  nTemp /= static_cast<long>(warehouse_count);
  r->inv_date_sk = jDate + (nTemp * 7); /* inventory is updated weekly */

  /*
   * the join between item and inventory is tricky. The item_id selected above
   * identifies a unique part num but item is an SCD, so we need to account
   * for that in selecting the SK to join with
   */
  r->inv_item_sk =
      matchSCDSK(r->inv_item_sk, r->inv_date_sk, ITEM, dsdGenContext);

  genrand_integer(
      &r->inv_quantity_on_hand,
      DIST_UNIFORM,
      INV_QUANTITY_MIN,
      INV_QUANTITY_MAX,
      0,
      INV_QUANTITY_ON_HAND,
      dsdGenContext);

  void* info = append_info_get(info_arr, INVENTORY);
  append_row_start(info);
  append_key(INV_DATE_SK, info, r->inv_date_sk);
  append_key(INV_ITEM_SK, info, r->inv_item_sk);
  append_key(INV_WAREHOUSE_SK, info, r->inv_warehouse_sk);
  append_integer(INV_QUANTITY_ON_HAND, info, r->inv_quantity_on_hand);
  append_row_end(info);

  return 0;
}

/*
 * Routine:
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
ds_key_t sc_w_inventory(int nScale, DSDGenContext& dsdGenContext) {
  ds_key_t kRes;
  date_t dTemp;
  int nDays;

  kRes = getIDCount(ITEM, dsdGenContext);
  kRes *= get_rowcount(WAREHOUSE, dsdGenContext);
  strtodt(&dTemp, DATE_MAXIMUM);
  nDays = dTemp.julian;
  strtodt(&dTemp, DATE_MINIMUM);
  nDays -= dTemp.julian;
  nDays += 1;
  nDays += 6;
  nDays /= 7; /* each items inventory is updated weekly */
  kRes *= nDays;

  return (kRes);
}
