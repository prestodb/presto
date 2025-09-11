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

#include "velox/tpcds/gen/dsdgen/include/w_web_returns.h"
#include <stdio.h>
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/error_msg.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/misc.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/pricing.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"
#include "velox/tpcds/gen/dsdgen/include/w_web_sales.h"

/*
 * Routine: mk_web_returns()
 * Purpose: populate a return fact *sync'd with a sales fact*
 * Algorithm: Since the returns need to be in line with a prior sale, they are
 *built by a call from the mk_catalog_sales() routine, and then add
 *return-related information Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int mk_w_web_returns(void* row, ds_key_t index, DSDGenContext& dsdGenContext) {
  int res = 0;

  decimal_t dMin, dMax;
  struct W_WEB_SALES_TBL* sale = &dsdGenContext.g_w_web_sales;
  struct W_WEB_RETURNS_TBL* r;
  tdef* pT = getSimpleTdefsByNumber(WEB_RETURNS, dsdGenContext);

  if (row == NULL)
    r = &dsdGenContext.g_w_web_returns;
  else
    r = static_cast<W_WEB_RETURNS_TBL*>(row);

  strtodec(&dMin, "1.00");
  strtodec(&dMax, "100000.00");

  nullSet(&pT->kNullBitMap, WR_NULLS, dsdGenContext);

  /*
   * Some of the information in the return is taken from the original sale
   * which has been regenerated
   */
  r->wr_item_sk = sale->ws_item_sk;
  r->wr_order_number = sale->ws_order_number;
  memcpy(
      static_cast<void*>(&r->wr_pricing),
      static_cast<void*>(&sale->ws_pricing),
      sizeof(ds_pricing_t));
  r->wr_web_page_sk = sale->ws_web_page_sk;

  /*
   * the rest of the columns are generated for this specific return
   */
  /* the items cannot be returned until they are shipped; offset is handled in
   * mk_join, based on sales date */
  r->wr_returned_date_sk =
      mk_join(WR_RETURNED_DATE_SK, DATET, sale->ws_ship_date_sk, dsdGenContext);
  r->wr_returned_time_sk =
      mk_join(WR_RETURNED_TIME_SK, TIMET, 1, dsdGenContext);

  /* most items are returned by the people they were shipped to, but some are
   * returned by other folks
   */
  r->wr_refunded_customer_sk =
      mk_join(WR_REFUNDED_CUSTOMER_SK, CUSTOMER, 1, dsdGenContext);
  r->wr_refunded_cdemo_sk =
      mk_join(WR_REFUNDED_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1, dsdGenContext);
  r->wr_refunded_hdemo_sk =
      mk_join(WR_REFUNDED_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1, dsdGenContext);
  r->wr_refunded_addr_sk =
      mk_join(WR_REFUNDED_ADDR_SK, CUSTOMER_ADDRESS, 1, dsdGenContext);
  if (genrand_integer(
          NULL,
          DIST_UNIFORM,
          0,
          99,
          0,
          WR_RETURNING_CUSTOMER_SK,
          dsdGenContext) < WS_GIFT_PCT) {
    r->wr_refunded_customer_sk = sale->ws_ship_customer_sk;
    r->wr_refunded_cdemo_sk = sale->ws_ship_cdemo_sk;
    r->wr_refunded_hdemo_sk = sale->ws_ship_hdemo_sk;
    r->wr_refunded_addr_sk = sale->ws_ship_addr_sk;
  }
  r->wr_returning_customer_sk = r->wr_refunded_customer_sk;
  r->wr_returning_cdemo_sk = r->wr_refunded_cdemo_sk;
  r->wr_returning_hdemo_sk = r->wr_refunded_hdemo_sk;
  r->wr_returning_addr_sk = r->wr_refunded_addr_sk;

  r->wr_reason_sk = mk_join(WR_REASON_SK, REASON, 1, dsdGenContext);
  genrand_integer(
      &r->wr_pricing.quantity,
      DIST_UNIFORM,
      1,
      sale->ws_pricing.quantity,
      0,
      WR_PRICING,
      dsdGenContext);
  set_pricing(WR_PRICING, &r->wr_pricing, dsdGenContext);

  return (res);
}
