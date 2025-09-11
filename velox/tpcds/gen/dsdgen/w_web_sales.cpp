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

#include "velox/tpcds/gen/dsdgen/include/w_web_sales.h"

#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/misc.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/parallel.h"
#include "velox/tpcds/gen/dsdgen/include/permute.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/pricing.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/scd.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"
#include "velox/tpcds/gen/dsdgen/include/w_web_returns.h"

#include <stdio.h>

// ds_key_t
// skipDays(int nTable, ds_key_t* pRemainder, DSDGenContext& dsdGenContext);

/*
 * the validation process requires generating a single lineitem
 * so the main mk_xxx routine has been split into a master record portion
 * and a detail/lineitem portion.
 */
W_WEB_SALES_TBL* mk_web_sales_master(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  decimal_t dMin, dMax;
  int nGiftPct;
  struct W_WEB_SALES_TBL* r;
  r = &dsdGenContext.g_w_web_sales;

  if (!dsdGenContext.mk_master_web_sales_init) {
    dsdGenContext.pWebSalesItemPermutation = makePermutation(
        dsdGenContext.nWebSalesItemCount =
            static_cast<int>(getIDCount(ITEM, dsdGenContext)),
        WS_PERMUTATION,
        dsdGenContext);
    dsdGenContext.mk_master_web_sales_init = 1;
  }
  strtodec(&dMin, "1.00");
  strtodec(&dMax, "100000.00");

  /***
   * some attributes reamin the same for each lineitem in an order; others are
   * different for each lineitem. Since the number of lineitems per order is
   * static, we can use a modulo to determine when to change the semi-static
   * values
   */

  r->ws_sold_date_sk = mk_join(WS_SOLD_DATE_SK, DATET, 1, dsdGenContext);
  r->ws_sold_time_sk = mk_join(WS_SOLD_TIME_SK, TIMET, 1, dsdGenContext);
  r->ws_bill_customer_sk =
      mk_join(WS_BILL_CUSTOMER_SK, CUSTOMER, 1, dsdGenContext);
  r->ws_bill_cdemo_sk =
      mk_join(WS_BILL_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1, dsdGenContext);
  r->ws_bill_hdemo_sk =
      mk_join(WS_BILL_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1, dsdGenContext);
  r->ws_bill_addr_sk =
      mk_join(WS_BILL_ADDR_SK, CUSTOMER_ADDRESS, 1, dsdGenContext);

  /* most orders are for the ordering customers, some are not */
  genrand_integer(
      &nGiftPct, DIST_UNIFORM, 0, 99, 0, WS_SHIP_CUSTOMER_SK, dsdGenContext);
  if (nGiftPct > WS_GIFT_PCT) {
    r->ws_ship_customer_sk =
        mk_join(WS_SHIP_CUSTOMER_SK, CUSTOMER, 2, dsdGenContext);
    r->ws_ship_cdemo_sk =
        mk_join(WS_SHIP_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2, dsdGenContext);
    r->ws_ship_hdemo_sk =
        mk_join(WS_SHIP_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2, dsdGenContext);
    r->ws_ship_addr_sk =
        mk_join(WS_SHIP_ADDR_SK, CUSTOMER_ADDRESS, 2, dsdGenContext);
  } else {
    r->ws_ship_customer_sk = r->ws_bill_customer_sk;
    r->ws_ship_cdemo_sk = r->ws_bill_cdemo_sk;
    r->ws_ship_hdemo_sk = r->ws_bill_hdemo_sk;
    r->ws_ship_addr_sk = r->ws_bill_addr_sk;
  }

  r->ws_order_number = index;
  genrand_integer(
      &dsdGenContext.nWebSalesItemIndex,
      DIST_UNIFORM,
      1,
      dsdGenContext.nWebSalesItemCount,
      0,
      WS_ITEM_SK,
      dsdGenContext);

  return r;
}

void mk_detail(
    void* info_arr,
    int bPrint,
    struct W_WEB_SALES_TBL* r,
    DSDGenContext& dsdGenContext) {
  int nShipLag, nTemp;
  tdef* pT = getSimpleTdefsByNumber(WEB_SALES, dsdGenContext);
  // r = &dsdGenContext.g_w_web_sales;

  nullSet(&pT->kNullBitMap, WS_NULLS, dsdGenContext);

  /* orders are shipped some number of days after they are ordered,
   * and not all lineitems ship at the same time
   */
  genrand_integer(
      &nShipLag,
      DIST_UNIFORM,
      WS_MIN_SHIP_DELAY,
      WS_MAX_SHIP_DELAY,
      0,
      WS_SHIP_DATE_SK,
      dsdGenContext);
  r->ws_ship_date_sk = r->ws_sold_date_sk + nShipLag;

  if (++dsdGenContext.nWebSalesItemIndex > dsdGenContext.nWebSalesItemCount)
    dsdGenContext.nWebSalesItemIndex = 1;
  r->ws_item_sk = matchSCDSK(
      getPermutationEntry(
          dsdGenContext.pWebSalesItemPermutation,
          dsdGenContext.nWebSalesItemIndex),
      r->ws_sold_date_sk,
      ITEM,
      dsdGenContext);

  /* the web page needs to be valid for the sale date */
  r->ws_web_page_sk =
      mk_join(WS_WEB_PAGE_SK, WEB_PAGE, r->ws_sold_date_sk, dsdGenContext);
  r->ws_web_site_sk =
      mk_join(WS_WEB_SITE_SK, WEB_SITE, r->ws_sold_date_sk, dsdGenContext);

  r->ws_ship_mode_sk = mk_join(WS_SHIP_MODE_SK, SHIP_MODE, 1, dsdGenContext);
  r->ws_warehouse_sk = mk_join(WS_WAREHOUSE_SK, WAREHOUSE, 1, dsdGenContext);
  r->ws_promo_sk = mk_join(WS_PROMO_SK, PROMOTION, 1, dsdGenContext);
  set_pricing(WS_PRICING, &r->ws_pricing, dsdGenContext);

  /**
   * having gone to the trouble to make the sale, now let's see if it gets
   * returned
   */
  genrand_integer(
      &nTemp, DIST_UNIFORM, 0, 99, 0, WR_IS_RETURNED, dsdGenContext);
  if (nTemp < WR_RETURN_PCT) {
    struct W_WEB_RETURNS_TBL w_web_returns;
    struct W_WEB_RETURNS_TBL* rr = &dsdGenContext.g_w_web_returns;
    mk_w_web_returns(rr, 1, dsdGenContext);

    void* info = append_info_get(info_arr, WEB_RETURNS);
    append_row_start(info);

    append_key(WR_RETURNED_DATE_SK, info, rr->wr_returned_date_sk);
    append_key(WR_RETURNED_TIME_SK, info, rr->wr_returned_time_sk);
    append_key(WR_ITEM_SK, info, rr->wr_item_sk);
    append_key(WR_REFUNDED_CUSTOMER_SK, info, rr->wr_refunded_customer_sk);
    append_key(WR_REFUNDED_CDEMO_SK, info, rr->wr_refunded_cdemo_sk);
    append_key(WR_REFUNDED_HDEMO_SK, info, rr->wr_refunded_hdemo_sk);
    append_key(WR_REFUNDED_ADDR_SK, info, rr->wr_refunded_addr_sk);
    append_key(WR_RETURNING_CUSTOMER_SK, info, rr->wr_returning_customer_sk);
    append_key(WR_RETURNING_CDEMO_SK, info, rr->wr_returning_cdemo_sk);
    append_key(WR_RETURNING_HDEMO_SK, info, rr->wr_returning_hdemo_sk);
    append_key(WR_RETURNING_ADDR_SK, info, rr->wr_returning_addr_sk);
    append_key(WR_WEB_PAGE_SK, info, rr->wr_web_page_sk);
    append_key(WR_REASON_SK, info, rr->wr_reason_sk);
    append_key(WR_ORDER_NUMBER, info, rr->wr_order_number);
    append_integer(WR_PRICING_QUANTITY, info, rr->wr_pricing.quantity);
    append_decimal(WR_PRICING_NET_PAID, info, &rr->wr_pricing.net_paid);
    append_decimal(WR_PRICING_EXT_TAX, info, &rr->wr_pricing.ext_tax);
    append_decimal(
        WR_PRICING_NET_PAID_INC_TAX, info, &rr->wr_pricing.net_paid_inc_tax);
    append_decimal(WR_PRICING_FEE, info, &rr->wr_pricing.fee);
    append_decimal(
        WR_PRICING_EXT_SHIP_COST, info, &rr->wr_pricing.ext_ship_cost);
    append_decimal(
        WR_PRICING_REFUNDED_CASH, info, &rr->wr_pricing.refunded_cash);
    append_decimal(
        WR_PRICING_REVERSED_CHARGE, info, &rr->wr_pricing.reversed_charge);
    append_decimal(WR_PRICING_STORE_CREDIT, info, &rr->wr_pricing.store_credit);
    append_decimal(WR_PRICING_NET_LOSS, info, &rr->wr_pricing.net_loss);

    append_row_end(info);
  }

  void* info = append_info_get(info_arr, WEB_SALES);
  append_row_start(info);

  append_key(WS_SOLD_DATE_SK, info, r->ws_sold_date_sk);
  append_key(WS_SOLD_TIME_SK, info, r->ws_sold_time_sk);
  append_key(WS_SHIP_DATE_SK, info, r->ws_ship_date_sk);
  append_key(WS_ITEM_SK, info, r->ws_item_sk);
  append_key(WS_BILL_CUSTOMER_SK, info, r->ws_bill_customer_sk);
  append_key(WS_BILL_CDEMO_SK, info, r->ws_bill_cdemo_sk);
  append_key(WS_BILL_HDEMO_SK, info, r->ws_bill_hdemo_sk);
  append_key(WS_BILL_ADDR_SK, info, r->ws_bill_addr_sk);
  append_key(WS_SHIP_CUSTOMER_SK, info, r->ws_ship_customer_sk);
  append_key(WS_SHIP_CDEMO_SK, info, r->ws_ship_cdemo_sk);
  append_key(WS_SHIP_HDEMO_SK, info, r->ws_ship_hdemo_sk);
  append_key(WS_SHIP_ADDR_SK, info, r->ws_ship_addr_sk);
  append_key(WS_WEB_PAGE_SK, info, r->ws_web_page_sk);
  append_key(WS_WEB_SITE_SK, info, r->ws_web_site_sk);
  append_key(WS_SHIP_MODE_SK, info, r->ws_ship_mode_sk);
  append_key(WS_WAREHOUSE_SK, info, r->ws_warehouse_sk);
  append_key(WS_PROMO_SK, info, r->ws_promo_sk);
  append_key(WS_ORDER_NUMBER, info, r->ws_order_number);
  append_integer(WS_PRICING_QUANTITY, info, r->ws_pricing.quantity);
  append_decimal(
      WS_PRICING_WHOLESALE_COST, info, &r->ws_pricing.wholesale_cost);
  append_decimal(WS_PRICING_LIST_PRICE, info, &r->ws_pricing.list_price);
  append_decimal(WS_PRICING_SALES_PRICE, info, &r->ws_pricing.sales_price);
  append_decimal(
      WS_PRICING_EXT_DISCOUNT_AMT, info, &r->ws_pricing.ext_discount_amt);
  append_decimal(
      WS_PRICING_EXT_SALES_PRICE, info, &r->ws_pricing.ext_sales_price);
  append_decimal(
      WS_PRICING_EXT_WHOLESALE_COST, info, &r->ws_pricing.ext_wholesale_cost);
  append_decimal(
      WS_PRICING_EXT_LIST_PRICE, info, &r->ws_pricing.ext_list_price);
  append_decimal(WS_PRICING_EXT_TAX, info, &r->ws_pricing.ext_tax);
  append_decimal(WS_PRICING_COUPON_AMT, info, &r->ws_pricing.coupon_amt);
  append_decimal(WS_PRICING_EXT_SHIP_COST, info, &r->ws_pricing.ext_ship_cost);
  append_decimal(WS_PRICING_NET_PAID, info, &r->ws_pricing.net_paid);
  append_decimal(
      WS_PRICING_NET_PAID_INC_TAX, info, &r->ws_pricing.net_paid_inc_tax);
  append_decimal(
      WS_PRICING_NET_PAID_INC_SHIP, info, &r->ws_pricing.net_paid_inc_ship);
  append_decimal(
      WS_PRICING_NET_PAID_INC_SHIP_TAX,
      info,
      &r->ws_pricing.net_paid_inc_ship_tax);
  append_decimal(WS_PRICING_NET_PROFIT, info, &r->ws_pricing.net_profit);

  append_row_end(info);

  return;
}

/*
 * mk_web_sales
 */
int mk_w_web_sales(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  int nLineitems, i;

  row_skip(WEB_RETURNS, (index - 1), dsdGenContext);
  /* build the static portion of an order */
  struct W_WEB_SALES_TBL* r;
  r = mk_web_sales_master(info_arr, index, dsdGenContext);

  /* set the number of lineitems and build them */
  genrand_integer(
      &nLineitems, DIST_UNIFORM, 8, 16, 9, WS_ORDER_NUMBER, dsdGenContext);
  for (i = 1; i <= nLineitems; i++) {
    mk_detail(info_arr, 1, r, dsdGenContext);
  }

  /**
   * and finally return 1 since we have already printed the rows
   */
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
int vld_web_sales(
    int nTable,
    ds_key_t kRow,
    int* Permutation,
    DSDGenContext& dsdGenContext) {
  int nLineitem, nMaxLineitem, i;

  row_skip(nTable, kRow - 1, dsdGenContext);
  row_skip(WEB_RETURNS, (kRow - 1), dsdGenContext);
  struct W_WEB_SALES_TBL* r;
  r = mk_web_sales_master(NULL, kRow, dsdGenContext);
  genrand_integer(
      &nMaxLineitem, DIST_UNIFORM, 8, 16, 9, WS_ORDER_NUMBER, dsdGenContext);
  genrand_integer(
      &nLineitem,
      DIST_UNIFORM,
      1,
      nMaxLineitem,
      0,
      WS_PRICING_QUANTITY,
      dsdGenContext);
  for (i = 1; i < nLineitem; i++) {
    mk_detail(NULL, 0, r, dsdGenContext);
  }
  mk_detail(NULL, 1, r, dsdGenContext);

  return (0);
}
