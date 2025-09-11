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

#include "velox/tpcds/gen/dsdgen/include/w_catalog_sales.h"

#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/build_support.h"
#include "velox/tpcds/gen/dsdgen/include/columns.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/constants.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/decimal.h"
#include "velox/tpcds/gen/dsdgen/include/genrand.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/parallel.h"
#include "velox/tpcds/gen/dsdgen/include/params.h"
#include "velox/tpcds/gen/dsdgen/include/permute.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/scaling.h"
#include "velox/tpcds/gen/dsdgen/include/scd.h"
#include "velox/tpcds/gen/dsdgen/include/tables.h"
#include "velox/tpcds/gen/dsdgen/include/tdefs.h"
#include "velox/tpcds/gen/dsdgen/include/w_catalog_returns.h"

#include <stdio.h>

ds_key_t
skipDays(int nTable, ds_key_t* pRemainder, DSDGenContext& dsdGenContext);

/*
 * the validation process requires generating a single lineitem
 * so the main mk_xxx routine has been split into a master record portion
 * and a detail/lineitem portion.
 */
W_CATALOG_SALES_TBL* mk_catalog_sales_master(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  decimal_t dZero, dHundred, dOne, dOneHalf;
  int nGiftPct;
  struct W_CATALOG_SALES_TBL* r;
  r = &dsdGenContext.g_w_catalog_sales;

  if (!dsdGenContext.mk_master_catalog_sales_init) {
    dsdGenContext.jDate =
        skipDays(CATALOG_SALES, &dsdGenContext.kNewDateIndex, dsdGenContext);
    dsdGenContext.pCatalogSalesItemPermutation = makePermutation(
        (dsdGenContext.nCatalogSalesItemCount =
             static_cast<int>(getIDCount(ITEM, dsdGenContext))),
        CS_PERMUTE,
        dsdGenContext);

    dsdGenContext.mk_master_catalog_sales_init = 1;
  }

  strtodec(&dZero, "0.00");
  strtodec(&dHundred, "100.00");
  strtodec(&dOne, "1.00");
  strtodec(&dOneHalf, "0.50");

  while (index > dsdGenContext.kNewDateIndex) /* need to move to a new date */
  {
    dsdGenContext.jDate += 1;
    dsdGenContext.kNewDateIndex +=
        dateScaling(CATALOG_SALES, dsdGenContext.jDate, dsdGenContext);
  }

  /***
   * some attributes remain the same for each lineitem in an order; others are
   * different for each lineitem.
   *
   * Parallel generation causes another problem, since the values that get
   * seeded may come from a prior row. If we are seeding at the start of a
   * parallel chunk, hunt backwards in the RNG stream to find the most recent
   * values that were used to set the values of the orderline-invariant
   * columns
   */

  r->cs_sold_date_sk = dsdGenContext.jDate;
  r->cs_sold_time_sk =
      mk_join(CS_SOLD_TIME_SK, TIMET, r->cs_call_center_sk, dsdGenContext);
  r->cs_call_center_sk = (r->cs_sold_date_sk == -1)
      ? -1
      : mk_join(
            CS_CALL_CENTER_SK, CALL_CENTER, r->cs_sold_date_sk, dsdGenContext);

  r->cs_bill_customer_sk =
      mk_join(CS_BILL_CUSTOMER_SK, CUSTOMER, 1, dsdGenContext);
  r->cs_bill_cdemo_sk =
      mk_join(CS_BILL_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1, dsdGenContext);
  r->cs_bill_hdemo_sk =
      mk_join(CS_BILL_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1, dsdGenContext);
  r->cs_bill_addr_sk =
      mk_join(CS_BILL_ADDR_SK, CUSTOMER_ADDRESS, 1, dsdGenContext);

  /* most orders are for the ordering customers, some are not */
  genrand_integer(
      &nGiftPct, DIST_UNIFORM, 0, 99, 0, CS_SHIP_CUSTOMER_SK, dsdGenContext);
  if (nGiftPct <= CS_GIFT_PCT) {
    r->cs_ship_customer_sk =
        mk_join(CS_SHIP_CUSTOMER_SK, CUSTOMER, 2, dsdGenContext);
    r->cs_ship_cdemo_sk =
        mk_join(CS_SHIP_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 2, dsdGenContext);
    r->cs_ship_hdemo_sk =
        mk_join(CS_SHIP_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 2, dsdGenContext);
    r->cs_ship_addr_sk =
        mk_join(CS_SHIP_ADDR_SK, CUSTOMER_ADDRESS, 2, dsdGenContext);
  } else {
    r->cs_ship_customer_sk = r->cs_bill_customer_sk;
    r->cs_ship_cdemo_sk = r->cs_bill_cdemo_sk;
    r->cs_ship_hdemo_sk = r->cs_bill_hdemo_sk;
    r->cs_ship_addr_sk = r->cs_bill_addr_sk;
  }

  r->cs_order_number = index;
  genrand_integer(
      &dsdGenContext.nTicketItemBase,
      DIST_UNIFORM,
      1,
      dsdGenContext.nCatalogSalesItemCount,
      0,
      CS_SOLD_ITEM_SK,
      dsdGenContext);

  return r;
}

static void mk_detail(
    void* info_arr,
    int bPrint,
    struct W_CATALOG_SALES_TBL* r,
    DSDGenContext& dsdGenContext) {
  int nShipLag, nTemp;
  ds_key_t kItem;
  tdef* pTdef = getSimpleTdefsByNumber(CATALOG_SALES, dsdGenContext);

  r = &dsdGenContext.g_w_catalog_sales;

  nullSet(&pTdef->kNullBitMap, CS_NULLS, dsdGenContext);

  /* orders are shipped some number of days after they are ordered */
  genrand_integer(
      &nShipLag,
      DIST_UNIFORM,
      CS_MIN_SHIP_DELAY,
      CS_MAX_SHIP_DELAY,
      0,
      CS_SHIP_DATE_SK,
      dsdGenContext);
  r->cs_ship_date_sk =
      (r->cs_sold_date_sk == -1) ? -1 : r->cs_sold_date_sk + nShipLag;

  /*
   * items need to be unique within an order
   * use a sequence within the permutation
   * NB: Permutations are 1-based
   */
  if (++dsdGenContext.nTicketItemBase > dsdGenContext.nCatalogSalesItemCount)
    dsdGenContext.nTicketItemBase = 1;
  kItem = getPermutationEntry(
      dsdGenContext.pCatalogSalesItemPermutation,
      dsdGenContext.nTicketItemBase);
  r->cs_sold_item_sk =
      matchSCDSK(kItem, r->cs_sold_date_sk, ITEM, dsdGenContext);

  /* catalog page needs to be from a catlog active at the time of the sale */
  r->cs_catalog_page_sk = (r->cs_sold_date_sk == -1) ? -1
                                                     : mk_join(
                                                           CS_CATALOG_PAGE_SK,
                                                           CATALOG_PAGE,
                                                           r->cs_sold_date_sk,
                                                           dsdGenContext);

  r->cs_ship_mode_sk = mk_join(CS_SHIP_MODE_SK, SHIP_MODE, 1, dsdGenContext);
  r->cs_warehouse_sk = mk_join(CS_WAREHOUSE_SK, WAREHOUSE, 1, dsdGenContext);
  r->cs_promo_sk = mk_join(CS_PROMO_SK, PROMOTION, 1, dsdGenContext);
  set_pricing(CS_PRICING, &r->cs_pricing, dsdGenContext);

  /**
   * having gone to the trouble to make the sale, now let's see if it gets
   * returned
   */
  genrand_integer(
      &nTemp, DIST_UNIFORM, 0, 99, 0, CR_IS_RETURNED, dsdGenContext);
  if (nTemp < CR_RETURN_PCT) {
    struct W_CATALOG_RETURNS_TBL w_catalog_returns;
    struct W_CATALOG_RETURNS_TBL* rr = &dsdGenContext.g_w_catalog_returns;
    mk_w_catalog_returns(rr, 1, dsdGenContext);

    void* info = append_info_get(info_arr, CATALOG_RETURNS);
    append_row_start(info);

    append_key(CR_RETURNED_DATE_SK, info, rr->cr_returned_date_sk);
    append_key(CR_RETURNED_TIME_SK, info, rr->cr_returned_time_sk);
    append_key(CR_ITEM_SK, info, rr->cr_item_sk);
    append_key(CR_REFUNDED_CUSTOMER_SK, info, rr->cr_refunded_customer_sk);
    append_key(CR_REFUNDED_CDEMO_SK, info, rr->cr_refunded_cdemo_sk);
    append_key(CR_REFUNDED_HDEMO_SK, info, rr->cr_refunded_hdemo_sk);
    append_key(CR_REFUNDED_ADDR_SK, info, rr->cr_refunded_addr_sk);
    append_key(CR_RETURNING_CUSTOMER_SK, info, rr->cr_returning_customer_sk);
    append_key(CR_RETURNING_CDEMO_SK, info, rr->cr_returning_cdemo_sk);
    append_key(CR_RETURNING_HDEMO_SK, info, rr->cr_returning_hdemo_sk);
    append_key(CR_RETURNING_ADDR_SK, info, rr->cr_returning_addr_sk);
    append_key(CR_CALL_CENTER_SK, info, rr->cr_call_center_sk);
    append_key(CR_CATALOG_PAGE_SK, info, rr->cr_catalog_page_sk);
    append_key(CR_SHIP_MODE_SK, info, rr->cr_ship_mode_sk);
    append_key(CR_WAREHOUSE_SK, info, rr->cr_warehouse_sk);
    append_key(CR_REASON_SK, info, rr->cr_reason_sk);
    append_key(CR_ORDER_NUMBER, info, rr->cr_order_number);
    append_integer(CR_PRICING_QUANTITY, info, rr->cr_pricing.quantity);
    append_decimal(CR_PRICING_NET_PAID, info, &rr->cr_pricing.net_paid);
    append_decimal(CR_PRICING_EXT_TAX, info, &rr->cr_pricing.ext_tax);
    append_decimal(
        CR_PRICING_NET_PAID_INC_TAX, info, &rr->cr_pricing.net_paid_inc_tax);
    append_decimal(CR_PRICING_FEE, info, &rr->cr_pricing.fee);
    append_decimal(
        CR_PRICING_EXT_SHIP_COST, info, &rr->cr_pricing.ext_ship_cost);
    append_decimal(
        CR_PRICING_REFUNDED_CASH, info, &rr->cr_pricing.refunded_cash);
    append_decimal(
        CR_PRICING_REVERSED_CHARGE, info, &rr->cr_pricing.reversed_charge);
    append_decimal(CR_PRICING_STORE_CREDIT, info, &rr->cr_pricing.store_credit);
    append_decimal(CR_PRICING_NET_LOSS, info, &rr->cr_pricing.net_loss);

    append_row_end(info);
  }

  void* info = append_info_get(info_arr, CATALOG_SALES);
  append_row_start(info);

  append_key(CS_SOLD_DATE_SK, info, r->cs_sold_date_sk);
  append_key(CS_SOLD_TIME_SK, info, r->cs_sold_time_sk);
  append_key(CS_SHIP_DATE_SK, info, r->cs_ship_date_sk);
  append_key(CS_BILL_CUSTOMER_SK, info, r->cs_bill_customer_sk);
  append_key(CS_BILL_CDEMO_SK, info, r->cs_bill_cdemo_sk);
  append_key(CS_BILL_HDEMO_SK, info, r->cs_bill_hdemo_sk);
  append_key(CS_BILL_ADDR_SK, info, r->cs_bill_addr_sk);
  append_key(CS_SHIP_CUSTOMER_SK, info, r->cs_ship_customer_sk);
  append_key(CS_SHIP_CDEMO_SK, info, r->cs_ship_cdemo_sk);
  append_key(CS_SHIP_HDEMO_SK, info, r->cs_ship_hdemo_sk);
  append_key(CS_SHIP_ADDR_SK, info, r->cs_ship_addr_sk);
  append_key(CS_CALL_CENTER_SK, info, r->cs_call_center_sk);
  append_key(CS_CATALOG_PAGE_SK, info, r->cs_catalog_page_sk);
  append_key(CS_SHIP_MODE_SK, info, r->cs_ship_mode_sk);
  append_key(CS_WAREHOUSE_SK, info, r->cs_warehouse_sk);
  append_key(CS_SOLD_ITEM_SK, info, r->cs_sold_item_sk);
  append_key(CS_PROMO_SK, info, r->cs_promo_sk);
  append_key(CS_ORDER_NUMBER, info, r->cs_order_number);
  append_integer(CS_PRICING_QUANTITY, info, r->cs_pricing.quantity);
  append_decimal(
      CS_PRICING_WHOLESALE_COST, info, &r->cs_pricing.wholesale_cost);
  append_decimal(CS_PRICING_LIST_PRICE, info, &r->cs_pricing.list_price);
  append_decimal(CS_PRICING_SALES_PRICE, info, &r->cs_pricing.sales_price);
  append_decimal(
      CS_PRICING_EXT_DISCOUNT_AMOUNT, info, &r->cs_pricing.ext_discount_amt);
  append_decimal(
      CS_PRICING_EXT_SALES_PRICE, info, &r->cs_pricing.ext_sales_price);
  append_decimal(
      CS_PRICING_EXT_WHOLESALE_COST, info, &r->cs_pricing.ext_wholesale_cost);
  append_decimal(
      CS_PRICING_EXT_LIST_PRICE, info, &r->cs_pricing.ext_list_price);
  append_decimal(CS_PRICING_EXT_TAX, info, &r->cs_pricing.ext_tax);
  append_decimal(CS_PRICING_COUPON_AMT, info, &r->cs_pricing.coupon_amt);
  append_decimal(CS_PRICING_EXT_SHIP_COST, info, &r->cs_pricing.ext_ship_cost);
  append_decimal(CS_PRICING_NET_PAID, info, &r->cs_pricing.net_paid);
  append_decimal(
      CS_PRICING_NET_PAID_INC_TAX, info, &r->cs_pricing.net_paid_inc_tax);
  append_decimal(
      CS_PRICING_NET_PAID_INC_SHIP, info, &r->cs_pricing.net_paid_inc_ship);
  append_decimal(
      CS_PRICING_NET_PAID_INC_SHIP_TAX,
      info,
      &r->cs_pricing.net_paid_inc_ship_tax);
  append_decimal(CS_PRICING_NET_PROFIT, info, &r->cs_pricing.net_profit);

  append_row_end(info);

  return;
}

/*
 * Routine: mk_catalog_sales()
 * Purpose: build rows for the catalog sales table
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
 * 20020902 jms Need to link order date/time to call center record
 * 20020902 jms Should promos be tied to item id?
 */
int mk_w_catalog_sales(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  int nLineitems, i;

  row_skip(CATALOG_RETURNS, (index - 1), dsdGenContext);
  struct W_CATALOG_SALES_TBL* r;
  r = mk_catalog_sales_master(info_arr, index, dsdGenContext);

  /*
   * now we select the number of lineitems in this order, and loop through
   * them, printing as we go
   */
  genrand_integer(
      &nLineitems, DIST_UNIFORM, 4, 14, 0, CS_ORDER_NUMBER, dsdGenContext);
  for (i = 1; i <= nLineitems; i++) {
    mk_detail(info_arr, 1, r, dsdGenContext);
  }

  /**
   * and finally return 1 since we have already printed the rows.
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
int vld_w_catalog_sales(
    int nTable,
    ds_key_t kRow,
    int* Permutation,
    DSDGenContext& dsdGenContext) {
  int nLineitem, nMaxLineitem, i;

  row_skip(nTable, kRow - 1, dsdGenContext);
  row_skip(CATALOG_RETURNS, (kRow - 1), dsdGenContext);
  dsdGenContext.jDate =
      skipDays(CATALOG_SALES, &dsdGenContext.kNewDateIndex, dsdGenContext);

  struct W_CATALOG_SALES_TBL* r;
  r = mk_catalog_sales_master(NULL, kRow, dsdGenContext);
  genrand_integer(
      &nMaxLineitem, DIST_UNIFORM, 4, 14, 9, CS_ORDER_NUMBER, dsdGenContext);
  genrand_integer(
      &nLineitem,
      DIST_UNIFORM,
      1,
      nMaxLineitem,
      0,
      CS_PRICING_QUANTITY,
      dsdGenContext);
  for (i = 1; i < nLineitem; i++) {
    mk_detail(NULL, 0, r, dsdGenContext);
  }
  mk_detail(NULL, 1, r, dsdGenContext);

  return (0);
}
