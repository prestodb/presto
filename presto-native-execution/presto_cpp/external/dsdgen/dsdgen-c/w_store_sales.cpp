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

#include "w_store_sales.h"

#include "append_info.h"
#include "build_support.h"
#include "columns.h"
#include "config.h"
#include "constants.h"
#include "decimal.h"
#include "genrand.h"
#include "nulls.h"
#include "parallel.h"
#include "permute.h"
#include "porting.h"
#include "scaling.h"
#include "scd.h"
#include "tables.h"
#include "tdefs.h"
#include "w_store_returns.h"

// ds_key_t
// skipDays(int nTable, ds_key_t* pRemainder, DSDGenContext& dsdGenContext);

/*
 * mk_store_sales
 */
W_STORE_SALES_TBL* mk_master_store_sales(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  decimal_t dMin, dMax;
  int nMaxItemCount;
  struct W_STORE_SALES_TBL* r;
  r = &dsdGenContext.g_w_store_sales;

  if (!dsdGenContext.mk_master_store_sales_init) {
    strtodec(&dMin, "1.00");
    strtodec(&dMax, "100000.00");
    nMaxItemCount = 20;
    dsdGenContext.pStoreSalesItemPermutation = makePermutation(
        NULL,
        dsdGenContext.nStoreSalesItemCount =
            static_cast<int>(getIDCount(ITEM, dsdGenContext)),
        SS_PERMUTATION,
        dsdGenContext);

    dsdGenContext.mk_master_store_sales_init = 1;
  } else {
    strtodec(&dMin, "1.00");
    strtodec(&dMax, "100000.00");
  }

  r->ss_sold_store_sk = mk_join(SS_SOLD_STORE_SK, STORE, 1, dsdGenContext);
  r->ss_sold_time_sk = mk_join(SS_SOLD_TIME_SK, TIME, 1, dsdGenContext);
  r->ss_sold_date_sk = mk_join(SS_SOLD_DATE_SK, DATET, 1, dsdGenContext);
  r->ss_sold_customer_sk =
      mk_join(SS_SOLD_CUSTOMER_SK, CUSTOMER, 1, dsdGenContext);
  r->ss_sold_cdemo_sk =
      mk_join(SS_SOLD_CDEMO_SK, CUSTOMER_DEMOGRAPHICS, 1, dsdGenContext);
  r->ss_sold_hdemo_sk =
      mk_join(SS_SOLD_HDEMO_SK, HOUSEHOLD_DEMOGRAPHICS, 1, dsdGenContext);
  r->ss_sold_addr_sk =
      mk_join(SS_SOLD_ADDR_SK, CUSTOMER_ADDRESS, 1, dsdGenContext);
  r->ss_ticket_number = index;
  genrand_integer(
      &dsdGenContext.nStoreSalesItemIndex,
      DIST_UNIFORM,
      1,
      dsdGenContext.nStoreSalesItemCount,
      0,
      SS_SOLD_ITEM_SK,
      dsdGenContext);

  return r;
}

void mk_detail(
    void* info_arr,
    int bPrint,
    struct W_STORE_SALES_TBL* r,
    DSDGenContext& dsdGenContext) {
  int nTemp;
  tdef* pT = getSimpleTdefsByNumber(STORE_SALES, dsdGenContext);

  nullSet(&pT->kNullBitMap, SS_NULLS, dsdGenContext);
  /*
   * items need to be unique within an order
   * use a sequence within the permutation
   */
  if (++dsdGenContext.nStoreSalesItemIndex > dsdGenContext.nStoreSalesItemCount)
    dsdGenContext.nStoreSalesItemIndex = 1;
  int getperm = getPermutationEntry(
      dsdGenContext.pStoreSalesItemPermutation,
      dsdGenContext.nStoreSalesItemIndex);
  r->ss_sold_item_sk =
      matchSCDSK(getperm, r->ss_sold_date_sk, ITEM, dsdGenContext);
  r->ss_sold_promo_sk = mk_join(SS_SOLD_PROMO_SK, PROMOTION, 1, dsdGenContext);
  set_pricing(SS_PRICING, &r->ss_pricing, dsdGenContext);

  /**
   * having gone to the trouble to make the sale, now let's see if it gets
   * returned
   */
  genrand_integer(
      &nTemp, DIST_UNIFORM, 0, 99, 0, SR_IS_RETURNED, dsdGenContext);
  if (nTemp < SR_RETURN_PCT) {
    struct W_STORE_RETURNS_TBL w_web_returns;
    struct W_STORE_RETURNS_TBL* rr = &w_web_returns;
    mk_w_store_returns(rr, 1, dsdGenContext);

    void* info = append_info_get(info_arr, STORE_RETURNS);
    append_row_start(info);

    append_key(SR_RETURNED_DATE_SK, info, rr->sr_returned_date_sk);
    append_key(SR_RETURNED_TIME_SK, info, rr->sr_returned_time_sk);
    append_key(SR_ITEM_SK, info, rr->sr_item_sk);
    append_key(SR_CUSTOMER_SK, info, rr->sr_customer_sk);
    append_key(SR_CDEMO_SK, info, rr->sr_cdemo_sk);
    append_key(SR_HDEMO_SK, info, rr->sr_hdemo_sk);
    append_key(SR_ADDR_SK, info, rr->sr_addr_sk);
    append_key(SR_STORE_SK, info, rr->sr_store_sk);
    append_key(SR_REASON_SK, info, rr->sr_reason_sk);
    append_key(SR_TICKET_NUMBER, info, rr->sr_ticket_number);
    append_integer(SR_PRICING_QUANTITY, info, rr->sr_pricing.quantity);
    append_decimal(SR_PRICING_NET_PAID, info, &rr->sr_pricing.net_paid);
    append_decimal(SR_PRICING_EXT_TAX, info, &rr->sr_pricing.ext_tax);
    append_decimal(
        SR_PRICING_NET_PAID_INC_TAX, info, &rr->sr_pricing.net_paid_inc_tax);
    append_decimal(SR_PRICING_FEE, info, &rr->sr_pricing.fee);
    append_decimal(
        SR_PRICING_EXT_SHIP_COST, info, &rr->sr_pricing.ext_ship_cost);
    append_decimal(
        SR_PRICING_REFUNDED_CASH, info, &rr->sr_pricing.refunded_cash);
    append_decimal(
        SR_PRICING_REVERSED_CHARGE, info, &rr->sr_pricing.reversed_charge);
    append_decimal(SR_PRICING_STORE_CREDIT, info, &rr->sr_pricing.store_credit);
    append_decimal(SR_PRICING_NET_LOSS, info, &rr->sr_pricing.net_loss);
    append_row_end(info);
  }

  void* info = append_info_get(info_arr, STORE_SALES);
  append_row_start(info);

  append_key(SS_SOLD_DATE_SK, info, r->ss_sold_date_sk);
  append_key(SS_SOLD_TIME_SK, info, r->ss_sold_time_sk);
  append_key(SS_SOLD_ITEM_SK, info, r->ss_sold_item_sk);
  append_key(SS_SOLD_CUSTOMER_SK, info, r->ss_sold_customer_sk);
  append_key(SS_SOLD_CDEMO_SK, info, r->ss_sold_cdemo_sk);
  append_key(SS_SOLD_HDEMO_SK, info, r->ss_sold_hdemo_sk);
  append_key(SS_SOLD_ADDR_SK, info, r->ss_sold_addr_sk);
  append_key(SS_SOLD_STORE_SK, info, r->ss_sold_store_sk);
  append_key(SS_SOLD_PROMO_SK, info, r->ss_sold_promo_sk);
  append_key(SS_TICKET_NUMBER, info, r->ss_ticket_number);
  append_integer(SS_PRICING_QUANTITY, info, r->ss_pricing.quantity);
  append_decimal(
      SS_PRICING_WHOLESALE_COST, info, &r->ss_pricing.wholesale_cost);
  append_decimal(SS_PRICING_LIST_PRICE, info, &r->ss_pricing.list_price);
  append_decimal(SS_PRICING_SALES_PRICE, info, &r->ss_pricing.sales_price);
  append_decimal(SS_PRICING_COUPON_AMT, info, &r->ss_pricing.coupon_amt);
  append_decimal(
      SS_PRICING_EXT_SALES_PRICE, info, &r->ss_pricing.ext_sales_price);
  append_decimal(
      SS_PRICING_EXT_WHOLESALE_COST, info, &r->ss_pricing.ext_wholesale_cost);
  append_decimal(
      SS_PRICING_EXT_LIST_PRICE, info, &r->ss_pricing.ext_list_price);
  append_decimal(SS_PRICING_EXT_TAX, info, &r->ss_pricing.ext_tax);
  append_decimal(SS_PRICING_COUPON_AMT, info, &r->ss_pricing.coupon_amt);
  append_decimal(SS_PRICING_NET_PAID, info, &r->ss_pricing.net_paid);
  append_decimal(
      SS_PRICING_NET_PAID_INC_TAX, info, &r->ss_pricing.net_paid_inc_tax);
  append_decimal(SS_PRICING_NET_PROFIT, info, &r->ss_pricing.net_profit);

  append_row_end(info);

  return;
}

/*
 * mk_store_sales
 */
int mk_w_store_sales(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext) {
  int nLineitems, i;
  row_skip(STORE_RETURNS, (index - 1), dsdGenContext);

  /* build the static portion of an order */
  struct W_STORE_SALES_TBL* r;
  r = mk_master_store_sales(info_arr, index, dsdGenContext);

  /* set the number of lineitems and build them */
  genrand_integer(
      &nLineitems, DIST_UNIFORM, 8, 16, 0, SS_TICKET_NUMBER, dsdGenContext);
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
int vld_w_store_sales(
    int nTable,
    ds_key_t kRow,
    int* Permutation,
    DSDGenContext& dsdGenContext) {
  int nLineitem, nMaxLineitem, i;

  row_skip(nTable, kRow - 1, dsdGenContext);
  row_skip(STORE_RETURNS, kRow - 1, dsdGenContext);
  // dsdGenContext.jDate =
  //     skipDays(STORE_SALES, &dsdGenContext.kNewDateIndex, dsdGenContext);

  struct W_STORE_SALES_TBL* r;
  r = mk_master_store_sales(NULL, kRow, dsdGenContext);
  genrand_integer(
      &nMaxLineitem, DIST_UNIFORM, 8, 16, 9, SS_TICKET_NUMBER, dsdGenContext);
  genrand_integer(
      &nLineitem,
      DIST_UNIFORM,
      1,
      nMaxLineitem,
      0,
      SS_PRICING_QUANTITY,
      dsdGenContext);
  for (i = 1; i < nLineitem; i++) {
    mk_detail(NULL, 0, r, dsdGenContext);
  }
  mk_detail(NULL, 1, r, dsdGenContext);

  return (0);
}
