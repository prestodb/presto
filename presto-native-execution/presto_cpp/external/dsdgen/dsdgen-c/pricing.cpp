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

#include "pricing.h"
#include <stdio.h>
#include "columns.h"
#include "config.h"
#include "constants.h"
#include "decimal.h"
#include "dist.h"
#include "error_msg.h"
#include "genrand.h"
#include "porting.h"
#include "w_web_sales.h"

#define MAX_LIMIT 12
static ds_limits_t aPriceLimits[MAX_LIMIT] = {
    {CS_PRICING,
     CS_QUANTITY_MAX,
     CS_MARKUP_MAX,
     CS_DISCOUNT_MAX,
     CS_WHOLESALE_MAX,
     CS_COUPON_MAX},
    {SS_PRICING,
     SS_QUANTITY_MAX,
     SS_MARKUP_MAX,
     SS_DISCOUNT_MAX,
     SS_WHOLESALE_MAX,
     SS_COUPON_MAX},
    {WS_PRICING,
     WS_QUANTITY_MAX,
     WS_MARKUP_MAX,
     WS_DISCOUNT_MAX,
     WS_WHOLESALE_MAX,
     WS_COUPON_MAX},
    {CR_PRICING,
     CS_QUANTITY_MAX,
     CS_MARKUP_MAX,
     CS_DISCOUNT_MAX,
     CS_WHOLESALE_MAX,
     CS_COUPON_MAX},
    {SR_PRICING,
     SS_QUANTITY_MAX,
     SS_MARKUP_MAX,
     SS_DISCOUNT_MAX,
     SS_WHOLESALE_MAX,
     SS_COUPON_MAX},
    {WR_PRICING,
     WS_QUANTITY_MAX,
     WS_MARKUP_MAX,
     WS_DISCOUNT_MAX,
     WS_WHOLESALE_MAX,
     WS_COUPON_MAX},
    {S_PLINE_PRICING,
     WS_QUANTITY_MAX,
     WS_MARKUP_MAX,
     WS_DISCOUNT_MAX,
     WS_WHOLESALE_MAX,
     WS_COUPON_MAX},
    {S_CLIN_PRICING,
     WS_QUANTITY_MAX,
     WS_MARKUP_MAX,
     WS_DISCOUNT_MAX,
     WS_WHOLESALE_MAX,
     WS_COUPON_MAX},
    {S_WLIN_PRICING,
     WS_QUANTITY_MAX,
     WS_MARKUP_MAX,
     WS_DISCOUNT_MAX,
     WS_WHOLESALE_MAX,
     WS_COUPON_MAX},
    {S_SRET_PRICING,
     WS_QUANTITY_MAX,
     WS_MARKUP_MAX,
     WS_DISCOUNT_MAX,
     WS_WHOLESALE_MAX,
     WS_COUPON_MAX},
    {S_CRET_PRICING,
     WS_QUANTITY_MAX,
     WS_MARKUP_MAX,
     WS_DISCOUNT_MAX,
     WS_WHOLESALE_MAX,
     WS_COUPON_MAX},
    {S_WRET_PRICING,
     WS_QUANTITY_MAX,
     WS_MARKUP_MAX,
     WS_DISCOUNT_MAX,
     WS_WHOLESALE_MAX,
     WS_COUPON_MAX}

};

/*
 * Routine: set_pricing(int nTabId, ds_pricing_t *pPricing)
 * Purpose: handle the various pricing calculations for the fact tables
 * Notes:
 *	the RNG usage is not kept in sync between sales pricing and returns
 *pricing. If the calculations look wrong, it may be necessary to "waste" some
 *RNG calls on one side or the other to bring things back in line Data
 *Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void set_pricing(
    int nTabId,
    ds_pricing_t* pPricing,
    DSDGenContext& dsdGenContext) {
  int nLastId = -1, nQuantityMax = std::numeric_limits<int>::max(),
      nQuantityMin = 1;
  decimal_t dQuantity, dMarkupMin, dDiscountMin, dWholesaleMin, dMarkupMax,
      dDiscountMax, dWholesaleMax, dCouponMin, dCouponMax, dZero, dOneHalf,
      d9pct, dOne, dTemp, dHundred;
  decimal_t dMarkup, dCoupon, dShipping, dDiscount, dTemp2;
  int i, nCashPct, nCreditPct, nCouponUsage;

  strtodec(&dMarkupMin, "0.00");
  strtodec(&dDiscountMin, "0.00");
  strtodec(&dWholesaleMin, "1.00");
  strtodec(&dCouponMin, "0.00");
  strtodec(&dZero, "0.00");
  strtodec(&dOneHalf, "0.50");
  strtodec(&d9pct, "0.09");
  strtodec(&dWholesaleMin, "1.00");
  strtodec(&dHundred, "100.00");
  strtodec(&dOne, "1.00");

  if (nTabId != nLastId) {
    nLastId = -1;
    for (i = 0; i < MAX_LIMIT; i++) {
      if (nTabId == aPriceLimits[i].nId)
        nLastId = i;
    }
    if (nLastId == -1)
      INTERNAL("No pricing limits defined");
    nQuantityMax = atoi(aPriceLimits[nLastId].szQuantity);
    strtodec(&dDiscountMax, aPriceLimits[nLastId].szDiscount);
    strtodec(&dMarkupMax, aPriceLimits[nLastId].szMarkUp);
    strtodec(&dWholesaleMax, aPriceLimits[nLastId].szWholesale);
    strtodec(&dCouponMax, aPriceLimits[nLastId].szCoupon);
  }

  switch (nTabId) {
    case SS_PRICING:
    case CS_PRICING:
    case WS_PRICING:
    case S_PLINE_PRICING:
    case S_CLIN_PRICING:
    case S_WLIN_PRICING:
      genrand_integer(
          &pPricing->quantity,
          DIST_UNIFORM,
          nQuantityMin,
          nQuantityMax,
          0,
          nTabId,
          dsdGenContext);
      itodec(&dQuantity, pPricing->quantity);
      genrand_decimal(
          &pPricing->wholesale_cost,
          DIST_UNIFORM,
          &dWholesaleMin,
          &dWholesaleMax,
          NULL,
          nTabId,
          dsdGenContext);

      /* ext_wholesale_cost = wholesale_cost * quantity */
      decimal_t_op(
          &pPricing->ext_wholesale_cost,
          OP_MULT,
          &dQuantity,
          &pPricing->wholesale_cost);

      /* list_price = wholesale_cost * (1 + markup) */
      genrand_decimal(
          &dMarkup,
          DIST_UNIFORM,
          &dMarkupMin,
          &dMarkupMax,
          NULL,
          nTabId,
          dsdGenContext);
      decimal_t_op(&dMarkup, OP_PLUS, &dMarkup, &dOne);
      decimal_t_op(
          &pPricing->list_price, OP_MULT, &pPricing->wholesale_cost, &dMarkup);

      /* sales_price = list_price * (1 - discount)*/
      genrand_decimal(
          &dDiscount,
          DIST_UNIFORM,
          &dDiscountMin,
          &dDiscountMax,
          NULL,
          nTabId,
          dsdGenContext);
      NegateDecimal(&dDiscount);
      decimal_t_op(&pPricing->ext_discount_amt, OP_PLUS, &dDiscount, &dOne);
      decimal_t_op(
          &pPricing->sales_price,
          OP_MULT,
          &pPricing->list_price,
          &pPricing->ext_discount_amt);

      /* ext_list_price = list_price * quantity */
      decimal_t_op(
          &pPricing->ext_list_price,
          OP_MULT,
          &pPricing->list_price,
          &dQuantity);

      /* ext_sales_price = sales_price * quantity */
      decimal_t_op(
          &pPricing->ext_sales_price,
          OP_MULT,
          &pPricing->sales_price,
          &dQuantity);

      /* ext_discount_amt = ext_list_price - ext_sales_price */
      decimal_t_op(
          &pPricing->ext_discount_amt,
          OP_MINUS,
          &pPricing->ext_list_price,
          &pPricing->ext_sales_price);

      /* coupon_amt = ext_sales_price * coupon */
      genrand_decimal(
          &dCoupon, DIST_UNIFORM, &dZero, &dOne, NULL, nTabId, dsdGenContext);
      genrand_integer(
          &nCouponUsage, DIST_UNIFORM, 1, 100, 0, nTabId, dsdGenContext);
      if (nCouponUsage <= 20) /* 20% of sales employ a coupon */
        decimal_t_op(
            &pPricing->coupon_amt,
            OP_MULT,
            &pPricing->ext_sales_price,
            &dCoupon);
      else
        memcpy(&pPricing->coupon_amt, &dZero, sizeof(decimal_t));

      /* net_paid = ext_sales_price - coupon_amt */
      decimal_t_op(
          &pPricing->net_paid,
          OP_MINUS,
          &pPricing->ext_sales_price,
          &pPricing->coupon_amt);

      /* shipping_cost = list_price * shipping */
      genrand_decimal(
          &dShipping,
          DIST_UNIFORM,
          &dZero,
          &dOneHalf,
          NULL,
          nTabId,
          dsdGenContext);
      decimal_t_op(
          &pPricing->ship_cost, OP_MULT, &pPricing->list_price, &dShipping);

      /* ext_shipping_cost = shipping_cost * quantity */
      decimal_t_op(
          &pPricing->ext_ship_cost, OP_MULT, &pPricing->ship_cost, &dQuantity);

      /* net_paid_inc_ship = net_paid + ext_shipping_cost */
      decimal_t_op(
          &pPricing->net_paid_inc_ship,
          OP_PLUS,
          &pPricing->net_paid,
          &pPricing->ext_ship_cost);

      /* ext_tax = tax * net_paid */
      genrand_decimal(
          &pPricing->tax_pct,
          DIST_UNIFORM,
          &dZero,
          &d9pct,
          NULL,
          nTabId,
          dsdGenContext);
      decimal_t_op(
          &pPricing->ext_tax, OP_MULT, &pPricing->net_paid, &pPricing->tax_pct);

      /* net_paid_inc_tax = net_paid + ext_tax */
      decimal_t_op(
          &pPricing->net_paid_inc_tax,
          OP_PLUS,
          &pPricing->net_paid,
          &pPricing->ext_tax);

      /* net_paid_inc_ship_tax = net_paid_inc_tax + ext_shipping_cost */
      decimal_t_op(
          &pPricing->net_paid_inc_ship_tax,
          OP_PLUS,
          &pPricing->net_paid_inc_ship,
          &pPricing->ext_tax);

      /* net_profit = net_paid - ext_wholesale_cost */
      decimal_t_op(
          &pPricing->net_profit,
          OP_MINUS,
          &pPricing->net_paid,
          &pPricing->ext_wholesale_cost);
      break;
    case CR_PRICING:
    case SR_PRICING:
    case WR_PRICING:
      /* quantity is determined before we are called */
      /* ext_wholesale_cost = wholesale_cost * quantity */
      itodec(&dQuantity, pPricing->quantity);
      decimal_t_op(
          &pPricing->ext_wholesale_cost,
          OP_MULT,
          &dQuantity,
          &pPricing->wholesale_cost);

      /* ext_list_price = list_price * quantity */
      decimal_t_op(
          &pPricing->ext_list_price,
          OP_MULT,
          &pPricing->list_price,
          &dQuantity);

      /* ext_sales_price = sales_price * quantity */
      decimal_t_op(
          &pPricing->ext_sales_price,
          OP_MULT,
          &pPricing->sales_price,
          &dQuantity);

      /* net_paid = ext_list_price (couppons don't effect returns) */
      memcpy(
          &pPricing->net_paid, &pPricing->ext_sales_price, sizeof(decimal_t));

      /* shipping_cost = list_price * shipping */
      genrand_decimal(
          &dShipping,
          DIST_UNIFORM,
          &dZero,
          &dOneHalf,
          NULL,
          nTabId,
          dsdGenContext);
      decimal_t_op(
          &pPricing->ship_cost, OP_MULT, &pPricing->list_price, &dShipping);

      /* ext_shipping_cost = shipping_cost * quantity */
      decimal_t_op(
          &pPricing->ext_ship_cost, OP_MULT, &pPricing->ship_cost, &dQuantity);

      /* net_paid_inc_ship = net_paid + ext_shipping_cost */
      decimal_t_op(
          &pPricing->net_paid_inc_ship,
          OP_PLUS,
          &pPricing->net_paid,
          &pPricing->ext_ship_cost);

      /* ext_tax = tax * net_paid */
      decimal_t_op(
          &pPricing->ext_tax, OP_MULT, &pPricing->net_paid, &pPricing->tax_pct);

      /* net_paid_inc_tax = net_paid + ext_tax */
      decimal_t_op(
          &pPricing->net_paid_inc_tax,
          OP_PLUS,
          &pPricing->net_paid,
          &pPricing->ext_tax);

      /* net_paid_inc_ship_tax = net_paid_inc_tax + ext_shipping_cost */
      decimal_t_op(
          &pPricing->net_paid_inc_ship_tax,
          OP_PLUS,
          &pPricing->net_paid_inc_ship,
          &pPricing->ext_tax);

      /* net_profit = net_paid - ext_wholesale_cost */
      decimal_t_op(
          &pPricing->net_profit,
          OP_MINUS,
          &pPricing->net_paid,
          &pPricing->ext_wholesale_cost);

      /* see to it that the returned amounts add up to the total returned */
      /* allocate some of return to cash */
      genrand_integer(
          &nCashPct, DIST_UNIFORM, 0, 100, 0, nTabId, dsdGenContext);
      itodec(&dTemp, nCashPct);
      decimal_t_op(&pPricing->refunded_cash, OP_DIV, &dTemp, &dHundred);
      decimal_t_op(
          &pPricing->refunded_cash,
          OP_MULT,
          &pPricing->refunded_cash,
          &pPricing->net_paid);

      /* allocate some to reversed charges */
      genrand_integer(
          &nCreditPct, DIST_UNIFORM, 1, 100, 0, nTabId, dsdGenContext);
      itodec(&dTemp2, nCreditPct);
      decimal_t_op(&dTemp, OP_DIV, &dTemp2, &dHundred);
      decimal_t_op(
          &dTemp2, OP_MINUS, &pPricing->net_paid, &pPricing->refunded_cash);
      decimal_t_op(&pPricing->reversed_charge, OP_MULT, &dTemp2, &dTemp);

      /* the rest is store credit */
      decimal_t_op(
          &pPricing->store_credit,
          OP_MINUS,
          &pPricing->net_paid,
          &pPricing->reversed_charge);
      decimal_t_op(
          &pPricing->store_credit,
          OP_MINUS,
          &pPricing->store_credit,
          &pPricing->refunded_cash);

      /* pick a fee for the return */
      genrand_decimal(
          &pPricing->fee,
          DIST_UNIFORM,
          &dOneHalf,
          &dHundred,
          &dZero,
          nTabId,
          dsdGenContext);

      /* and calculate the net effect */
      decimal_t_op(
          &pPricing->net_loss,
          OP_MINUS,
          &pPricing->net_paid_inc_ship_tax,
          &pPricing->store_credit);
      decimal_t_op(
          &pPricing->net_loss,
          OP_MINUS,
          &pPricing->net_loss,
          &pPricing->refunded_cash);
      decimal_t_op(
          &pPricing->net_loss,
          OP_MINUS,
          &pPricing->net_loss,
          &pPricing->reversed_charge);
      decimal_t_op(
          &pPricing->net_loss, OP_PLUS, &pPricing->net_loss, &pPricing->fee);
      break;
  }

  return;
}
