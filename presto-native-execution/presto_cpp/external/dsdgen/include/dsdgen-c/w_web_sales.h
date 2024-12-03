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

#include "porting.h"
#include "pricing.h"

/***
*** WS_xxx Web Sales Defines
***/
#define WS_QUANTITY_MAX "100"
#define WS_MARKUP_MAX "2.00"
#define WS_DISCOUNT_MAX "1.00"
#define WS_WHOLESALE_MAX "100.00"
#define WS_COUPON_MAX "0.50"
#define WS_GIFT_PCT                                         \
  7 /* liklihood that a purchase is shipped to someone else \
     */
#define WS_ITEMS_PER_ORDER 9 /* number of lineitems in an order */
#define WS_MIN_SHIP_DELAY 1 /* time between order date and ship date */
#define WS_MAX_SHIP_DELAY 120

int mk_w_web_sales(
    void* info_arr,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);
int vld_web_sales(
    int nTable,
    ds_key_t kRow,
    int* Permutation,
    DSDGenContext& dsdGenContext);
