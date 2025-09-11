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

#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/tpcds/gen/dsdgen/include/pricing.h"

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
