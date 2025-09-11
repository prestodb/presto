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

#ifndef PRICING_H
#define PRICING_H
#include "velox/tpcds/gen/dsdgen/include/decimal.h"

typedef struct DS_LIMITS_T {
  int nId;
  const char* szQuantity;
  const char* szMarkUp;
  const char* szDiscount;
  const char* szWholesale;
  const char* szCoupon;
} ds_limits_t;

void set_pricing(
    int nTabId,
    ds_pricing_t* pPricing,
    DSDGenContext& dsdGenContext);
#endif
