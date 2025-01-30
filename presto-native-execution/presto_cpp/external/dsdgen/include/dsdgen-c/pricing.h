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

#ifndef PRICING_H
#define PRICING_H
#include "decimal.h"

typedef struct DS_LIMITS_T {
  int nId;
  char* szQuantity;
  char* szMarkUp;
  char* szDiscount;
  char* szWholesale;
  char* szCoupon;
} ds_limits_t;

void set_pricing(
    int nTabId,
    ds_pricing_t* pPricing,
    DSDGenContext& dsdGenContext);
#endif
