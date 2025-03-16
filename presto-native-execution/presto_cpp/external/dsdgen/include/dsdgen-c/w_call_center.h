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

#ifndef W_CALL_CENTER_H
#define W_CALL_CENTER_H
#include "address.h"
#include "constants.h"
#include "date.h"
#include "decimal.h"
#include "dist.h"
#include "pricing.h"

#define MIN_CC_TAX_PERCENTAGE "0.00"
#define MAX_CC_TAX_PERCENTAGE "0.12"

int mk_w_call_center(
    void* pDest,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);
int pr_w_call_center(void* pSrc);
int ld_w_call_center(void* r);

#endif
