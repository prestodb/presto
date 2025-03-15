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

#ifndef W_STORE_RETURNS_H
#define W_STORE_RETURNS_H
#include "decimal.h"
#include "pricing.h"

#define SR_SAME_CUSTOMER 80

int mk_w_store_returns(
    void* row,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);
#endif
