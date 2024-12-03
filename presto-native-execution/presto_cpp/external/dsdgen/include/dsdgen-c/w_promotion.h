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

#ifndef W_PROMOTION_H
#define W_PROMOTION_H

#include "constants.h"
#include "decimal.h"
#include "porting.h"

int mk_w_promotion(
    void* info_arr,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);

#endif
