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

#include "dist.h"
#include "porting.h"

int mk_w_inventory(
    void* info_arr,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);

ds_key_t sc_w_inventory(int nScale, DSDGenContext& dsdGenContext);
