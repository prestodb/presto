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

#ifndef W_WAREHOUSE_H
#define W_WAREHOUSE_H

#include "address.h"
#include "porting.h"

int mk_w_warehouse(
    void* info_arr,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);

#endif
