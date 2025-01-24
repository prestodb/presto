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

#ifndef W_STORE_SALES_H
#define W_STORE_SALES_H

#include "constants.h"
#include "pricing.h"

int mk_w_store_sales(
    void* info_arr,
    ds_key_t kIndex,
    DSDGenContext& dsdGenContext);
int vld_w_store_sales(
    int nTable,
    ds_key_t kRow,
    int* Permutation,
    DSDGenContext& dsdGenContext);
#endif
