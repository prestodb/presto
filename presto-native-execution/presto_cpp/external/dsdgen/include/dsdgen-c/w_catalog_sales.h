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

#ifndef W_CATALOG_SALES_H
#define W_CATALOG_SALES_H

#include "dist.h"
#include "pricing.h"

int mk_w_catalog_sales(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext);
int vld_w_catalog_sales(
    int nTable,
    ds_key_t kRow,
    int* Permutation,
    DSDGenContext& dsdGenContext);
#endif
