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
/*
 * DATE table structure
 */
#ifndef W_DATETBL_H
#define W_DATETBL_H

#include "constants.h"
#include "dist.h"
#include "porting.h"

int mk_w_date(void* info_arr, ds_key_t kIndex, DSDGenContext& dsdGenContext);

int vld_w_date(
    int nTable,
    ds_key_t kRow,
    int* Permutation,
    DSDGenContext& dsdGenContext);
#endif
