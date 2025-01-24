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

#ifndef W_ITEM_H
#define W_ITEM_H

#include "constants.h"
#include "decimal.h"
#include "porting.h"

#define I_PROMO_PERCENTAGE \
  20 /* percent of items that have associated promotions */
#define MIN_ITEM_MARKDOWN_PCT "0.30"
#define MAX_ITEM_MARKDOWN_PCT "0.90"

int mk_w_item(void* info_arr, ds_key_t kIndex, DSDGenContext& dsdGenContext);
int vld_w_item(
    int nTable,
    ds_key_t kRow,
    int* Permutation,
    DSDGenContext& dsdGenContext);
#endif
