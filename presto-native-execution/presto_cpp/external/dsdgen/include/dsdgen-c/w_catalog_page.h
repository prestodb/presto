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

#ifndef CATALOG_PAGE_H
#define CATALOG_PAGE_H

#include "constants.h"
#include "dist.h"
#include "porting.h"

int mk_w_catalog_page(
    void* info_arr,
    ds_key_t index,
    DSDGenContext& dsdGenContext);

#endif
